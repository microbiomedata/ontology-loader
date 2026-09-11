"""Ontology Processor class to process ontology terms and relations."""

import gzip
import logging
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Iterable, Iterator

import pystow
from linkml_runtime.dumpers import json_dumper
from nmdc_schema.nmdc import OntologyClass, OntologyRelation
from oaklib.implementations.sqldb.sql_implementation import SqlImplementation
from sqlalchemy import create_engine
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Map of closure values to (predicate list, closure relation name).
# `None` means "no ancestry closure; emit only direct relationships."
# `all` and `none` are convenience values handled in `_normalize_closure_spec`;
# they don't appear here because they don't map to a single (predicates, name) pair.
_CLOSURE_SPECS = {
    "combined": (["rdfs:subClassOf", "BFO:0000050"], "entailed_isa_partof_closure"),
    "isa": (["rdfs:subClassOf"], "entailed_isa_closure"),
    "partof": (["BFO:0000050"], "entailed_partof_closure"),
}

# What `all` expands to. Kept in stable order so log output is deterministic.
_ALL_CLOSURES = ("combined", "isa", "partof")

# User-facing valid values. `all` and `none` are convenience aliases handled in
# `_normalize_closure_spec`. Stable order matches the CLI choice list.
VALID_CLOSURES = ("combined", "isa", "partof", "all", "none")


def normalize_curie_list(curies: str | Iterable[str]) -> tuple[str, ...]:
    """
    Return CURIEs as a deduplicated tuple, treating a bare string as one CURIE.

    ``str`` satisfies ``Iterable[str]``, so a caller passing a single CURIE rather
    than a sequence would otherwise have it iterated character by character. That
    fails silently: the placeholders match nothing, the exclusion set is empty, and
    the load quietly runs unfiltered. Verified before this guard existed:
    ``exclude_descendants_of="ENVO:01000254"`` excluded 0 terms where the same value
    in a tuple excluded 400.
    """
    if isinstance(curies, str):
        curies = (curies,)
    return tuple(dict.fromkeys(curies))


class OntologyProcessor:
    """Ontology Processor class to process ontology terms and relations."""

    def __init__(self, ontology: str, force_refresh: bool = True, exclude_descendants_of: Iterable[str] = ()) -> None:
        """
        Initialize the OntologyProcessor with a given SQLite ontology.

        :param ontology: The ontology prefix (e.g., "envo", "go", "uberon", etc.)
        :param exclude_descendants_of: Exclude proper subclass descendants; retain the named roots.
        :param force_refresh: If True (default, preserves 0.2.x behavior), wipe any cached pystow
            directory for this ontology and re-download from S3. If False, reuse the cached
            artifact when present; pystow.ensure() still downloads if the cache is empty.
        """
        self.exclude_descendants_of = normalize_curie_list(exclude_descendants_of)
        self.ontology = ontology

        self.force_refresh = force_refresh
        # Cache the lowercased ontology name once
        self._ontology_lc = ontology.lower()

        self.ontology_db_path = self.download_and_prepare_ontology()
        # Supply the engine so oaklib cannot reopen the source with write access.
        self.adapter = SqlImplementation(engine=create_engine("sqlite://", creator=self._connect_readonly))
        self.excluded_entities = self._load_excluded_entities()
        self.adapter.precompute_lookups()  # Optimize lookups

        # Cache root terms for efficient lookups
        self.root_terms = self._roots_from_statements()

    def _connect_readonly(self) -> sqlite3.Connection:
        """Open the semsql source with SQLite-enforced read-only access."""
        return sqlite3.connect(f"{Path(self.ontology_db_path).resolve().as_uri()}?mode=ro", uri=True)

    def _exclusion_query(self) -> tuple[str, list[str]]:
        """Select proper subclass descendants, retaining every explicitly named root."""
        placeholders = ",".join("?" for _ in self.exclude_descendants_of)
        query = (
            "SELECT DISTINCT subject FROM entailed_edge "  # noqa: S608 -- only placeholder syntax is interpolated
            f"WHERE predicate='rdfs:subClassOf' AND object IN ({placeholders}) "
            "AND subject <> object "
            f"AND subject NOT IN ({placeholders})"
        )
        return query, [*self.exclude_descendants_of, *self.exclude_descendants_of]

    def _load_excluded_entities(self) -> set[str]:
        """Materialize the exclusion set once for oaklib's entity and direct relation streams."""
        if not self.exclude_descendants_of:
            return set()
        query, params = self._exclusion_query()
        with closing(self._connect_readonly()) as connection:
            excluded = {row[0] for row in connection.execute(query, params)}
        logger.info("Excluding %d proper descendants of %s", len(excluded), self.exclude_descendants_of)
        return excluded

    def download_and_prepare_ontology(self):
        """Download and prepare the ontology database for processing."""
        logger.info(f"Preparing ontology: {self.ontology}")

        # Get the ontology-specific pystow directory
        source_ontology_module = pystow.module(self.ontology).base  # Example: ~/.pystow/envo

        if source_ontology_module.exists():
            if self.force_refresh:
                logger.info(f"Removing existing pystow directory for {self.ontology}: {source_ontology_module}")
                shutil.rmtree(source_ontology_module)
            else:
                logger.info(f"Reusing cached pystow directory for {self.ontology}: {source_ontology_module}")

        # Define ontology URL. The raw bbop-sqlite S3 bucket's public access has been retired
        # (INCATools/semantic-sql#112); object paths are unchanged, only the host differs. See
        # https://github.com/microbiomedata/ontology-loader/issues/59.
        ontology_db_url_prefix = "https://semanticsql.berkeleybop.io/"
        ontology_db_url_suffix = ".db.gz"
        ontology_url = ontology_db_url_prefix + self.ontology + ontology_db_url_suffix

        # Define paths (download to the module-specific directory).
        # pystow.ensure() is a no-op if the file already exists at the expected path,
        # so this is what handles the "reuse cache if present, download if missing"
        # branch when force_refresh=False.
        #
        # The CDN sits behind a Browser Integrity Check that 403s default client User-Agents
        # (Python-urllib, bare curl/wget), so this must use the 'requests' backend with an
        # explicit non-default User-Agent -- pystow's default 'urllib' backend has no clean way
        # to set request headers at all.
        compressed_path = pystow.ensure(
            self.ontology,
            f"{self.ontology}.db.gz",
            url=ontology_url,
            download_kwargs={
                "backend": "requests",
                "headers": {"User-Agent": "ontology-loader (https://github.com/microbiomedata/ontology-loader)"},
            },
        )
        decompressed_path = compressed_path.with_suffix("")  # Remove .gz to get .db file

        # Extract the file if not already extracted
        if not decompressed_path.exists():
            logger.info(f"Extracting {compressed_path} to {decompressed_path}...")
            with gzip.open(compressed_path, "rb") as f_in:
                with open(decompressed_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

        logger.info(f"Ontology database is ready at: {decompressed_path}")
        return decompressed_path

    def _create_ontology_class(self, entity_id, is_obsolete=False):
        """
        Create an OntologyClass instance with common attributes.

        :param entity_id: The entity ID for the ontology class
        :param is_obsolete: Whether the entity is obsolete
        :return: An OntologyClass instance
        """
        ontology_class = OntologyClass(
            id=entity_id,
            type="nmdc:OntologyClass",
            # sorted() because oaklib builds this list as `list(set(...))`, whose order follows
            # Python's per-process hash randomization. Without sorting, loading the same ontology
            # twice writes different documents and every meticulous run rewrites classes whose
            # content has not changed. See
            # https://github.com/microbiomedata/ontology-loader/issues/76
            #
            # This is a local workaround, not the root fix. The root cause is upstream and tracked
            # at https://github.com/INCATools/ontology-access-kit/issues/909 . If oaklib starts
            # returning a deterministic order, this sorted() becomes redundant rather than wrong,
            # so it can stay until someone confirms the upstream fix has shipped.
            alternative_names=sorted(self.adapter.entity_aliases(entity_id) or []),
            definition=self.adapter.definition(entity_id) or "",
            relations=[],
            is_root=entity_id in self.root_terms,
            is_obsolete=is_obsolete,
            name=self.adapter.label(entity_id) or "",
        )

        # Ensure boolean values are properly set
        if ontology_class.is_root is None:
            ontology_class.is_root = False
        if ontology_class.is_obsolete is None:
            ontology_class.is_obsolete = is_obsolete

        return ontology_class

    def _matches_ontology(self, entity_id: str) -> bool:
        """Case-insensitive check that ``entity_id`` is a CURIE in this ontology."""
        head, sep, _ = entity_id.partition(":")
        return bool(sep) and head.lower() == self._ontology_lc

    def _roots_from_statements(self) -> set[str]:
        """
        Return the same root CURIEs as ``adapter.roots()``, from one SQL query.

        oaklib has no SQL-backed override for ``roots()``, so the naive
        interface implementation runs: it lists every ``owl:Class``, iterates
        every relationship in the ontology through the ORM, and discards any
        class that appears as a subject. On NCBITaxon that costs 462 seconds
        and 2.86 GB to return three CURIEs. Upstream:
        https://github.com/INCATools/ontology-access-kit/issues/881

        This reproduces the same definition against the semsql views directly.
        A root is a declared class with no outgoing relationship,
        ignoring self-edges and ``owl:Thing`` objects, and is not deprecated.
        Deliberately not filtered to this ontology's own prefix: the shipped
        behaviour considers imported parents, so an ENVO term whose only
        parent is a BFO term is not a root, and that is preserved here.

        ``deprecated_node`` is the same view oaklib's ``obsoletes()`` reads,
        so ``filter_obsoletes=True`` is preserved. Blank nodes and SWRL
        subjects are excluded to match ``entities()``. The ``ESCAPE`` clauses
        are load-bearing: an unescaped ``_`` is a single-character wildcard,
        which would also match a one-letter CURIE prefix.

        Include all six sources used by the SQL adapter's non-index relationship
        path. RBox predicates can apply to class/property-punned subjects too.
        ``roots()`` supplies no subjects, so neither the subject index's OWL
        meta-class filter nor reverse equivalent-class traversal applies.
        In particular, class-valued ``rdf:type`` is not filtered further.
        Check these extra sources only for candidates surviving the bulk edge
        exclusion. Expand the ``class_node`` and ``object_property_node``
        membership checks into indexed declaration lookups: their DISTINCT
        views otherwise scan all declarations in correlated subqueries.

        Verified equal to ``adapter.roots()`` on ENVO, PO, OBI, PATO and
        NCBITaxon. ``test_roots_from_statements_matches_adapter_roots`` pins
        the ENVO case so a divergence fails CI rather than silently
        mislabelling roots.
        """
        query = """
            SELECT DISTINCT declared.subject
            FROM statements AS declared
            WHERE declared.predicate = 'rdf:type' AND declared.object = 'owl:Class'
              AND declared.subject NOT LIKE '\\_:%' ESCAPE '\\'
              -- GLOB, not LIKE: SQLite's LIKE folds ASCII case, while oaklib's
              -- entities() drops SWRL ids with a case-sensitive startswith. LIKE
              -- here would also remove `<URN:SWRLx`, which oaklib keeps as a root.
              AND declared.subject NOT GLOB '<urn:swrl*'
              AND declared.subject NOT IN ('owl:Thing', 'owl:Nothing')
              AND declared.subject NOT IN (
                  SELECT parent.subject FROM edge AS parent
                  -- NULL-safe on purpose. semsql keeps literal values in
                  -- statements.value, so owl_has_value's filler is NULL for a
                  -- literal restriction. oaklib still emits that relationship
                  -- (_is_blank(None) is falsy, not an error) and roots() removes
                  -- the subject. Plain <> against NULL evaluates to unknown,
                  -- which would drop the row and leave the class a root.
                  WHERE parent.object IS NOT parent.subject
                    AND parent.object IS NOT 'owl:Thing'
                    AND (parent.object IS NULL OR parent.object NOT LIKE '\\_:%' ESCAPE '\\')
              )
              AND NOT EXISTS (
                  SELECT 1 FROM (
                      SELECT subject, object FROM statements AS relationship
                      WHERE subject = declared.subject
                        AND (
                            (predicate IN ('owl:equivalentClass', 'rdf:type')
                             AND EXISTS (
                                 SELECT 1 FROM statements AS object_class
                                 WHERE object_class.subject = relationship.object
                                   AND object_class.predicate = 'rdf:type'
                                   AND object_class.object = 'owl:Class'
                             ))
                            OR predicate IN ('rdfs:domain', 'rdfs:range', 'owl:inverseOf')
                            OR (EXISTS (
                                SELECT 1 FROM statements AS property
                                WHERE property.subject = relationship.predicate
                                  AND property.predicate = 'rdf:type'
                                  AND property.object = 'owl:ObjectProperty'
                            ) AND object <> '')
                        )
                      UNION ALL
                      SELECT subclass.subject, restriction.filler AS object
                      FROM rdfs_subclass_of_statement AS subclass
                      JOIN owl_has_value AS restriction ON subclass.object = restriction.id
                      WHERE subclass.subject = declared.subject
                  ) AS parent
                  -- NULL-safe on purpose. semsql keeps literal values in
                  -- statements.value, so owl_has_value's filler is NULL for a
                  -- literal restriction. oaklib still emits that relationship
                  -- (_is_blank(None) is falsy, not an error) and roots() removes
                  -- the subject. Plain <> against NULL evaluates to unknown,
                  -- which would drop the row and leave the class a root.
                  WHERE parent.object IS NOT parent.subject
                    AND parent.object IS NOT 'owl:Thing'
                    AND (parent.object IS NULL OR parent.object NOT LIKE '\\_:%' ESCAPE '\\')
              )
              AND declared.subject NOT IN (SELECT obsolete.id FROM deprecated_node AS obsolete)
        """
        with closing(self._connect_readonly()) as connection:
            return {subject for (subject,) in connection.execute(query)}

    def _ancestry_query(self, predicates: list[str]) -> tuple[str, list[str]]:
        """Build the ancestry SQL and parameters, deduplicating only across predicates."""
        placeholders = ",".join("?" for _ in predicates)
        prefix_pattern = f"{self._ontology_lc}:%"
        distinct = "DISTINCT " if len(predicates) > 1 else ""
        query = (
            f"SELECT {distinct}subject, object FROM entailed_edge "  # noqa: S608 -- predicates
            f"WHERE predicate IN ({placeholders}) AND subject LIKE ? AND object LIKE ?"
        )
        params = [*predicates, prefix_pattern, prefix_pattern]
        if self.exclude_descendants_of:
            exclusion_query, exclusion_params = self._exclusion_query()
            # Drop a relation when EITHER endpoint is excluded, including kept-to-excluded
            # part_of edges. A subclass-only closure cannot exercise that boundary case.
            query = (
                f"WITH excluded AS ({exclusion_query}) {query} "  # noqa: S608 -- composed parameterized SQL
                "AND subject NOT IN (SELECT subject FROM excluded) "
                "AND object NOT IN (SELECT subject FROM excluded)"
            )
            params = [*exclusion_params, *params]
        return query, params

    def _ancestry_pairs_from_entailed_edge(
        self, predicates: list[str], relevant_entities: set[str]
    ) -> Iterator[tuple[str, str]]:
        """
        Return every (subject, object) ancestry pair for ``predicates``, from semsql's own table.

        Reads ``entailed_edge`` in one bulk query instead of calling ``adapter.ancestors()`` once
        per entity. ``entailed_edge`` is not reliably reflexive per predicate: every entity in
        ``relevant_entities`` gets an explicit self-pair, and native self-loop rows are skipped
        during the scan so a self-pair is never emitted twice. ``DISTINCT`` is used only for multiple
        predicates: a single predicate cannot produce a duplicate pair, but multiple predicates
        can reach the same pair via more than one predicate. Omitting ``DISTINCT`` for a single
        predicate avoids SQLite building a temporary B-tree of all ancestry pairs.

        :param predicates: List of predicate CURIEs (e.g. ``["rdfs:subClassOf"]`` or
            ``["rdfs:subClassOf", "BFO:0000050"]``) to include in this closure.
        :param relevant_entities: The exact, non-deprecated subject set for this ontology -- the
            same set ``get_relations_closure`` builds via ``self.adapter.entities()`` before
            calling this method. Used to filter subjects only; objects are filtered by id prefix
            alone, not deprecation, matching the old per-entity loop's behavior.
        :return: Generator of (subject, object) tuples. Subject is always in ``relevant_entities``;
            object is prefix-matched to this ontology but not deprecation-filtered.
        """
        query, params = self._ancestry_query(predicates)
        # Generator, not fetchall(): NCBITaxon-scale closures are tens of millions of rows. The
        # `with` block stays open across the caller's iteration because a generator suspends at
        # `yield` rather than returning; contextlib.closing() is required because
        # sqlite3.Connection's own context manager only commits/rolls back, it does not close.
        with closing(self._connect_readonly()) as con:
            for subject, obj in con.execute(query, params):
                if subject == obj:
                    continue
                if subject in relevant_entities and self._matches_ontology(obj):
                    yield subject, obj
        for entity in relevant_entities:
            yield entity, entity

    def get_terms_and_metadata(self) -> list[OntologyClass]:
        """Return all ontology classes as a list for meticulous loading."""
        return list(self.iter_terms_and_metadata())

    def iter_terms_and_metadata(self) -> Iterator[OntologyClass]:
        """Yield ontology classes without retaining them or embedding relations."""
        # Process non-obsolete entities
        for entity in tqdm(
            self.adapter.entities(filter_obsoletes=True),
            desc=f"Extracting {self.ontology} classes (non-obsolete)",
            unit="entity",
        ):
            # oaklib owns entities(); filter in Python until the SQL rewrite in issue #80.
            if self._matches_ontology(entity) and entity not in self.excluded_entities:
                ontology_class = self._create_ontology_class(entity, is_obsolete=False)
                yield ontology_class

        # Process obsolete entities
        for obsolete_entity in tqdm(
            self.adapter.obsoletes(),
            desc=f"Extracting {self.ontology} classes (obsolete)",
            unit="entity",
        ):
            if self._matches_ontology(obsolete_entity) and obsolete_entity not in self.excluded_entities:
                ontology_class = self._create_ontology_class(obsolete_entity, is_obsolete=True)
                yield ontology_class

    def get_relations_closure(
        self, closure: str | Iterable[str] = "combined", ontology_terms: list[OntologyClass] | None = None
    ) -> tuple[list[dict], list[OntologyClass]]:
        """
        Retrieve ontology direct relations + ancestry closure for the configured ontology.

        :param closure: A closure spec — either a single string or an iterable of strings drawn from
            {'combined', 'isa', 'partof', 'all', 'none'}. Multiple values combine: e.g.
            ``['combined', 'isa']`` emits both ``entailed_isa_partof_closure`` and
            ``entailed_isa_closure``. ``'all'`` and ``'none'`` are convenience values and are each
            exclusive — passing either together with any other value raises ValueError.

            - 'combined': rdfs:subClassOf + BFO:0000050 → entailed_isa_partof_closure (Sierra default)
            - 'isa': rdfs:subClassOf → entailed_isa_closure
            - 'partof': BFO:0000050 → entailed_partof_closure
            - 'all': expands to 'combined', 'isa', and 'partof'
            - 'none': no ancestry closure; only direct relationships
        :param ontology_terms: List of OntologyClass objects (default: None).
        :return: Tuple of (ontology_relations, updated_ontology_terms).
        """
        ontology_terms_dict = {term.id: term for term in (ontology_terms or [])}
        ontology_relations = []
        for relation in self._iter_relation_objects(closure):
            if relation.subject in ontology_terms_dict:
                ontology_terms_dict[relation.subject].relations.append(relation)
            ontology_relations.append(json_dumper.to_dict(relation))
        return ontology_relations, list(ontology_terms_dict.values())

    def iter_relations_closure(self, closure: str | Iterable[str] = "combined") -> Iterator[dict]:
        """Yield relation dictionaries without retaining classes or embedding relations."""
        for relation in self._iter_relation_objects(closure):
            yield json_dumper.to_dict(relation)

    def _iter_relation_objects(self, closure: str | Iterable[str]) -> Iterator[OntologyRelation]:
        """Yield direct and ancestry relations using the same rules for both loading modes."""
        closures = _normalize_closure_spec(closure)
        # Direct relationships: union of all predicates across the requested closures.
        # For 'none' alone, fall back to the combined predicate set so direct relationships still emit.
        if closures == ("none",):
            direct_predicates = ["rdfs:subClassOf", "BFO:0000050"]
            ancestry_specs = []
        else:
            direct_predicates_set: set[str] = set()
            ancestry_specs = []
            for c in closures:
                preds, name = _CLOSURE_SPECS[c]
                direct_predicates_set.update(preds)
                ancestry_specs.append((preds, name))
            direct_predicates = list(direct_predicates_set)

        # Get all relevant entities in one pass
        logger.info("Collecting relevant entities...")
        relevant_entities = {
            entity
            for entity in self.adapter.entities()
            if self._matches_ontology(entity) and entity not in self.excluded_entities
        }
        logger.info(f"Found {len(relevant_entities)} relevant entities")

        # Process all direct relationships in one batch
        logger.info("Processing direct relationships...")
        relationship_count = 0
        predicate_set = set(direct_predicates)

        # Get all relationships at once and filter as we process them
        for subject, predicate, obj in self.adapter.relationships():
            # Direct oaklib relations obey the same either-endpoint exclusion rule as SQL.
            if subject in relevant_entities and obj not in self.excluded_entities and predicate in predicate_set:
                yield OntologyRelation(subject=subject, predicate=predicate, object=obj, type="nmdc:OntologyRelation")
                relationship_count += 1

        logger.info(f"Processed {relationship_count} direct relationships")

        ancestry_count = 0
        if not ancestry_specs:
            logger.info("closure='none': skipping ancestry computation.")
        else:
            logger.info(
                f"Processing ancestry relationships across {len(ancestry_specs)} closure type(s): "
                + ", ".join(name for _, name in ancestry_specs)
            )
            for preds, closure_predicate_name in ancestry_specs:
                pairs = self._ancestry_pairs_from_entailed_edge(preds, relevant_entities)
                for subject, obj in tqdm(
                    pairs,
                    desc=f"Emitting {self.ontology} {closure_predicate_name}",
                    unit="pair",
                ):
                    yield OntologyRelation(
                        subject=subject, predicate=closure_predicate_name, object=obj, type="nmdc:OntologyRelation"
                    )
                    ancestry_count += 1
            logger.info(f"Processed {ancestry_count} ancestry relationships")

        logger.info(f"Total relations: {relationship_count + ancestry_count}")


def _normalize_closure_spec(closure) -> tuple:
    """
    Normalize the ``closure`` argument to a deduped tuple in stable order.

    Accepts a single string or any iterable of strings. Returns a tuple of concrete
    closure names: a subset of ``('combined', 'isa', 'partof')``, or the single-element
    tuple ``('none',)``.

    Convenience values:
      - ``'all'``: expands to ``('combined', 'isa', 'partof')``. Exclusive — cannot be
        combined with any other value.
      - ``'none'``: emit no ancestry closure. Exclusive — cannot be combined with any
        other value.

    Raises ``ValueError`` on unknown values or on illegal combinations.
    """
    if isinstance(closure, str):
        items = [closure]
    else:
        items = list(closure)
    if not items:
        raise ValueError(f"closure must include at least one value from {VALID_CLOSURES}; got empty.")
    seen: list[str] = []
    for c in items:
        if c not in VALID_CLOSURES:
            raise ValueError(f"Unknown closure {c!r}; expected one of {VALID_CLOSURES}.")
        if c not in seen:
            seen.append(c)

    for exclusive in ("all", "none"):
        if exclusive in seen and len(seen) > 1:
            others = [c for c in seen if c != exclusive]
            raise ValueError(f"closure={exclusive!r} is exclusive; cannot be combined with {others}.")

    if seen == ["all"]:
        return _ALL_CLOSURES
    return tuple(seen)
