"""Ontology Processor class to process ontology terms and relations."""

import gzip
import logging
import shutil
import sqlite3

import pystow
from linkml_runtime.dumpers import json_dumper
from nmdc_schema.nmdc import OntologyClass, OntologyRelation
from oaklib import get_adapter
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


def _create_relation(subject, predicate, obj, ontology_terms_dict):
    """
    Create an ontology relation and update related ontology terms.

    :param subject: Subject of the relation
    :param predicate: Predicate of the relation
    :param obj: Object of the relation
    :param ontology_terms_dict: Dictionary of ontology terms for fast lookup
    :return: Dictionary representation of the relation
    """
    ontology_relation = OntologyRelation(
        subject=subject,
        predicate=predicate,
        object=obj,
        type="nmdc:OntologyRelation",
    )

    # Update the term's relations list if it exists in our dictionary
    if subject in ontology_terms_dict:
        ontology_terms_dict[subject].relations.append(ontology_relation)

    # Convert and return the relation dictionary
    return json_dumper.to_dict(ontology_relation)


class OntologyProcessor:
    """Ontology Processor class to process ontology terms and relations."""

    def __init__(self, ontology: str, force_refresh: bool = True):
        """
        Initialize the OntologyProcessor with a given SQLite ontology.

        :param ontology: The ontology prefix (e.g., "envo", "go", "uberon", etc.)
        :param force_refresh: If True (default, preserves 0.2.x behavior), wipe any cached pystow
            directory for this ontology and re-download from S3. If False, reuse the cached
            artifact when present; pystow.ensure() still downloads if the cache is empty.
        """
        self.ontology = ontology

        self.force_refresh = force_refresh
        # Cache the lowercased ontology name once
        self._ontology_lc = ontology.lower()

        self.ontology_db_path = self.download_and_prepare_ontology()
        self.adapter = get_adapter(f"sqlite:{self.ontology_db_path}")
        self.adapter.precompute_lookups()  # Optimize lookups

        # Cache root terms for efficient lookups
        self.root_terms = set(self.adapter.roots())

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
            alternative_names=self.adapter.entity_aliases(entity_id) or [],
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

    def _ancestry_pairs_from_entailed_edge(self, predicates: list):
        """
        Return every (subject, object) ancestry pair for ``predicates``, from semsql's own table.

        Reads directly from ``entailed_edge`` instead of issuing one ``adapter.ancestors()`` call
        per entity. ``entailed_edge`` already holds the full transitive closure that ``ancestors()`` computes
        on the fly, reflexive edges included (verified: a self-loop for each entity is present in
        the table, matching the ``reflexive=True`` semantics the old per-entity loop asked for
        explicitly). One indexed scan replaces ~2.7M individual SQL traversals for NCBITaxon.
        Measured against the real NCBITaxon semsql database: 29m9s (the old per-entity loop, from
        a real run's own log) versus 13.0s for an unfiltered bulk scan of all 51,991,442 rows --
        about 134x faster. Correctness verified against the old ``adapter.ancestors()`` output on
        real envo entities (exact match, including a multi-ancestor case).

        ``DISTINCT`` is unconditional, not just for multi-predicate closures like ``combined``:
        the old code deduplicated ancestors per entity via a Python ``set()`` regardless of how
        many predicates were requested, and a ``combined`` closure can reach the same (subject,
        object) pair via more than one predicate.

        :param predicates: List of predicate CURIEs (e.g. ``["rdfs:subClassOf"]`` or
            ``["rdfs:subClassOf", "BFO:0000050"]``) to include in this closure.
        :return: Generator of (subject, object) tuples, both sides filtered to this ontology.
        """
        placeholders = ",".join("?" for _ in predicates)
        prefix_pattern = f"{self._ontology_lc}:%"
        query = (
            f"SELECT DISTINCT subject, object FROM entailed_edge "  # noqa: S608 -- predicates
            f"WHERE predicate IN ({placeholders}) AND subject LIKE ? AND object LIKE ?"
        )
        params = [*predicates, prefix_pattern, prefix_pattern]
        # A generator, not fetchall(): NCBITaxon-scale closures are tens of millions of rows, and
        # materializing that as a Python list would reintroduce the same peak-memory problem this
        # change exists to avoid, even though the SQL scan itself is fast. The `with` block stays
        # open across the caller's iteration because a generator suspends at `yield` rather than
        # returning, keeping the connection alive until the caller is done or the generator is
        # garbage-collected. Copilot review on this PR.
        with sqlite3.connect(self.ontology_db_path) as con:
            yield from con.execute(query, params)

    def get_terms_and_metadata(self):
        """Retrieve all terms that belong to this ontology and return a list of OntologyClass objects."""
        ontology_classes = []

        # Process non-obsolete entities
        for entity in tqdm(
            self.adapter.entities(filter_obsoletes=True),
            desc=f"Extracting {self.ontology} classes (non-obsolete)",
            unit="entity",
        ):
            if self._matches_ontology(entity):
                ontology_class = self._create_ontology_class(entity, is_obsolete=False)
                ontology_classes.append(ontology_class)

        # Process obsolete entities
        for obsolete_entity in tqdm(
            self.adapter.obsoletes(),
            desc=f"Extracting {self.ontology} classes (obsolete)",
            unit="entity",
        ):
            if self._matches_ontology(obsolete_entity):
                ontology_class = self._create_ontology_class(obsolete_entity, is_obsolete=True)
                ontology_classes.append(ontology_class)

        return ontology_classes

    def get_relations_closure(self, closure="combined", ontology_terms: list = None) -> tuple:
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

        ontology_relations = []

        # Create dictionary for fast lookup of ontology terms
        ontology_terms_dict = {term.id: term for term in (ontology_terms or [])}

        # Get all relevant entities in one pass
        logger.info("Collecting relevant entities...")
        relevant_entities = set(entity for entity in self.adapter.entities() if self._matches_ontology(entity))
        logger.info(f"Found {len(relevant_entities)} relevant entities")

        # Process all direct relationships in one batch
        logger.info("Processing direct relationships...")
        relationship_count = 0
        predicate_set = set(direct_predicates)

        # Get all relationships at once and filter as we process them
        for subject, predicate, obj in self.adapter.relationships():
            if subject in relevant_entities and predicate in predicate_set:
                relation_dict = _create_relation(subject, predicate, obj, ontology_terms_dict)
                ontology_relations.append(relation_dict)
                relationship_count += 1

        logger.info(f"Processed {relationship_count} direct relationships")

        if not ancestry_specs:
            logger.info("closure='none': skipping ancestry computation.")
        else:
            logger.info(
                f"Processing ancestry relationships across {len(ancestry_specs)} closure type(s): "
                + ", ".join(name for _, name in ancestry_specs)
            )
            ancestry_count = 0
            for preds, closure_predicate_name in ancestry_specs:
                pairs = self._ancestry_pairs_from_entailed_edge(preds)
                for subject, obj in tqdm(
                    pairs,
                    desc=f"Emitting {self.ontology} {closure_predicate_name}",
                    unit="pair",
                ):
                    relation_dict = _create_relation(subject, closure_predicate_name, obj, ontology_terms_dict)
                    ontology_relations.append(relation_dict)
                    ancestry_count += 1
            logger.info(f"Processed {ancestry_count} ancestry relationships")

        logger.info(f"Total relations: {len(ontology_relations)}")

        # Return the relations and updated ontology terms
        return ontology_relations, list(ontology_terms_dict.values())


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
