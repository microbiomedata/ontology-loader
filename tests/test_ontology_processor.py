"""Test OntologyProcessor class and its methods."""

import itertools

import pytest

from src.ontology_loader.ontology_processor import OntologyProcessor


@pytest.mark.parametrize(
    "ontology_name, entity_id, expected",
    [
        # Same-case prefixes (the historical path)
        ("envo", "ENVO:00002005", True),
        ("envo", "envo:00002005", True),
        ("uberon", "UBERON:0000001", True),
        ("po", "PO:0000001", True),
        # Mixed-case prefixes that the prior `.upper()`-based filter dropped silently
        ("ncbitaxon", "NCBITaxon:9606", True),
        ("ncbitaxon", "NCBITAXON:9606", True),
        ("ncbitaxon", "ncbitaxon:9606", True),
        ("chebi", "CHEBI:12345", True),
        # Wrong ontology — must reject
        ("envo", "UBERON:0000001", False),
        ("ncbitaxon", "PR:Q9606", False),
        # Missing colon — must reject
        ("ncbitaxon", "NCBITaxon", False),
        ("envo", "ENVO", False),
        ("envo", "", False),
    ],
)
def test_matches_ontology(ontology_name, entity_id, expected):
    """`_matches_ontology` compares the CURIE head case-insensitively to the configured ontology."""

    # Avoid the heavy `OntologyProcessor.__init__` (which downloads + opens sqlite); the method
    # only depends on `self._ontology_lc`, so a minimal stand-in object is sufficient.
    class _Fake:
        pass

    fake = _Fake()
    fake._ontology_lc = ontology_name.lower()
    assert OntologyProcessor._matches_ontology(fake, entity_id) is expected


def test_ontology_processor():
    """Test OntologyProcessor initialization and ontology retrieval."""
    ontology_name = "envo"
    processor = OntologyProcessor(ontology_name)

    assert processor.ontology == ontology_name
    assert processor.ontology_db_path.exists()


def test_get_terms_and_metadata():
    """Test retrieval of ontology terms and metadata."""
    processor = OntologyProcessor("envo")
    ontology_classes = processor.get_terms_and_metadata()

    assert isinstance(ontology_classes, list)
    for ontology_class in ontology_classes:
        assert "id" in ontology_class and "type" in ontology_class
        assert ontology_class["type"] == "nmdc:OntologyClass"


def test_get_relations_closure():
    """Test retrieval of ontology relations closure."""
    processor = OntologyProcessor("envo")
    ontology_relations, _ = processor.get_relations_closure()

    assert isinstance(ontology_relations, list)
    assert all(isinstance(rel, dict) for rel in ontology_relations)
    for rel in ontology_relations:
        assert "subject" in rel
        assert "predicate" in rel
        assert "object" in rel


@pytest.mark.parametrize(
    "predicate",
    [
        "rdfs:subClassOf",
        # part_of: almost never reflexive in entailed_edge itself (2 self-loops out of 36,857
        # envo edges, vs subClassOf's 6,906 out of 75,484) -- catches the gap Copilot review found
        # on this PR, where relying on the table's own reflexivity silently dropped self-pairs.
        "BFO:0000050",
    ],
)
def test_ancestry_pairs_from_entailed_edge_matches_adapter_ancestors(predicate):
    """
    The bulk entailed_edge query must match the old per-entity adapter.ancestors() loop exactly.

    See https://github.com/microbiomedata/ontology-loader/issues/18: same result, one bulk query
    instead of one call per entity. Checked against real envo entities, not mocks: the risk here
    is a semantic mismatch between semsql's pre-materialized closure and oaklib's on-the-fly
    traversal (e.g. reflexivity, or a predicate not actually being entailed), which a mock can't
    catch because it can't be wrong about what oaklib itself does.
    """
    processor = OntologyProcessor("envo", force_refresh=False)

    sample_entities = list(
        itertools.islice(
            (entity for entity in processor.adapter.entities(filter_obsoletes=True) if entity.startswith("ENVO:")),
            20,
        )
    )
    assert len(sample_entities) == 20, "sanity check: envo should have at least 20 non-obsolete classes"
    relevant_entities = set(entity for entity in processor.adapter.entities() if processor._matches_ontology(entity))
    sample_entities_set = set(sample_entities)

    # One bulk query for all sample entities at once -- this is the whole point of the change.
    # Only retain pairs for the sampled subjects: the full closure can be tens of millions of
    # rows at NCBITaxon scale, and this test only needs 20 of them (Copilot review on this PR).
    #
    # Kept as a list, not deduplicated into the per-subject sets until after the uniqueness
    # check below: a set would silently swallow a duplicate (subject, object) emission, which is
    # exactly the shape of bug this test needs to catch (Copilot review on this PR -- an entity
    # whose self-pair entailed_edge already had natively could otherwise be yielded twice, once
    # from the SQL scan and once from the explicit reflexive union).
    pairs = list(processor._ancestry_pairs_from_entailed_edge([predicate], relevant_entities))
    sample_pairs = [(s, o) for s, o in pairs if s in sample_entities_set]
    assert len(sample_pairs) == len(set(sample_pairs)), "duplicate (subject, object) pair emitted"

    new_ancestors_by_subject = {}
    for subject, obj in sample_pairs:
        new_ancestors_by_subject.setdefault(subject, set()).add(obj)

    for entity in sample_entities:
        old_ancestors = {
            a
            for a in processor.adapter.ancestors(entity, reflexive=True, predicates=[predicate])
            if processor._matches_ontology(a)
        }
        new_ancestors = new_ancestors_by_subject.get(entity, set())
        assert new_ancestors == old_ancestors, f"mismatch for {entity} on predicate {predicate}"


def test_ancestry_pairs_from_entailed_edge_excludes_deprecated_subjects():
    """
    A deprecated/obsolete entity must not appear as a subject or object in the results.

    The old per-entity loop's start set came from ``self.adapter.entities()``, which defaults to
    ``filter_obsoletes=True`` and so never iterated a deprecated entity in the first place. A bare
    id-prefix match on ``entailed_edge`` does not know about deprecation, and envo's own
    ``entailed_edge`` does contain rows for deprecated subjects (453 for `rdfs:subClassOf` alone,
    verified directly against the sqlite file) -- so without filtering against the real entity set,
    an obsolete class could leak into the closure. Copilot review on this PR.
    """
    processor = OntologyProcessor("envo", force_refresh=False)

    obsolete_entities = [e for e in processor.adapter.obsoletes() if processor._matches_ontology(e)]
    assert obsolete_entities, "sanity check: envo should have at least one obsolete class"
    an_obsolete_entity = obsolete_entities[0]

    relevant_entities = set(entity for entity in processor.adapter.entities() if processor._matches_ontology(entity))
    assert an_obsolete_entity not in relevant_entities, (
        "sanity check: obsolete entities excluded from relevant_entities"
    )

    pairs = list(processor._ancestry_pairs_from_entailed_edge(["rdfs:subClassOf"], relevant_entities))

    subjects_and_objects = {s for s, _ in pairs} | {o for _, o in pairs}
    assert an_obsolete_entity not in subjects_and_objects
