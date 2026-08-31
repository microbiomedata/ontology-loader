"""Test OntologyProcessor class and its methods."""

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


def test_ancestry_pairs_from_entailed_edge_matches_adapter_ancestors():
    """
    The bulk entailed_edge query must match the old per-entity adapter.ancestors() loop exactly.

    See https://github.com/microbiomedata/ontology-loader/issues/18: same result, one bulk query
    instead of one call per entity. Checked against real envo entities, not mocks: the risk here
    is a semantic mismatch between semsql's pre-materialized closure and oaklib's on-the-fly
    traversal (e.g. reflexivity, or a predicate not actually being entailed), which a mock can't
    catch because it can't be wrong about what oaklib itself does.
    """
    processor = OntologyProcessor("envo", force_refresh=False)

    sample_entities = [
        entity for entity in processor.adapter.entities(filter_obsoletes=True) if entity.startswith("ENVO:")
    ][:20]
    assert len(sample_entities) == 20, "sanity check: envo should have at least 20 non-obsolete classes"

    # One bulk query for all sample entities at once -- this is the whole point of the change.
    pairs = processor._ancestry_pairs_from_entailed_edge(["rdfs:subClassOf"])
    new_ancestors_by_subject = {}
    for subject, obj in pairs:
        new_ancestors_by_subject.setdefault(subject, set()).add(obj)

    for entity in sample_entities:
        old_ancestors = {
            a
            for a in processor.adapter.ancestors(entity, reflexive=True, predicates=["rdfs:subClassOf"])
            if processor._matches_ontology(a)
        }
        new_ancestors = new_ancestors_by_subject.get(entity, set())
        assert new_ancestors == old_ancestors, f"mismatch for {entity}"
