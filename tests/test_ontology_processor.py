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
