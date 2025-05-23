"""Test OntologyProcessor class and its methods."""

from src.ontology_loader.ontology_processor import OntologyProcessor


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
