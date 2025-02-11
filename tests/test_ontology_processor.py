from src.ontology_loader.ontology_processor import OntologyProcessor
from nmdc_schema.nmdc import OntologyClass


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
    assert all(isinstance(cls, OntologyClass) for cls in ontology_classes)


def test_get_relations_closure():
    """Test retrieval of ontology relations closure."""
    processor = OntologyProcessor("envo")
    ontology_relations = processor.get_relations_closure()

    assert isinstance(ontology_relations, list)
    assert all(isinstance(rel, dict) for rel in ontology_relations)
    assert all("subject" in rel and "predicate" in rel and "object" in rel for rel in ontology_relations)
