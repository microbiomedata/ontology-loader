import pytest
from nmdc_schema.nmdc import OntologyClass, OntologyRelation
from ontology_loader.mongodb_loader import MongoDBLoader, Report
from ontology_loader.utils import load_yaml_from_package
from unittest.mock import MagicMock


@pytest.fixture()
def schema_view():
    """Load the NMDC schema view."""
    return load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")


@pytest.fixture()
def mock_mongo_loader(schema_view):
    """Mock MongoDBLoader to prevent actual database interactions."""
    loader = MongoDBLoader(schema_view)
    loader.client = MagicMock()
    loader.db = MagicMock()
    loader.db.get_collection = MagicMock()
    return loader


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.create_collection.return_value = MagicMock()
    return db


@pytest.fixture
def mock_ontology_classes():
    return [
        OntologyClass(id="ONT:001", name="Term1", type="nmdc:OntologyClass"),
        OntologyClass(id="ONT:002", name="Term2", type="nmdc:OntologyClass")
    ]


@pytest.fixture
def mock_ontology_relations():
    return [
        OntologyRelation(subject="ONT:001", predicate="related_to", object="ONT:002", type="nmdc:OntologyRelation"),
        OntologyRelation(subject="ONT:002", predicate="part_of", object="ONT:003", type="nmdc:OntologyRelation")
    ]


def test_upsert_new_ontology_data(mock_db, mock_ontology_classes, mock_ontology_relations):
    loader = MongoDBLoader()
    loader.db = mock_db
    class_collection = mock_db.create_collection.return_value

    mock_query_result = MagicMock()
    mock_query_result.rows = []
    mock_query_result.num_rows = 0  # Ensuring num_rows behaves like an integer

    class_collection.find.return_value = mock_query_result  # Mocking find() response

    report = loader.upsert_ontology_data(mock_ontology_classes, mock_ontology_relations)

    assert isinstance(report[0], Report)  # Class updates report
    assert isinstance(report[1], Report)  # Class insertions report
    assert isinstance(report[2], Report)  # Relation insertions report
    assert len(report[1].records) == len(mock_ontology_classes)  # All classes inserted
    assert len(report[2].records) == len(mock_ontology_relations)  # All relations inserted


def test_upsert_existing_ontology_data(mock_db, mock_ontology_classes):
    loader = MongoDBLoader()
    loader.db = mock_db
    class_collection = mock_db.create_collection.return_value

    existing_doc = {"id": "ONT:001", "name": "OldTerm", "type": "nmdc:OntologyClass"}

    mock_query_result = MagicMock()
    mock_query_result.rows = [existing_doc]
    mock_query_result.num_rows = 1  # Ensuring num_rows behaves like an integer

    class_collection.find.return_value = mock_query_result  # Mocking find() response

    report = loader.upsert_ontology_data(mock_ontology_classes, [])
    assert len(report[0].records) == 2  # One record should be updated
    assert len(report[1].records) == 0  # One record should be inserted (new class)


def test_handle_disappearing_relations(mock_db, mock_ontology_classes, mock_ontology_relations):
    loader = MongoDBLoader()
    loader.db = mock_db
    relation_collection = mock_db.create_collection.return_value

    mock_query_result = MagicMock()
    mock_query_result.rows = [
        {"subject": "ONT:001", "predicate": "entailed_isa_partof_closure",
         "object": "ONT:002", "type": "nmdc:OntologyRelation"},
        {"subject": "ONT:002", "predicate": "entailed_isa_partof_closure",
         "object": "ONT:003", "type": "nmdc:OntologyRelation"}
    ]
    mock_query_result.num_rows = 2  # Ensuring num_rows behaves like an integer

    relation_collection.find.return_value = mock_query_result  # Mocking find() response
    relation_collection.upsert = MagicMock()  # Mock upsert method

    updated_relations = [
        OntologyRelation(subject="ONT:001", predicate="related_to", object="ONT:003", type="nmdc:OntologyRelation")
        # Changed relation
    ]

    def mock_upsert(data, filter_fields, update_fields=None):
        for obj in data:
            if "subject" in obj:
                for ontology_class in mock_ontology_classes:
                    if ontology_class.id == obj["subject"]:
                        ontology_class.relations = [
                            OntologyRelation(
                                subject=obj["subject"],
                                predicate=obj["predicate"],
                                object=obj["object"],
                                type="nmdc:OntologyRelation",
                            )
                        ]

    relation_collection.upsert.side_effect = mock_upsert  # Simulate relation updates

    loader.upsert_ontology_data(mock_ontology_classes, updated_relations)

    # Ensure upsert was called
    relation_collection.upsert.assert_called()

    # Verify old relations were replaced
    assert len(mock_ontology_classes[0].relations) == 1
    assert mock_ontology_classes[0].relations[0].object == "ONT:003"
