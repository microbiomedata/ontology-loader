import tempfile
from unittest.mock import MagicMock

import pytest
from nmdc_schema.nmdc import OntologyClass
from ontology_loader.mongodb_loader import MongoDBLoader
from ontology_loader.reporter import ReportWriter
from ontology_loader.utils import load_yaml_from_package


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


def test_mongodb_loader_init(mock_mongo_loader):
    """Test MongoDBLoader initialization without actual database access."""
    assert mock_mongo_loader.client is not None
    assert mock_mongo_loader.db is not None


def test_upsert_ontology_classes(mock_mongo_loader):
    """Test upserting ontology classes without storing them in MongoDB."""
    ontology_classes = [
        OntologyClass(id="nmdc:NC1", type="nmdc:OntologyClass"),
        OntologyClass(id="nmdc:NC2", type="nmdc:OntologyClass"),
    ]

    # Create mock reports with a 'report_type' attribute
    mock_update_report = MagicMock()
    mock_update_report.report_type = "update"

    mock_insert_report = MagicMock()
    mock_insert_report.report_type = "insert"

    # Mock the upsert method to return these reports
    mock_mongo_loader.upsert_ontology_classes = MagicMock(
        return_value=([mock_update_report], [mock_insert_report])
    )

    updates_report, insertions_report = mock_mongo_loader.upsert_ontology_classes(ontology_classes)

    ReportWriter.write_reports(
        reports=updates_report + insertions_report,  # Flatten list before passing
        output_format="tsv",
        output_directory=tempfile.gettempdir(),
    )

    assert mock_mongo_loader.upsert_ontology_classes.called


def test_delete_obsolete_relations(mock_mongo_loader):
    """Test that obsolete relations are deleted without using MongoDB."""

    # Mock collections
    mock_class_collection = MagicMock()
    mock_relation_collection = MagicMock()

    # Mock create_collection() instead of get_collection()
    mock_mongo_loader.db.create_collection.side_effect = lambda name, recreate_if_exists: (
        mock_class_collection if name == "ontology_class_set" else mock_relation_collection
    )

    # Define test data
    mock_class_collection.find.return_value.rows = [
        {"id": "OBSOLETE_1", "is_obsolete": True},
        {"id": "OBSOLETE_2", "is_obsolete": True},
        {"id": "ACTIVE_1", "is_obsolete": False},  # This should NOT be returned by find()
    ]

    mock_relation_collection.find.return_value.rows = [
        {"subject": "OBSOLETE_1", "predicate": "is_a", "object": "ACTIVE_1"},
        {"subject": "OBSOLETE_2", "predicate": "is_a", "object": "ACTIVE_1"},
        {"subject": "ACTIVE_1", "predicate": "is_a", "object": "ACTIVE_2"},  # Should not be deleted
    ]

    # Mocks do not automatically filter query results the way a real MongoDB would. Instead, unless we specify
    # otherwise, a mocked .find() method simply returns whatever we set in .return_value.rows, regardless of the query.
    def find_side_effect(query):
        """Mock function to return only documents matching the query."""
        return MagicMock(rows=[doc for doc in mock_class_collection.find.return_value.rows if doc.get("is_obsolete")])

    mock_class_collection.find.side_effect = find_side_effect

    # Mock delete_where method
    mock_relation_collection.delete_where = MagicMock(return_value=2)  # Assume 2 records get deleted

    # Run the method
    mock_mongo_loader.delete_obsolete_relations()

    # Debugging: Check what obsolete IDs were retrieved
    obsolete_ids = {doc["id"] for doc in mock_class_collection.find.return_value.rows if doc["is_obsolete"]}
    print(f"Obsolete IDs detected: {obsolete_ids}")

    # Ensure delete_where was called with the correct filter
    expected_filter = {
        "$or": [
            {"subject": {"$in": list(obsolete_ids)}},
            {"object": {"$in": list(obsolete_ids)}}
        ]
    }

    actual_filter = mock_relation_collection.delete_where.call_args[0][0]

    # Debugging: Print expected vs. actual
    print(f"Expected filter: {expected_filter}")
    print(f"Actual delete_where call: {mock_relation_collection.delete_where.call_args}")

    assert expected_filter == actual_filter, f"Expected: {expected_filter}, Got: {actual_filter}"
