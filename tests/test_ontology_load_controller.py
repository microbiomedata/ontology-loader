"""Test the OntologyLoader class."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ontology_loader.mongodb_loader import MongoDBLoader, _handle_obsolete_terms
from ontology_loader.ontology_load_controller import OntologyLoaderController
from ontology_loader.utils import load_yaml_from_package


@pytest.fixture
def schema_view():
    """
    Load the NMDC schema view.

    :return: NMDC schema, schemaview object.
    """
    return load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")


@pytest.fixture
def ontology_loader():
    """
    Initialize the OntologyLoader with test parameters.

    :return: OntologyLoaderController instance.
    """
    return OntologyLoaderController(
        source_ontology="envo",
        output_directory=tempfile.gettempdir(),
        generate_reports=True,
    )


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_ontology_loader_run(schema_view, ontology_loader):
    """
    Test running the ontology loader and inserting data into MongoDB.

    :param schema_view: NMDC schema view.
    :param ontology_loader: OntologyLoaderController instance.
    """
    ontology_loader.run_ontology_loader()

    # Connect to MongoDB and verify inserted data
    db_manager = MongoDBLoader(
        schema_view=schema_view,
    )

    # Check ontology class insertions
    collection = db_manager.db.create_collection("ontology_class_set", recreate_if_exists=False)
    query_results = collection.find({})
    assert query_results.num_rows > 0, "No ontology classes were inserted into MongoDB"

    # Check ontology relation insertions
    relation_collection = db_manager.db.create_collection("ontology_relation_set", recreate_if_exists=False)
    relation_results = relation_collection.find({})
    assert relation_results.num_rows > 0, "No ontology relations were inserted into MongoDB"


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_ontology_loader_reports(ontology_loader):
    """
    Test whether reports are generated after running the ontology loader.

    :param ontology_loader: OntologyLoaderController instance.
    """
    ontology_loader.run_ontology_loader()

    # Verify reports exist in the output directory
    updates_report = Path(ontology_loader.output_directory) / "ontology_updates.tsv"
    insertions_report = Path(ontology_loader.output_directory) / "ontology_inserts.tsv"

    assert updates_report.exists(), "Updates report was not generated"
    assert insertions_report.exists(), "Insertions report was not generated"

    # Check report file contents
    with updates_report.open() as f:
        lines = f.readlines()
        assert len(lines) > 0, "Updates report is empty"

    with insertions_report.open() as f:
        lines = f.readlines()
        assert len(lines) > 0, "Insertions report is empty"


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_obsolete_handling_in_ontology_loader():
    """Test the handling of obsolete terms when processing ontology data."""
    # Use a custom temp directory for this test

    # Connect to MongoDB
    schema_view = load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")
    db_manager = MongoDBLoader(schema_view=schema_view)

    # Create a fake obsolete term and add it to the database to ensure we have something to test with
    class_collection = db_manager.db.create_collection("ontology_class_set", recreate_if_exists=False)
    test_obsolete_term = {
        "id": "TEST:0000001",
        "name": "Test Obsolete Term",
        "type": "nmdc:OntologyClass",
        "is_obsolete": True,
        "relations": ["test_relation"],
    }

    class_collection.upsert(
        [test_obsolete_term], filter_fields=["id"], update_fields=["id", "name", "type", "is_obsolete"]
    )

    # Create a test relation that references the obsolete term (will be processed by _handle_obsolete_terms)
    relation_collection = db_manager.db.create_collection("ontology_relation_set", recreate_if_exists=False)
    test_relation = {
        "subject": "TEST:0000001",
        "predicate": "test_relation",
        "object": "TEST:0000002",
        "type": "nmdc:OntologyRelation",
    }
    relation_collection.upsert(
        [test_relation],
        filter_fields=["subject", "predicate", "object"],
        update_fields=["subject", "predicate", "object", "type"],
    )

    # Create a test class that's not obsolete (for comparison)
    normal_term = {
        "id": "TEST:0000002",
        "name": "Test Normal Term",
        "type": "nmdc:OntologyClass",
        "is_obsolete": False,
        "relations": ["test_relation"],
    }
    class_collection.upsert([normal_term], filter_fields=["id"], update_fields=["id", "name", "type", "is_obsolete"])

    # Directly call the _handle_obsolete_terms function to test its behavior
    # This ensures we're explicitly testing the obsolete handling without
    # going through the whole loader process
    _handle_obsolete_terms(["TEST:0000001"], class_collection, relation_collection)

    # Check that our test obsolete term is still marked as obsolete in the database
    obsolete_query_results = class_collection.find({"id": "TEST:0000001"})
    assert obsolete_query_results.num_rows > 0, "Test obsolete term not found"
    assert obsolete_query_results.rows[0]["is_obsolete"] is True

    # Check that the relation referencing our obsolete term has been removed
    subject_relations = relation_collection.find({"subject": "TEST:0000001"})
    assert subject_relations.num_rows == 0, "Found relations with obsolete term as subject"


def test_obsolete_handling_with_mocks():
    """Test obsolete term handling with mocks to check the expected behavior."""
    # Import the functions we need to test
    from ontology_loader.mongodb_loader import _handle_obsolete_terms

    # Create mock collections
    class_collection = MagicMock()
    relation_collection = MagicMock()

    # Create a dictionary instead of a class because _handle_obsolete_terms
    # expects to be able to modify the term with dictionary operations
    term_dict = {
        "id": "ENVO:0000001",
        "name": "Test Term",
        "relations": ["some_relation"],
        "is_obsolete": False,
        "type": "nmdc:OntologyClass",
    }

    # Configure the find method to return our test term
    mock_query_result = MagicMock()
    mock_query_result.rows = [term_dict]  # Use the dictionary directly
    mock_query_result.num_rows = 1

    # Set up class_collection.find to return our mock_query_result
    # Use side_effect to handle different term_id values
    def mock_find(criteria):
        # If looking for one of our test terms, return it
        if criteria.get("id") in ["ENVO:0000001", "ENVO:0000002"]:
            return mock_query_result
        # Otherwise return empty result
        empty_result = MagicMock()
        empty_result.rows = []
        empty_result.num_rows = 0
        return empty_result

    class_collection.find.side_effect = mock_find

    # Capture what gets passed to upsert
    upserted_data = None

    def capture_upsert(data, filter_fields, update_fields=None):
        nonlocal upserted_data
        upserted_data = data[0] if data else None

    class_collection.upsert.side_effect = capture_upsert

    # Define obsolete terms
    obsolete_terms = ["ENVO:0000001", "ENVO:0000002"]

    # Call the function we're testing
    _handle_obsolete_terms(obsolete_terms, class_collection, relation_collection)

    # Verify the upserted data has the correct values
    assert upserted_data is not None, "No data was passed to upsert"
    assert upserted_data["is_obsolete"] is True, "Term was not marked as obsolete"
    assert upserted_data["relations"] == [], "Relations were not cleared"

    # Verify relations were deleted
    relation_collection.delete.assert_called_with(
        {"$or": [{"subject": {"$in": obsolete_terms}}, {"object": {"$in": obsolete_terms}}]}
    )

    # Verify class_collection.upsert was called
    class_collection.upsert.assert_called()
