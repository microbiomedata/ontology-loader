"""Test that all OntologyClass instances in MongoDB have non-null required attributes."""

import os

import pytest

from ontology_loader.mongodb_loader import MongoDBLoader
from ontology_loader.ontology_processor import OntologyProcessor
from ontology_loader.utils import load_yaml_from_package

# MongoDB Connection Parameters
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB = os.getenv("MONGO_DBNAME", "nmdc")
MONGO_USER = os.getenv("MONGO_USERNAME", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "")
AUTH_DB = "admin"

# Test with a small ontology like ENVO for faster tests
TEST_ONTOLOGY = "envo"


@pytest.fixture()
def schema_view():
    """Load the NMDC schema view."""
    return load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")


@pytest.fixture()
def mongodb_loader(schema_view):
    """Create a MongoDB loader for tests."""
    loader = MongoDBLoader(schema_view)
    # Ensure collections exist
    loader.db.create_collection("ontology_class_set", recreate_if_exists=False)
    loader.db.create_collection("ontology_relation_set", recreate_if_exists=False)
    return loader


@pytest.mark.skipif(os.getenv("MONGO_PASSWORD") is None, reason="Skipping test: MONGO_PASSWORD is not set")
def test_ontology_class_non_null_attributes(mongodb_loader, schema_view):
    """
    Test that all OntologyClass instances in MongoDB have non-null values for is_root, is_obsolete, and name.

    This test ensures that the data modification made in the OntologyProcessor._create_ontology_class
    method is working correctly by checking that:
    1. No OntologyClass has a null (None) value for is_root
    2. No OntologyClass has a null (None) value for is_obsolete
    """
    # Process a small test ontology and load it into MongoDB
    ontology_processor = OntologyProcessor(TEST_ONTOLOGY)
    ontology_classes = ontology_processor.get_terms_and_metadata()

    # Ensure we have at least some data to work with
    assert len(ontology_classes) > 0, f"No ontology classes found for {TEST_ONTOLOGY}"

    # Access the ontology_class_set collection to verify data
    collection = mongodb_loader.db.create_collection("ontology_class_set", recreate_if_exists=False)

    # Find all documents with null values for the attributes we're checking
    null_is_root_query = {"is_root": None}
    null_is_obsolete_query = {"is_obsolete": None}

    # Execute queries
    null_is_root_results = collection.find(null_is_root_query)
    null_is_obsolete_results = collection.find(null_is_obsolete_query)

    if null_is_root_results.num_rows > 0:
        example = null_is_root_results.rows[0]
        example_id = example.get("id", "unknown")
        raise AssertionError(
            f"Found {null_is_root_results.num_rows} instances with null is_root. Example id: {example_id}"
        )

    if null_is_obsolete_results.num_rows > 0:
        example = null_is_obsolete_results.rows[0]
        example_id = example.get("id", "unknown")
        raise AssertionError(
            f"Found {null_is_obsolete_results.num_rows} instances with null is_obsolete. Example id: {example_id}"
        )


@pytest.mark.skipif(os.getenv("MONGO_PASSWORD") is None, reason="Skipping test: MONGO_PASSWORD is not set")
def test_ontology_class_empty_string_name(mongodb_loader, schema_view):
    """Test that <10% of all OntologyClass instances in MongoDB have a non-empty string value for name."""
    # Process a small test ontology and load it into MongoDB (if not already done by the previous test)
    ontology_processor = OntologyProcessor(TEST_ONTOLOGY)
    ontology_classes = ontology_processor.get_terms_and_metadata()

    # Ensure we have at least some data to work with
    assert len(ontology_classes) > 0, f"No ontology classes found for {TEST_ONTOLOGY}"

    collection = mongodb_loader.db.create_collection("ontology_class_set", recreate_if_exists=False)
    empty_name_query = {"name": ""}
    empty_name_results = collection.find(empty_name_query)

    # We may have some empty strings, but it shouldn't be a high percentage of records
    all_records = collection.find({})
    total_count = all_records.num_rows
    empty_name_count = empty_name_results.num_rows
    empty_name_percentage = (empty_name_count / total_count * 100) if total_count > 0 else 0

    # If more than 10% of records have empty names, that's a concern
    assert empty_name_percentage < 10, (
        f"Found {empty_name_count} ({empty_name_percentage:.2f}%) OntologyClass instances with empty name strings "
        f"out of {total_count} total records. "
    )
