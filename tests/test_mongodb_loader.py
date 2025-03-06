"""Test the MongoDBLoader class."""

import tempfile
from dataclasses import asdict
from nmdc_schema.nmdc import OntologyClass, OntologyRelation

from ontology_loader.mongodb_loader import MongoDBLoader
from ontology_loader.reporter import ReportWriter
from ontology_loader.utils import load_yaml_from_package
import pytest
from unittest.mock import MagicMock

@pytest.fixture()
def schema_view():
    """Load the NMDC schema view."""
    return load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")


def test_mongodb_loader_init(schema_view):
    """Test MongoDBLoader initialization and cleanup."""
    loader = MongoDBLoader(schema_view)

    assert loader.client is not None
    assert loader.db is not None


def test_upsert_ontology_classes(schema_view):
    """Test upserting ontology classes into MongoDB."""
    loader = MongoDBLoader(schema_view)

    ontology_classes = [
        OntologyClass(id="nmdc:NC1", type="nmdc:OntologyClass"),
        OntologyClass(id="nmdc:NC2", type="nmdc:OntologyClass"),
    ]

    # creating this collection here, effectively wipes out any previous test data.
    collection = loader.db.create_collection("test_collection", recreate_if_exists=True)
    updates_report, insertions_report = loader.upsert_ontology_classes(ontology_classes)
    loader.upsert_ontology_classes(ontology_classes, collection_name="test_collection")
    ReportWriter.write_reports(
        reports=[updates_report, insertions_report], output_format="tsv", output_directory=tempfile.gettempdir()
    )

    assert updates_report or insertions_report

    # Query the collection for unique IDs
    query_results = collection.find({"$or": [{"id": "nmdc:NC1"}, {"id": "nmdc:NC2"}]})  # Retrieve only the 'id' field
    rows = query_results.rows
    unique_ids = []
    for row in rows:
        unique_ids.append(row["id"])
    ids = set(unique_ids)
    assert "nmdc:NC1" in ids
    assert "nmdc:NC2" in ids


@pytest.fixture(scope="function")
def mongo_loader(schema_view):
    """Fixture to initialize MongoDBLoader and clean the database before and after the test."""
    loader = MongoDBLoader(schema_view)

    # Ensure collections exist and are cleaned up before the test
    class_collection = loader.db.get_collection("ontology_class_set")
    relation_collection = loader.db.get_collection("ontology_relation_set")

    class_collection.delete([{}])  # Use LinkML-store's delete method
    relation_collection.delete([{}])  # Delete all records

    yield loader  # Provide the loader to the test function

    # Cleanup after test
    class_collection.delete([{}])  # Delete all records
    relation_collection.delete([{}])  # Delete all records

def test_delete_obsolete_relations(mongo_loader):
    """Test that relations involving obsolete ontology classes are deleted."""

    # Insert test ontology classes using MongoDBLoader client
    class_collection = mongo_loader.db.get_collection("ontology_class_set")
    class_collection.insert({"id": "OBSOLETE_1", "is_obsolete": True})
    class_collection.insert({"id": "OBSOLETE_2", "is_obsolete": True})
    class_collection.insert({"id": "ACTIVE_1", "is_obsolete": False})  # Should NOT be deleted

    # Insert test ontology relations using MongoDBLoader client
    relation_collection = mongo_loader.db.get_collection("ontology_relation_set")
    relation_collection.insert({"subject": "OBSOLETE_1", "predicate": "is_a", "object": "ACTIVE_1"})
    relation_collection.insert({"subject": "ACTIVE_1", "predicate": "is_a", "object": "OBSOLETE_2"})
    relation_collection.insert({"subject": "ACTIVE_1", "predicate": "is_a", "object": "ACTIVE_2"})  # Should NOT be deleted

    # Run the deletion method
    mongo_loader.delete_obsolete_relations()

    # Extract only the rows from the query result
    remaining_relations = relation_collection.find({}).rows

    # Expected: Only the last relation should remain
    expected_remaining = [
        {"subject": "ACTIVE_1", "predicate": "is_a", "object": "ACTIVE_2"}
    ]

    assert len(remaining_relations) == len(expected_remaining)
    for relation in remaining_relations:
        # Ensure only expected relations exist (ignoring MongoDB's internal `_id` field)
        relation.pop("_id", None)
        assert relation in expected_remaining
