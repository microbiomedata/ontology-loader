"""Test the MongoDBLoader class."""

import tempfile
from dataclasses import asdict

import pytest
from nmdc_schema.nmdc import OntologyClass, OntologyRelation

from ontology_loader.mongodb_loader import MongoDBLoader
from ontology_loader.reporter import ReportWriter
from ontology_loader.utils import load_yaml_from_package


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


def test_insert_ontology_relations(schema_view):
    """Test inserting ontology relations."""
    loader = MongoDBLoader(schema_view)

    ontology_relations = [
        asdict(
            OntologyRelation(subject="nmdc:NC1",
                             predicate="nmdc:is_a",
                             object="nmdc:NC2",
                             type="nmdc:OntologyRelation")
        ),
        asdict(
            OntologyRelation(subject="nmdc:NC2",
                             predicate="nmdc:is_a",
                             object="nmdc:NC3",
                             type="nmdc:OntologyRelation")
        ),
    ]
    # creating this collection here, effectively wipes out any previous test data.
    collection = loader.db.create_collection("test_collection", recreate_if_exists=True)
    loader.insert_ontology_relations(ontology_relations, collection_name="test_collection")
    query_results = collection.find({"$or": [{"subject": "nmdc:NC1"}, {"subject": "nmdc:NC2"}]})

    rows = query_results.rows
    print("query_results", query_results)

    # Expected records
    expected_records = [
        {"subject": "nmdc:NC1",
         "predicate": "nmdc:is_a",
         "object": "nmdc:NC2",
         "type": "nmdc:OntologyRelation"},
        {"subject": "nmdc:NC2",
         "predicate": "nmdc:is_a",
         "object": "nmdc:NC3",
         "type": "nmdc:OntologyRelation"},
    ]

    for expected_record in expected_records:
        assert expected_record in rows
