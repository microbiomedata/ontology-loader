"""Test the OntologyLoader class."""

import os
import pytest
from pathlib import Path
from src.ontology_loader.loader import OntologyLoader
from src.ontology_loader.mongodb_loader import MongoDBLoader
from src.ontology_loader.utils import load_yaml_from_package

@pytest.fixture
def schema_view():
    """Load the NMDC schema view."""
    return load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")


@pytest.fixture
def ontology_loader():
    """Initialize the OntologyLoader with test parameters."""
    return OntologyLoader(
        db_host=os.getenv("MONGO_HOST", "localhost"),
        db_port=int(os.getenv("MONGO_PORT", 27018)),
        db_name="test_nmdc",
        db_user=os.getenv("MONGO_USER", "admin"),
        db_password=os.getenv("MONGO_PASSWORD", ""),
        source_ontology="envo",
        output_directory="/tmp",
        generate_reports=True
    )


def test_ontology_loader_run(schema_view, ontology_loader):
    """Test running the ontology loader and inserting data into MongoDB."""
    ontology_loader.run_ontology_loader()

    # Connect to MongoDB and verify inserted data
    db_manager = MongoDBLoader(
        schema_view=schema_view,
        db_host=ontology_loader.db_host,
        db_port=ontology_loader.db_port,
        db_name=ontology_loader.db_name,
        db_user=ontology_loader.db_user,
        db_password=ontology_loader.db_password,
    )

    # Check ontology class insertions
    collection = db_manager.db.create_collection("ontology_class_set", recreate_if_exists=False)
    query_results = collection.find({})
    assert query_results.num_rows > 0, "No ontology classes were inserted into MongoDB"

    # Check ontology relation insertions
    relation_collection = db_manager.db.create_collection("ontology_relation_set", recreate_if_exists=False)
    relation_results = relation_collection.find({})
    assert relation_results.num_rows > 0, "No ontology relations were inserted into MongoDB"


def test_ontology_loader_reports(ontology_loader):
    """Test whether reports are generated after running the ontology loader."""
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