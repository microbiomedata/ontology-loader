"""Test the OntologyLoader class."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from nmdc_schema.nmdc import OntologyClass, OntologyRelation

from ontology_loader.mongodb_loader import MongoDBLoader
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


@pytest.mark.skipif(os.getenv("MONGO_PASSWORD") is None, reason="Skipping test: MONGO_PASSWORD is not set")
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


@pytest.mark.skipif(os.getenv("MONGO_PASSWORD") is None, reason="Skipping test: MONGO_PASSWORD is not set")
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


@pytest.mark.skipif(os.getenv("MONGO_PASSWORD") is None, reason="Skipping test: MONGO_PASSWORD is not set")
def test_obsolete_handling_in_ontology_loader():
    """Test the handling of obsolete terms when processing ontology data."""
    # Use a custom temp directory for this test
    output_dir = tempfile.mkdtemp()

    # Create a loader with test parameters
    loader = OntologyLoaderController(
        source_ontology="envo",  # Using envo as it's likely to have obsolete terms
        output_directory=output_dir,
        generate_reports=True,
    )

    # Run the ontology loader
    loader.run_ontology_loader()

    # Connect to MongoDB
    schema_view = load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")
    db_manager = MongoDBLoader(schema_view=schema_view)

    # Check for obsolete terms in the database
    class_collection = db_manager.db.create_collection("ontology_class_set", recreate_if_exists=False)
    obsolete_query_results = class_collection.find({"is_obsolete": True})

    # Assert that we have at least some obsolete terms
    assert obsolete_query_results.num_rows > 0, "No obsolete terms found in the database"

    # Verify that obsolete terms don't have relations referencing them
    relation_collection = db_manager.db.create_collection("ontology_relation_set", recreate_if_exists=False)

    # Get IDs of obsolete terms
    obsolete_term_ids = [row["id"] for row in obsolete_query_results.rows]

    # Check relations referencing obsolete terms
    for term_id in obsolete_term_ids:
        # Should be no relations with obsolete term as subject
        subject_relations = relation_collection.find({"subject": term_id})
        assert subject_relations.num_rows == 0, f"Found relations with obsolete term {term_id} as subject"

        # Should be no relations with obsolete term as object
        object_relations = relation_collection.find({"object": term_id})
        assert object_relations.num_rows == 0, f"Found relations with obsolete term {term_id} as object"


@patch("ontology_loader.ontology_processor.OntologyProcessor")
@patch("ontology_loader.mongodb_loader.MongoDBLoader")
@pytest.mark.skipif(os.getenv("MONGO_PASSWORD") is None, reason="Skipping test: MONGO_PASSWORD is not set")
def test_obsolete_terms_end_to_end(mock_mongodb_loader, mock_ontology_processor):
    """
    Test the end-to-end flow of obsolete term handling from processor to database.

    This test mocks the ontology processor to return some obsolete terms and verifies
    that the controller correctly passes them to the MongoDB loader.
    """
    # Create mock ontology classes with some obsolete terms
    mock_classes = [
        OntologyClass(id="ENVO:0000001", name="Term1", type="nmdc:OntologyClass"),
        OntologyClass(id="ENVO:0000002", name="Term2", type="nmdc:OntologyClass", is_obsolete=True),
        OntologyClass(id="ENVO:0000003", name="Term3", type="nmdc:OntologyClass"),
    ]

    # Create mock relations
    mock_relations = [
        OntologyRelation(
            subject="ENVO:0000001", predicate="related_to", object="ENVO:0000003", type="nmdc:OntologyRelation"
        ),
        # Deliberately including a relation to/from an obsolete term - should be filtered out
        OntologyRelation(
            subject="ENVO:0000002", predicate="related_to", object="ENVO:0000001", type="nmdc:OntologyRelation"
        ),
        OntologyRelation(
            subject="ENVO:0000003", predicate="related_to", object="ENVO:0000002", type="nmdc:OntologyRelation"
        ),
    ]

    # Configure mock ontology processor
    processor_instance = mock_ontology_processor.return_value
    processor_instance.get_terms_and_metadata.return_value = mock_classes
    processor_instance.get_relations_closure.return_value = (mock_relations, mock_classes)

    # Configure mock MongoDB loader
    loader_instance = mock_mongodb_loader.return_value
    loader_instance.upsert_ontology_data.return_value = (MagicMock(), MagicMock(), MagicMock())

    # Run the controller
    controller = OntologyLoaderController(source_ontology="envo", output_directory=tempfile.gettempdir())
    controller.run_ontology_loader()

    # Verify interactions
    processor_instance.get_terms_and_metadata.assert_called_once()
    processor_instance.get_relations_closure.assert_called_once_with(ontology_terms=mock_classes)

    # Verify that the loader received the ontology data with obsolete terms
    loader_instance.upsert_ontology_data.assert_called_once()

    # Extract the arguments passed to upsert_ontology_data
    call_args = loader_instance.upsert_ontology_data.call_args[0]
    passed_classes = call_args[0]

    # Verify obsolete term was correctly passed
    assert any(cls.id == "ENVO:0000002" and cls.is_obsolete for cls in passed_classes)

    # The upsert_ontology_data function will handle the obsolete terms, so we don't need
    # to check that the relations to/from obsolete terms were removed here
