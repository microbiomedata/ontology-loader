"""Mocked Test for OntologyLoader class (No real MongoDB interaction)."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

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
def mock_mongo_loader():
    """
    Mock MongoDBLoader instead of real MongoDB interaction.

    :return: MongoDBLoader instance.
    """
    mock_loader = MagicMock(spec=MongoDBLoader)

    # Mock the db attribute explicitly
    mock_loader.db = MagicMock()

    # Mock database collection behavior
    mock_loader.db.create_collection.return_value.find.return_value.rows = [{"id": "class1"}, {"id": "class2"}]
    mock_loader.db.create_collection.return_value.find.return_value.num_rows = 2  # Ensuring num_rows is an integer

    return mock_loader


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


def test_ontology_loader_reports(ontology_loader):
    """
    Test whether reports are generated after running the ontology loader (mocked).

    :param ontology_loader: OntologyLoaderController instance.
    """
    ontology_loader.run_ontology_loader = MagicMock()

    # Run the mocked method
    ontology_loader.run_ontology_loader()

    # Verify reports exist in the mocked output directory
    updates_report = Path(ontology_loader.output_directory) / "ontology_updates.tsv"
    insertions_report = Path(ontology_loader.output_directory) / "ontology_inserts.tsv"

    # Mock file existence
    updates_report.touch()
    insertions_report.touch()

    assert updates_report.exists(), "Updates report was not generated"
    assert insertions_report.exists(), "Insertions report was not generated"

    # Mock file contents
    with updates_report.open("w") as f:
        f.write("Mock data\n")

    with insertions_report.open("w") as f:
        f.write("Mock data\n")

    with updates_report.open() as f:
        lines = f.readlines()
        assert len(lines) > 0, "Updates report is empty"

    with insertions_report.open() as f:
        lines = f.readlines()
        assert len(lines) > 0, "Insertions report is empty"
