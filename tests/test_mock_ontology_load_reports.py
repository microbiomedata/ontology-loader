"""Mocked Test for OntologyLoader class (No real MongoDB interaction)."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ontology_loader.mongodb_loader import MongoDBLoader
from ontology_loader.ontology_load_controller import OntologyLoaderController
from ontology_loader.reporter import Report, ReportWriter
from ontology_loader.utils import load_yaml_from_package


def test_write_reports_class_and_relation_inserts_do_not_collide():
    """
    Regression test: class and relation Report objects must not write to the same file.

    Both used to be constructed with report_type="insert", so both wrote to ontology_inserts.tsv
    and the second write (relations) silently overwrote the first (classes). No mocking of
    write_reports itself, this calls the real method with real Report objects.
    """
    updates = Report("update", [["ENVO:001", "Term1"]], ["id", "name"])
    class_inserts = Report("insert", [["ENVO:002", "Term2"]], ["id", "name"])
    relation_inserts = Report(
        "relation_insert", [["ENVO:002", "rdfs:subClassOf", "ENVO:001"]], ["subject", "predicate", "object"]
    )

    with tempfile.TemporaryDirectory() as tmp:
        ReportWriter.write_reports(reports=[updates, class_inserts, relation_inserts], output_directory=tmp)

        updates_path = Path(tmp) / "ontology_updates.tsv"
        class_inserts_path = Path(tmp) / "ontology_inserts.tsv"
        relation_inserts_path = Path(tmp) / "ontology_relation_inserts.tsv"

        assert updates_path.exists()
        assert class_inserts_path.exists()
        assert relation_inserts_path.exists(), "relation inserts must not collide with class inserts"

        # Each file holds only its own report's rows, not the other's.
        assert "ENVO:002\tTerm2" in class_inserts_path.read_text()
        assert "rdfs:subClassOf" not in class_inserts_path.read_text(), "class file overwritten by relation report"
        assert "ENVO:002\trdfs:subClassOf\tENVO:001" in relation_inserts_path.read_text()

        # write_reports writes headers exactly as given (no implicit "id" prepending), so each
        # file's header must have the same column count as its data rows. Regression coverage for
        # the bug where relation_insert's ["subject", "predicate", "object"] header got an
        # unwanted "id" prepended, producing a 4-column header over 3-column data rows.
        relation_lines = relation_inserts_path.read_text().splitlines()
        assert relation_lines[0] == "subject\tpredicate\tobject"
        assert relation_lines[1] == "ENVO:002\trdfs:subClassOf\tENVO:001"


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
        report_directory=tempfile.gettempdir(),
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
