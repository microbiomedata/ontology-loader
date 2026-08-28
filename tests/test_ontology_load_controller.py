"""Test the OntologyLoader class."""

import csv
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pymongo import MongoClient

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
def scratch_mongo(request):
    """
    Provide a dedicated scratch MongoDB database for one test, cleaned up unconditionally afterward.

    Same pattern as test_cli_smoke.py's live-Mongo test and this file's own
    test_ontology_loader_reports_multi_ontology_do_not_collide: refuse to run if a database with
    this scratch name already exists (a leftover from a previous failed run), and drop it in a
    teardown that runs regardless of whether the test passed, failed, or errored. Named after the
    requesting test so concurrent/successive scratch databases in this file never collide with
    each other.

    :return: (MongoClient, db_name) for the caller to pass through as
        OntologyLoaderController(mongo_client=..., db_name=...) or MongoDBLoader(mongo_client=..., db_name=...).
    """
    # Self-gate rather than let a bare os.environ["MONGO_PASSWORD"] raise KeyError: any future
    # test using this fixture without the live-DB skipif marker (or a DB-less CI run) should skip
    # gracefully, not error during fixture setup.
    if os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true":
        pytest.skip("Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true")

    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    user = os.environ["MONGO_USERNAME"] if "MONGO_USERNAME" in os.environ else "admin"
    pw = os.environ["MONGO_PASSWORD"]
    # MongoDB database names cap at 63 chars; truncate defensively for any future long test name
    # ("ol_scratch_" is short enough that today's test names all fit without truncation).
    db_name = f"ol_scratch_{request.node.name}"[:63]

    client = MongoClient(
        host=host,
        port=port,
        username=user,
        password=pw,
        authSource="admin",
        directConnection=True,
    )
    try:
        if db_name in client.list_database_names():
            pytest.fail(
                f"scratch database {db_name!r} already exists on the target MongoDB "
                f"({host}:{port}). Refusing to run to avoid overwriting it. "
                f"Investigate, then drop it explicitly to re-enable this test."
            )

        yield client, db_name

        client.drop_database(db_name)
    finally:
        client.close()


@pytest.fixture
def ontology_loader(scratch_mongo):
    """
    Initialize the OntologyLoader with test parameters, against a dedicated scratch database.

    :return: OntologyLoaderController instance.
    """
    client, db_name = scratch_mongo
    return OntologyLoaderController(
        source_ontology="envo",
        report_directory=tempfile.gettempdir(),
        mongo_client=client,
        db_name=db_name,
    )


@pytest.fixture
def mock_mongo_client():
    """
    Create a mock MongoDB client.

    :return: Mock MongoDB client.
    """
    return MagicMock()


@pytest.fixture
def ontology_loader_with_client(mock_mongo_client):
    """
    Initialize the OntologyLoader with a mock MongoDB client.

    :param mock_mongo_client: Mock MongoDB client.
    :return: OntologyLoaderController instance with a mock client.
    """
    return OntologyLoaderController(
        source_ontology="envo",
        report_directory=tempfile.gettempdir(),
        mongo_client=mock_mongo_client,
        db_name="test_db",
    )


def test_ontology_loader_rejects_duplicate_source_ontology():
    """
    A repeated ontology name in source_ontology must raise, not silently collide.

    Each ontology in a multi-ontology run gets a report subdirectory named after it (see
    run_ontology_loader's report_output_directory); ["envo", "envo"] would resolve both
    iterations to the same subdirectory, and the second run's reports would overwrite the
    first's. No MongoDB connection needed: this raises in __init__, before any DB access.
    """
    with pytest.raises(ValueError, match="duplicates"):
        OntologyLoaderController(source_ontology=["envo", "po", "envo"])


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_ontology_loader_run(schema_view, ontology_loader, scratch_mongo):
    """
    Test running the ontology loader and inserting data into MongoDB.

    :param schema_view: NMDC schema view.
    :param ontology_loader: OntologyLoaderController instance, already pointed at scratch_mongo.
    :param scratch_mongo: (client, db_name) fixture is function-scoped, so requesting it here
        yields the same instance ontology_loader was built with, for verifying its writes.
    """
    ontology_loader.run_ontology_loader()

    # Connect to MongoDB and verify inserted data, in the same scratch database ontology_loader
    # just wrote to (not whatever database MongoDBConfig() would otherwise default to).
    client, db_name = scratch_mongo
    db_manager = MongoDBLoader(
        schema_view=schema_view,
        mongo_client=client,
        db_name=db_name,
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
    relation_insertions_report = Path(ontology_loader.output_directory) / "ontology_relation_inserts.tsv"

    assert updates_report.exists(), "Updates report was not generated"
    assert insertions_report.exists(), "Insertions report was not generated"
    assert relation_insertions_report.exists(), "Relation insertions report was not generated"

    # Parse with csv.reader, not readlines()+split("\t"): a class's `relations` field can embed a
    # literal newline inside a quoted TSV field, which readlines() would wrongly split into
    # multiple "rows".
    def _read_tsv_rows(path):
        with path.open(newline="") as f:
            return list(csv.reader(f, delimiter="\t"))

    updates_rows = _read_tsv_rows(updates_report)
    insert_rows = _read_tsv_rows(insertions_report)
    relation_rows = _read_tsv_rows(relation_insertions_report)

    # Whether a class lands in updates.tsv or inserts.tsv depends on whether this Mongo already
    # has it from a prior run, so check the combined total rather than assuming either file
    # specifically holds data rows (a header-only file in one is expected; a header-only file in
    # *both* is not, and would mean run_ontology_loader() processed nothing).
    assert len(updates_rows) + len(insert_rows) > 2, "Neither updates nor inserts report has any data rows"

    # The class-inserts file must hold only class rows, not have been overwritten by the
    # relation-inserts write (the historical bug: both used to share one filename).
    insertions_header = insert_rows[0]
    assert insertions_header[0] == "id"
    assert "subject" not in insertions_header, "class inserts file was overwritten by the relation report"

    # And the relation-inserts file, now that it has its own filename, must actually hold
    # relation rows (subject/predicate/object headers) rather than just its header, or class rows.
    assert "subject" in relation_rows[0], "relation inserts file does not have relation headers"
    assert len(relation_rows) > 1, "Relation inserts report has no data rows"

    # Every data row must have the same column count as its header: catches the report writer
    # silently emitting a mismatched header (e.g. write_reports prepending an "id" column a
    # report's records don't actually have, as it used to for the relation report).
    for label, rows in (
        ("updates", updates_rows),
        ("inserts", insert_rows),
        ("relation inserts", relation_rows),
    ):
        header_columns = len(rows[0])
        for row in rows[1:]:
            assert len(row) == header_columns, (
                f"{label} report: data row column count doesn't match its header ({header_columns} columns): {row!r}"
            )


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_ontology_loader_reports_multi_ontology_do_not_collide():
    """
    Loading more than one ontology in one invocation must not overwrite earlier reports.

    Historical bug: report_directory was shared flat across the whole run, and each ontology's
    write used fixed filenames, so only the last ontology's reports survived.
    """
    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    user = os.environ["MONGO_USERNAME"] if "MONGO_USERNAME" in os.environ else "admin"
    pw = os.environ["MONGO_PASSWORD"]
    db_name = "ontology_loader_multi_ontology_test"

    client = MongoClient(
        host=host,
        port=port,
        username=user,
        password=pw,
        authSource="admin",
        directConnection=True,
    )

    # Same scratch-database safety pattern as test_cli_smoke.py's live-Mongo test: refuse to run
    # against a leftover database from a previous failed run rather than silently reusing it.
    if db_name in client.list_database_names():
        pytest.fail(
            f"scratch database {db_name!r} already exists on the target MongoDB "
            f"({host}:{port}). Refusing to run to avoid overwriting it. "
            f"Investigate, then drop it explicitly to re-enable this test."
        )

    try:
        with tempfile.TemporaryDirectory() as tmp:
            loader = OntologyLoaderController(
                source_ontology=["envo", "po"],
                report_directory=tmp,
                mongo_client=client,
                db_name=db_name,
            )
            loader.run_ontology_loader()

            # Whether a class lands in updates.tsv or inserts.tsv depends on whether this Mongo
            # already has it from a prior run (fresh CI Mongo: all inserts; a reused dev Mongo:
            # all updates), so check the combined total rather than assuming either file
            # specifically is populated. envo (~4,366 classes) and po (~1,998 classes) differ
            # enough that identical combined totals would mean one ontology's files got the
            # other's content.
            combined_row_counts = {}
            for ontology in ("envo", "po"):
                updates_report = Path(tmp) / ontology / "ontology_updates.tsv"
                insertions_report = Path(tmp) / ontology / "ontology_inserts.tsv"
                assert updates_report.exists(), f"{ontology}'s updates report is missing"
                assert insertions_report.exists(), f"{ontology}'s inserts report is missing"

                # csv.reader, not readlines()+split("\t"): a class's `relations` field can embed a
                # literal newline inside a quoted TSV field, which readlines() would wrongly split
                # into multiple "rows".
                with updates_report.open(newline="") as f:
                    updates_rows = list(csv.reader(f, delimiter="\t"))
                with insertions_report.open(newline="") as f:
                    insert_rows = list(csv.reader(f, delimiter="\t"))
                combined_row_counts[ontology] = len(updates_rows) + len(insert_rows)
                assert combined_row_counts[ontology] > 2, (
                    f"{ontology}'s combined report row count ({combined_row_counts[ontology]}) is "
                    "header-only in both files; run_ontology_loader() may not have processed anything"
                )

                # Every data row must have the same column count as its header: catches a
                # mismatched report header/record shape, not just a missing file.
                for label, rows in (("updates", updates_rows), ("inserts", insert_rows)):
                    header_columns = len(rows[0])
                    for row in rows[1:]:
                        assert len(row) == header_columns, (
                            f"{ontology} {label} report: data row column count doesn't match its "
                            f"header ({header_columns} columns): {row!r}"
                        )

            assert combined_row_counts["envo"] != combined_row_counts["po"], (
                f"envo and po report row counts are identical ({combined_row_counts}), "
                "suggesting one overwrote the other rather than each landing in its own file"
            )
    finally:
        # Clean up unconditionally so reruns are deterministic and the dev's MongoDB doesn't
        # accumulate test leftovers across runs.
        client.drop_database(db_name)


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_obsolete_handling_in_ontology_loader(scratch_mongo):
    """
    Test the handling of obsolete terms when processing ontology data.

    Runs against a dedicated scratch database (scratch_mongo) instead of whatever database
    MongoDBConfig() would otherwise default to, so this never writes TEST:* documents into a
    developer's real ontology_class_set/ontology_relation_set collections. Cleanup is
    unconditional (the whole database is dropped by the fixture), not a delete() call at the end
    of the test body that a failed assertion above it would skip.
    """
    client, db_name = scratch_mongo
    schema_view = load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")
    db_manager = MongoDBLoader(schema_view=schema_view, mongo_client=client, db_name=db_name)

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


@pytest.mark.skip(reason="Test needs more complete mocking of MongoDB interaction")
def test_ontology_loader_with_client(schema_view, ontology_loader_with_client, mock_mongo_client):
    """
    Test running the ontology loader with a provided MongoDB client.

    Note: This test is skipped until we can properly mock all MongoDB interactions.

    :param schema_view: NMDC schema view.
    :param ontology_loader_with_client: OntologyLoaderController instance with a mock client.
    :param mock_mongo_client: Mock MongoDB client.
    """
    # This test verifies that the mongo_client is correctly passed from
    # OntologyLoaderController to MongoDBLoader.

    # We've already verified in test_init_with_existing_client that
    # MongoDBLoader properly uses an existing client when provided.

    # Just verify that the client reference and db_name are maintained
    assert ontology_loader_with_client.mongo_client == mock_mongo_client
    assert ontology_loader_with_client.db_name == "test_db"


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
