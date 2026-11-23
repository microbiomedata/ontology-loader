"""
Class documents leave out unset optional fields instead of storing them as null.

See https://github.com/microbiomedata/ontology-loader/issues/91: every production
``ontology_class_set`` document had ``description: null`` because class documents were built with
``dataclasses.asdict``, which emits every field.
"""

import os

import pytest
from nmdc_schema.nmdc import OntologyClass
from pymongo import MongoClient

from ontology_loader.mongodb_loader import MongoDBLoader, _class_to_doc


def _make_class(**overrides) -> OntologyClass:
    """Build an OntologyClass shaped like the ones OntologyProcessor creates, with no description."""
    values = {
        "id": "TEST:0001",
        "type": "nmdc:OntologyClass",
        "name": "test term",
        "definition": "",
        "alternative_names": [],
        "relations": [],
        "is_root": False,
        "is_obsolete": False,
    }
    values.update(overrides)
    return OntologyClass(**values)


def test_class_to_doc_omits_unset_description() -> None:
    """An unset description is left out of the document rather than stored as None."""
    doc = _class_to_doc(_make_class())
    assert "description" not in doc
    assert None not in doc.values()


def test_class_to_doc_keeps_a_set_description() -> None:
    """A description that has a value is kept."""
    assert _class_to_doc(_make_class(description="a description"))["description"] == "a description"


@pytest.mark.parametrize("field", ["is_root", "is_obsolete"])
def test_class_to_doc_defaults_booleans_to_false(field: str) -> None:
    """is_root and is_obsolete are False, not missing, when the class leaves them unset."""
    assert _class_to_doc(_make_class(**{field: None}))[field] is False


@pytest.mark.parametrize("field", ["definition", "name"])
def test_class_to_doc_keeps_empty_strings(field: str) -> None:
    """Only None is dropped: empty strings, which the processor writes on purpose, stay."""
    assert _class_to_doc(_make_class(**{field: ""}))[field] == ""


@pytest.fixture
def live_mongo_client():
    """Return a raw pymongo client against the live Mongo the other live-DB tests use."""
    return MongoClient(
        host=os.environ.get("MONGO_HOST", "localhost"),
        port=int(os.environ.get("MONGO_PORT", 27022)),
        username=os.environ.get("MONGO_USERNAME", "admin"),
        password=os.environ["MONGO_PASSWORD"],
    )


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
@pytest.mark.parametrize("mode", ["meticulous", "fast-initial"])
def test_stored_class_documents_have_no_null_description(live_mongo_client: MongoClient, mode: str) -> None:
    """Both load paths store class documents with no description key when description is unset."""
    db_name = f"ontology_loader_null_fields_test_{os.getpid()}"
    if db_name in live_mongo_client.list_database_names():
        pytest.fail(f"Scratch database {db_name!r} already exists; refusing to overwrite it")
    classes = [_make_class(id=f"TEST:{index:04d}") for index in range(3)]
    try:
        loader = MongoDBLoader(mongo_client=live_mongo_client, db_name=db_name)
        if mode == "meticulous":
            loader.upsert_ontology_data(classes, [])
        else:
            loader.insert_ontology_data_fast_initial(classes, [])
        stored = list(live_mongo_client[db_name]["ontology_class_set"].find({}, {"_id": 0}))
        assert len(stored) == 3
        assert all("description" not in document for document in stored)
        assert all(document["is_root"] is False and document["is_obsolete"] is False for document in stored)
    finally:
        live_mongo_client.drop_database(db_name)


def _scratch_db_name(live_mongo_client: MongoClient, label: str) -> str:
    """Return a scratch database name that does not exist yet, failing if it does."""
    db_name = f"ontology_loader_{label}_test_{os.getpid()}"
    if db_name in live_mongo_client.list_database_names():
        pytest.fail(f"Scratch database {db_name!r} already exists; refusing to overwrite it")
    return db_name


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_meticulous_reload_removes_a_cleared_description(live_mongo_client: MongoClient) -> None:
    """A description that had a value and is now unset is removed from the stored document."""
    db_name = _scratch_db_name(live_mongo_client, "cleared_field")
    try:
        loader = MongoDBLoader(mongo_client=live_mongo_client, db_name=db_name)
        loader.upsert_ontology_data([_make_class(description="old description")], [])
        updates, _, _ = loader.upsert_ontology_data([_make_class()], [])
        stored = live_mongo_client[db_name]["ontology_class_set"].find_one({"id": "TEST:0001"})
        assert "description" not in stored
        assert len(updates.records) == 1
    finally:
        live_mongo_client.drop_database(db_name)


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_meticulous_reload_removes_a_stored_null_description(live_mongo_client: MongoClient) -> None:
    """A document stored with description: null, as older releases wrote it, loses that key on reload."""
    db_name = _scratch_db_name(live_mongo_client, "stored_null")
    try:
        loader = MongoDBLoader(mongo_client=live_mongo_client, db_name=db_name)
        loader.upsert_ontology_data([_make_class()], [])
        collection = live_mongo_client[db_name]["ontology_class_set"]
        collection.update_one({"id": "TEST:0001"}, {"$set": {"description": None}})
        loader.upsert_ontology_data([_make_class()], [])
        assert "description" not in collection.find_one({"id": "TEST:0001"})
    finally:
        live_mongo_client.drop_database(db_name)


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)
def test_meticulous_reload_of_unchanged_class_with_unset_booleans_reports_no_update(
    live_mongo_client: MongoClient,
) -> None:
    """Reloading an unchanged class whose booleans are unset is not reported as an update."""
    db_name = _scratch_db_name(live_mongo_client, "unset_booleans")
    unchanged = _make_class(is_root=None, is_obsolete=None)
    try:
        loader = MongoDBLoader(mongo_client=live_mongo_client, db_name=db_name)
        loader.upsert_ontology_data([unchanged], [])
        updates, insertions, _ = loader.upsert_ontology_data([unchanged], [])
        assert updates.records == []
        assert insertions.records == []
    finally:
        live_mongo_client.drop_database(db_name)
