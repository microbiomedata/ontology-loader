"""Tests for the MongoDBLoader class with mocked database interactions."""

from unittest.mock import MagicMock

import pytest
from nmdc_schema.nmdc import OntologyClass, OntologyRelation

from ontology_loader.mongodb_loader import (
    MongoDBLoader,
    Report,
    _handle_obsolete_terms,
)
from ontology_loader.utils import load_yaml_from_package


@pytest.fixture()
def schema_view():
    """Load the NMDC schema view."""
    return load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")


@pytest.fixture()
def mock_mongo_client():
    """Create a mock MongoDB client."""
    return MagicMock()


@pytest.fixture
def mock_db():
    """Mock linkml_store database used for obsolete-term handling."""
    db = MagicMock()
    db.create_collection.return_value = MagicMock()
    return db


@pytest.fixture
def mock_py_db_factory():
    """
    Return a factory that builds a MagicMock acting as a pymongo Database.

    Default behavior:
        - `<db>[name].find({...})` returns iter([]) (no existing docs)
        - `<db>[name].bulk_write(ops, ...)` returns a BulkWriteResult-like
          mock whose `upserted_ids` covers every op index (all inserts).
    Tests can override per-collection behavior via the returned helpers.
    """

    def make(find_results=None, upserted_all=True):
        """
        Build a mock pymongo Database with configurable find/bulk_write responses.

        :param find_results: mapping of collection_name -> list of existing docs
            returned by ``find``. Defaults to empty for any collection.
        :param upserted_all: whether bulk_write returns all-inserted by default.
        """
        find_results = find_results or {}
        collection_cache: dict = {}

        def _collection_factory(name):
            if name in collection_cache:
                return collection_cache[name]
            collection = MagicMock()

            def _find(query):
                return iter(find_results.get(name, []))

            collection.find.side_effect = _find

            def _bulk_write(ops, ordered=False):
                result = MagicMock()
                if upserted_all:
                    result.upserted_ids = {i: f"oid-{i}" for i in range(len(ops))}
                else:
                    result.upserted_ids = {}
                return result

            collection.bulk_write.side_effect = _bulk_write
            collection_cache[name] = collection
            return collection

        py_db = MagicMock()
        py_db.__getitem__.side_effect = _collection_factory
        return py_db

    return make


@pytest.fixture
def mock_ontology_classes():
    """Mock ontology classes."""
    return [
        OntologyClass(id="ONT:001", name="Term1", type="nmdc:OntologyClass"),
        OntologyClass(id="ONT:002", name="Term2", type="nmdc:OntologyClass"),
    ]


@pytest.fixture
def mock_ontology_relations():
    """Mock ontology relations."""
    return [
        OntologyRelation(subject="ONT:001", predicate="related_to", object="ONT:002", type="nmdc:OntologyRelation"),
        OntologyRelation(subject="ONT:002", predicate="part_of", object="ONT:003", type="nmdc:OntologyRelation"),
    ]


@pytest.fixture
def mock_obsolete_classes():
    """Mock ontology classes with obsolete terms."""
    return [
        OntologyClass(id="ONT:001", name="Term1", type="nmdc:OntologyClass"),
        OntologyClass(id="ONT:002", name="Term2", type="nmdc:OntologyClass", is_obsolete=True),
        OntologyClass(id="ONT:003", name="Term3", type="nmdc:OntologyClass", is_obsolete=True),
    ]


def test_init_with_existing_client(mock_mongo_client):
    """Initializing MongoDBLoader with an existing MongoDB client stores it on the config."""
    loader = MongoDBLoader(mongo_client=mock_mongo_client, db_name="test_db")

    assert loader.db_config.has_existing_client()
    assert loader.db_config.existing_client == mock_mongo_client
    assert loader.db_config.db_name == "test_db"
    assert loader.db._native_client == mock_mongo_client


def test_upsert_new_ontology_data(mock_db, mock_py_db_factory, mock_ontology_classes, mock_ontology_relations):
    """All-new classes and relations land in the inserts reports under 'compared' mode."""
    loader = MongoDBLoader()
    loader.db = mock_db
    loader._py_db = mock_py_db_factory()  # default: no existing docs, all inserts

    report = loader.upsert_ontology_data(mock_ontology_classes, mock_ontology_relations)

    assert isinstance(report[0], Report)
    assert isinstance(report[1], Report)
    assert isinstance(report[2], Report)
    assert len(report[0].records) == 0  # no updates
    assert len(report[1].records) == len(mock_ontology_classes)  # all classes inserted
    assert len(report[2].records) == len(mock_ontology_relations)  # all relations inserted


def test_upsert_existing_ontology_data(mock_db, mock_py_db_factory, mock_ontology_classes):
    """When every input class has a matching existing doc with different fields, they all go to updates."""
    existing_docs = [
        {"id": "ONT:001", "name": "OldTerm1", "type": "nmdc:OntologyClass"},
        {"id": "ONT:002", "name": "OldTerm2", "type": "nmdc:OntologyClass"},
    ]
    loader = MongoDBLoader()
    loader.db = mock_db
    loader._py_db = mock_py_db_factory(find_results={"ontology_class_set": existing_docs})

    report = loader.upsert_ontology_data(mock_ontology_classes, [])

    assert len(report[0].records) == 2  # both classes updated (name differs)
    assert len(report[1].records) == 0  # no inserts


def test_upsert_with_report_mode_off(mock_db, mock_py_db_factory, mock_ontology_classes, mock_ontology_relations):
    """`report_mode='off'` should produce empty report lists and still call bulk_write."""
    loader = MongoDBLoader()
    loader.db = mock_db
    loader._py_db = mock_py_db_factory()

    report = loader.upsert_ontology_data(mock_ontology_classes, mock_ontology_relations, report_mode="off")

    assert len(report[0].records) == 0
    assert len(report[1].records) == 0
    assert len(report[2].records) == 0
    # bulk_write was invoked for both collections
    assert loader._py_db["ontology_class_set"].bulk_write.called
    assert loader._py_db["ontology_relation_set"].bulk_write.called


def test_upsert_with_report_mode_upsert(mock_db, mock_py_db_factory, mock_ontology_classes):
    """`report_mode='upsert'` skips the pre-read and classifies via BulkWriteResult."""
    loader = MongoDBLoader()
    loader.db = mock_db
    loader._py_db = mock_py_db_factory()  # all inserts

    report = loader.upsert_ontology_data(mock_ontology_classes, [], report_mode="upsert")

    # No pre-read should have happened
    assert not loader._py_db["ontology_class_set"].find.called
    assert len(report[1].records) == len(mock_ontology_classes)


def test_upsert_rejects_unknown_report_mode(mock_db, mock_py_db_factory, mock_ontology_classes):
    """An unrecognized report_mode raises ValueError before any DB call."""
    loader = MongoDBLoader()
    loader.db = mock_db
    loader._py_db = mock_py_db_factory()

    with pytest.raises(ValueError, match="report_mode"):
        loader.upsert_ontology_data(mock_ontology_classes, [], report_mode="bogus")


def test_handle_obsolete_terms_function(mock_db):
    """`_handle_obsolete_terms` marks terms obsolete and clears their relations."""
    class_collection = mock_db.create_collection.return_value
    relation_collection = mock_db.create_collection.return_value

    term_obj = OntologyClass(id="ONT:001", name="Term1", type="nmdc:OntologyClass", is_obsolete=False)
    term_obj.relations = ["some_relation"]

    mock_query_result = MagicMock()
    mock_query_result.rows = [term_obj]
    mock_query_result.num_rows = 1
    class_collection.find.return_value = mock_query_result

    upserted_data = None

    def capture_upsert(data, filter_fields, update_fields=None):
        nonlocal upserted_data
        upserted_data = data[0] if data else None

    class_collection.upsert.side_effect = capture_upsert

    obsolete_terms = ["ONT:001", "ONT:002"]
    _handle_obsolete_terms(obsolete_terms, class_collection, relation_collection)

    assert upserted_data is not None, "No data was passed to upsert"
    assert upserted_data["is_obsolete"] is True
    assert upserted_data["relations"] == []
    class_collection.upsert.assert_called()
    relation_collection.delete.assert_called_with(
        {"$or": [{"subject": {"$in": obsolete_terms}}, {"object": {"$in": obsolete_terms}}]}
    )


def test_upsert_ontology_data_with_obsolete_terms(
    mock_db, mock_py_db_factory, mock_obsolete_classes, mock_ontology_relations
):
    """`upsert_ontology_data` invokes obsolete handling on linkml_store and bulk writes on pymongo."""
    loader = MongoDBLoader()
    loader.db = mock_db
    loader._py_db = mock_py_db_factory()

    class_collection = mock_db.create_collection.return_value
    relation_collection = mock_db.create_collection.return_value

    # Obsolete handling reads each obsolete term via linkml_store find; return empty for both.
    def mock_find(criteria):
        result = MagicMock()
        result.rows = []
        result.num_rows = 0
        return result

    class_collection.find.side_effect = mock_find
    relation_collection.delete = MagicMock()

    loader.upsert_ontology_data(mock_obsolete_classes, mock_ontology_relations)

    obsolete_terms = ["ONT:002", "ONT:003"]
    relation_collection.delete.assert_any_call(
        {"$or": [{"subject": {"$in": obsolete_terms}}, {"object": {"$in": obsolete_terms}}]}
    )
    for term_id in obsolete_terms:
        class_collection.find.assert_any_call({"id": term_id})

    # Class bulk-write went through pymongo with all three input classes.
    py_class = loader._py_db["ontology_class_set"]
    assert py_class.bulk_write.called
