"""Tests for the MongoDBLoader class with mocked database interactions."""

from unittest.mock import MagicMock

import pytest
from nmdc_schema.nmdc import OntologyClass, OntologyRelation
from pymongo.errors import BulkWriteError, OperationFailure

from ontology_loader.mongodb_loader import (
    MongoDBLoader,
    Report,
    _bulk_insert_iter,
    _handle_obsolete_terms,
    _insert_batch_skipping_duplicates,
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


@pytest.fixture()
def mock_mongo_loader(schema_view):
    """Mock MongoDBLoader to prevent actual database interactions."""
    loader = MongoDBLoader(schema_view)
    loader.client = MagicMock()
    loader.db = MagicMock()
    loader.db.get_collection = MagicMock()
    return loader


@pytest.fixture
def mock_db():
    """
    Mock database.

    :return: Mock database.
    """
    db = MagicMock()
    db.create_collection.return_value = MagicMock()
    return db


@pytest.fixture
def mock_ontology_classes():
    """
    Mock ontology classes.

    :return: List of OntologyClass objects.
    """
    return [
        OntologyClass(id="ONT:001", name="Term1", type="nmdc:OntologyClass"),
        OntologyClass(id="ONT:002", name="Term2", type="nmdc:OntologyClass"),
    ]


@pytest.fixture
def mock_ontology_relations():
    """
    Mock ontology relations.

    :return: List of OntologyRelation objects.
    """
    return [
        OntologyRelation(subject="ONT:001", predicate="related_to", object="ONT:002", type="nmdc:OntologyRelation"),
        OntologyRelation(subject="ONT:002", predicate="part_of", object="ONT:003", type="nmdc:OntologyRelation"),
    ]


@pytest.fixture
def mock_obsolete_classes():
    """
    Mock ontology classes with obsolete terms.

    :return: List of OntologyClass objects.
    """
    return [
        OntologyClass(id="ONT:001", name="Term1", type="nmdc:OntologyClass"),
        OntologyClass(id="ONT:002", name="Term2", type="nmdc:OntologyClass", is_obsolete=True),
        OntologyClass(id="ONT:003", name="Term3", type="nmdc:OntologyClass", is_obsolete=True),
    ]


def test_init_with_existing_client(mock_mongo_client):
    """
    Test initializing MongoDBLoader with an existing MongoDB client.

    :param mock_mongo_client: Mock MongoDB client.
    """
    # Create a MongoDBLoader with an existing client and db_name
    loader = MongoDBLoader(mongo_client=mock_mongo_client, db_name="test_db")

    # Verify the client was stored in the config
    assert loader.db_config.has_existing_client()
    assert loader.db_config.existing_client == mock_mongo_client
    assert loader.db_config.db_name == "test_db"

    # Check that we're using the provided client in the MongoDB database
    assert loader.db._native_client == mock_mongo_client


def test_upsert_new_ontology_data(mock_db, mock_ontology_classes, mock_ontology_relations):
    """
    Test upserting new ontology data.

    :param mock_db: Mock database.
    :param mock_ontology_classes: Mock ontology classes.
    :param mock_ontology_relations: Mock ontology relations.
    """
    loader = MongoDBLoader()
    loader.db = mock_db
    class_collection = mock_db.create_collection.return_value

    mock_query_result = MagicMock()
    mock_query_result.rows = []
    mock_query_result.num_rows = 0  # Ensuring num_rows behaves like an integer

    class_collection.find.return_value = mock_query_result  # Mocking find() response

    report = loader.upsert_ontology_data(mock_ontology_classes, mock_ontology_relations)

    assert isinstance(report[0], Report)  # Class updates report
    assert isinstance(report[1], Report)  # Class insertions report
    assert isinstance(report[2], Report)  # Relation insertions report
    assert len(report[1].records) == len(mock_ontology_classes)  # All classes inserted
    assert len(report[2].records) == len(mock_ontology_relations)  # All relations inserted


def test_upsert_existing_ontology_data(mock_db, mock_ontology_classes):
    """
    Test upserting existing ontology data.

    :param mock_db: Mock database.
    :param mock_ontology_classes: Mock ontology classes.
    """
    loader = MongoDBLoader()
    loader.db = mock_db
    class_collection = mock_db.create_collection.return_value

    existing_doc = {"id": "ONT:001", "name": "OldTerm", "type": "nmdc:OntologyClass"}

    mock_query_result = MagicMock()
    mock_query_result.rows = [existing_doc]
    mock_query_result.num_rows = 1  # Ensuring num_rows behaves like an integer

    class_collection.find.return_value = mock_query_result  # Mocking find() response

    report = loader.upsert_ontology_data(mock_ontology_classes, [])
    assert len(report[0].records) == 2  # One record should be updated
    assert len(report[1].records) == 0  # One record should be inserted (new class)


def test_handle_disappearing_relations(mock_db, mock_ontology_classes, mock_ontology_relations):
    """
    Test handling of disappearing relations.

    :param mock_db: Mock database.
    :param mock_ontology_classes: Mock ontology classes.
    :param mock_ontology_relations: Mock ontology relations.
    """
    loader = MongoDBLoader()
    loader.db = mock_db
    relation_collection = mock_db.create_collection.return_value

    mock_query_result = MagicMock()
    mock_query_result.rows = [
        {
            "subject": "ONT:001",
            "predicate": "entailed_isa_partof_closure",
            "object": "ONT:002",
            "type": "nmdc:OntologyRelation",
        },
        {
            "subject": "ONT:002",
            "predicate": "entailed_isa_partof_closure",
            "object": "ONT:003",
            "type": "nmdc:OntologyRelation",
        },
    ]
    mock_query_result.num_rows = 2  # Ensuring num_rows behaves like an integer

    relation_collection.find.return_value = mock_query_result  # Mocking find() response
    relation_collection.upsert = MagicMock()  # Mock upsert method

    updated_relations = [
        OntologyRelation(subject="ONT:001", predicate="related_to", object="ONT:003", type="nmdc:OntologyRelation")
        # Changed relation
    ]

    def mock_upsert(data, filter_fields, update_fields=None):
        """
        Simulate relation updates.

        :param data: Data to upsert.
        :param filter_fields: Fields to filter on.
        :param update_fields: Fields to update.
        """
        for obj in data:
            if "subject" in obj:
                for ontology_class in mock_ontology_classes:
                    if ontology_class.id == obj["subject"]:
                        ontology_class.relations = [
                            OntologyRelation(
                                subject=obj["subject"],
                                predicate=obj["predicate"],
                                object=obj["object"],
                                type="nmdc:OntologyRelation",
                            )
                        ]

    relation_collection.upsert.side_effect = mock_upsert  # Simulate relation updates

    loader.upsert_ontology_data(mock_ontology_classes, updated_relations)

    # Ensure upsert was called
    relation_collection.upsert.assert_called()

    # Verify old relations were replaced
    assert len(mock_ontology_classes[0].relations) == 1
    assert mock_ontology_classes[0].relations[0].object == "ONT:003"


def test_handle_obsolete_terms_function(mock_db):
    """
    Test the _handle_obsolete_terms function directly.

    :param mock_db: Mock database.
    """
    class_collection = mock_db.create_collection.return_value
    relation_collection = mock_db.create_collection.return_value

    # Create a proper OntologyClass object
    term_obj = OntologyClass(id="ONT:001", name="Term1", type="nmdc:OntologyClass", is_obsolete=False)
    term_obj.relations = ["some_relation"]  # Add relations attribute

    # Set up the find method to return the term_obj
    mock_query_result = MagicMock()
    mock_query_result.rows = [term_obj]
    mock_query_result.num_rows = 1
    class_collection.find.return_value = mock_query_result

    # Capture what gets passed to upsert to verify changes
    upserted_data = None

    def capture_upsert(data, filter_fields, update_fields=None):
        nonlocal upserted_data
        upserted_data = data[0] if data else None

    class_collection.upsert.side_effect = capture_upsert

    # Test with list of obsolete terms
    obsolete_terms = ["ONT:001", "ONT:002"]
    _handle_obsolete_terms(obsolete_terms, class_collection, relation_collection)

    # Function creates a new dict from the original object, so the original stays unchanged
    # But we can check what was passed to upsert
    assert upserted_data is not None, "No data was passed to upsert"
    assert upserted_data["is_obsolete"] is True, "Term was not marked as obsolete"
    assert upserted_data["relations"] == [], "Relations were not cleared"

    # Verify class collection upsert was called
    class_collection.upsert.assert_called()

    # Verify relation collection had delete called
    relation_collection.delete.assert_called_with(
        {"$or": [{"subject": {"$in": obsolete_terms}}, {"object": {"$in": obsolete_terms}}]}
    )


def test_upsert_ontology_data_with_obsolete_terms(mock_db, mock_obsolete_classes, mock_ontology_relations):
    """
    Test upserting ontology data with obsolete terms.

    :param mock_db: Mock database.
    :param mock_obsolete_classes: Mock ontology classes with obsolete terms.
    :param mock_ontology_relations: Mock ontology relations.
    """
    loader = MongoDBLoader()
    loader.db = mock_db
    class_collection = mock_db.create_collection.return_value
    relation_collection = mock_db.create_collection.return_value

    # Since we're going to check multiple calls to find with different params,
    # we need a more sophisticated mock that can return different results
    def mock_find(criteria):
        # Return empty result for any query
        mock_result = MagicMock()
        mock_result.rows = []
        mock_result.num_rows = 0
        return mock_result

    class_collection.find.side_effect = mock_find

    # Setup captured data for upserting
    upserted_data = []

    def capture_upsert(data, filter_fields, update_fields=None):
        nonlocal upserted_data
        if data and isinstance(data, list):
            upserted_data.extend(data)

    class_collection.upsert.side_effect = capture_upsert

    # Configure relation collection delete method
    relation_collection.delete = MagicMock()

    # Run the upsert function
    loader.upsert_ontology_data(mock_obsolete_classes, mock_ontology_relations)

    # Verify obsolete terms were handled
    obsolete_terms = ["ONT:002", "ONT:003"]  # These are marked as obsolete in mock_obsolete_classes
    relation_collection.delete.assert_any_call(
        {"$or": [{"subject": {"$in": obsolete_terms}}, {"object": {"$in": obsolete_terms}}]}
    )

    # Verify class collection find was called for correct term lookups
    # The _handle_obsolete_terms function should look up each obsolete term
    for term_id in obsolete_terms:
        class_collection.find.assert_any_call({"id": term_id})


# --- fast-initial duplicate handling ---------------------------------------------------------


def test_insert_batch_skipping_duplicates_no_error():
    """A batch with no collisions inserts cleanly; no duplicates reported."""
    collection = MagicMock()
    batch = [{"id": "A"}, {"id": "B"}]

    inserted, dupes = _insert_batch_skipping_duplicates(collection, batch, "classes")

    collection.insert_many.assert_called_once_with(batch, ordered=False)
    assert inserted == 2
    assert dupes == 0


def test_insert_batch_skipping_duplicates_all_duplicate_key_errors():
    """
    A batch that is entirely duplicate-key rejections is treated as fully skipped, not re-raised.

    This is the exact rerun-without-clearing scenario: every document in the batch already
    exists, so pymongo reports one writeError per document, all code 11000.
    """
    collection = MagicMock()
    batch = [{"id": "A"}, {"id": "B"}, {"id": "C"}]
    collection.insert_many.side_effect = BulkWriteError(
        {
            "writeErrors": [
                {"index": 0, "code": 11000, "errmsg": "E11000 duplicate key"},
                {"index": 1, "code": 11000, "errmsg": "E11000 duplicate key"},
                {"index": 2, "code": 11000, "errmsg": "E11000 duplicate key"},
            ],
            "nInserted": 0,
        }
    )

    inserted, dupes = _insert_batch_skipping_duplicates(collection, batch, "classes")

    assert inserted == 0
    assert dupes == 3


def test_insert_batch_skipping_duplicates_partial_duplicates():
    """A batch with some new docs and some duplicates counts each correctly."""
    collection = MagicMock()
    batch = [{"id": "A"}, {"id": "B"}, {"id": "C"}]
    collection.insert_many.side_effect = BulkWriteError(
        {
            "writeErrors": [{"index": 1, "code": 11000, "errmsg": "E11000 duplicate key"}],
            "nInserted": 2,
        }
    )

    inserted, dupes = _insert_batch_skipping_duplicates(collection, batch, "classes")

    assert inserted == 2
    assert dupes == 1


def test_insert_batch_skipping_duplicates_reraises_non_duplicate_errors():
    """A write error that is NOT a duplicate key (e.g. a real validation failure) must propagate."""
    collection = MagicMock()
    batch = [{"id": "A"}]
    collection.insert_many.side_effect = BulkWriteError(
        {
            "writeErrors": [{"index": 0, "code": 121, "errmsg": "Document failed validation"}],
            "nInserted": 0,
        }
    )

    with pytest.raises(BulkWriteError):
        _insert_batch_skipping_duplicates(collection, batch, "classes")


def test_bulk_insert_iter_continues_past_a_duplicate_batch():
    """
    Verify the specific bug this fixes.

    A colliding early batch must not prevent later, non-colliding batches from being attempted.
    Two batches of size 2; batch 1 is entirely duplicates, batch 2 is entirely new.
    """
    collection = MagicMock()

    def insert_many_side_effect(batch, ordered):
        # First call (batch 1) collides; second call (batch 2) succeeds.
        if insert_many_side_effect.calls == 0:
            insert_many_side_effect.calls += 1
            raise BulkWriteError(
                {
                    "writeErrors": [
                        {"index": 0, "code": 11000, "errmsg": "dup"},
                        {"index": 1, "code": 11000, "errmsg": "dup"},
                    ],
                    "nInserted": 0,
                }
            )
        insert_many_side_effect.calls += 1

    insert_many_side_effect.calls = 0
    collection.insert_many.side_effect = insert_many_side_effect

    docs = [{"id": "A"}, {"id": "B"}, {"id": "C"}, {"id": "D"}]
    total, total_dupes = _bulk_insert_iter(collection, iter(docs), batch_size=2, label="classes")

    # Batch 2 must have been attempted despite batch 1's collision.
    assert collection.insert_many.call_count == 2
    assert total == 2  # batch 2's 2 new docs
    assert total_dupes == 2  # batch 1's 2 duplicates


def test_insert_batch_skipping_duplicates_reraises_write_concern_errors():
    """
    Verify a write-concern failure propagates instead of being treated as a successful batch.

    writeConcernErrors carry no writeErrors of their own, so a batch with a write-concern failure
    (e.g. replication timeout) and zero duplicate-key writeErrors must not be mistaken for "all
    inserted, zero duplicates."
    """
    collection = MagicMock()
    batch = [{"id": "A"}, {"id": "B"}]
    collection.insert_many.side_effect = BulkWriteError(
        {
            "writeErrors": [],
            "writeConcernErrors": [{"code": 64, "errmsg": "waiting for replication timed out"}],
            "nInserted": 2,
        }
    )

    with pytest.raises(BulkWriteError):
        _insert_batch_skipping_duplicates(collection, batch, "classes")


def test_insert_batch_skipping_duplicates_uses_ninserted_not_derived_count():
    """
    Inserted/duplicate counts must come from nInserted, not len(batch) - len(writeErrors).

    A batch where nInserted disagrees with the naive derived count (more writeErrors reported
    than documents actually failed to insert, e.g. a retried write counted twice) must trust
    nInserted rather than the arithmetic.
    """
    collection = MagicMock()
    batch = [{"id": "A"}, {"id": "B"}, {"id": "C"}]
    collection.insert_many.side_effect = BulkWriteError(
        {
            # 2 writeErrors would naively imply 1 inserted (3 - 2), but nInserted says 2.
            "writeErrors": [
                {"index": 0, "code": 11000, "errmsg": "dup"},
                {"index": 1, "code": 11000, "errmsg": "dup"},
            ],
            "writeConcernErrors": [],
            "nInserted": 2,
        }
    )

    inserted, dupes = _insert_batch_skipping_duplicates(collection, batch, "classes")

    assert inserted == 2  # from nInserted, not (3 - 2) == 1
    assert dupes == 2  # len(writeErrors), unaffected


def test_fast_initial_index_build_failure_raises_actionable_error(
    mock_mongo_client, mock_ontology_classes, mock_ontology_relations
):
    """
    Verify a unique-index build failing on pre-existing duplicate data raises a clear, actionable error.

    A GitHub Copilot suppressed comment on
    https://github.com/microbiomedata/ontology-loader/pull/60 correctly pointed out that the docstring's
    claim ("does not require those collections to already be duplicate-free") was wrong:
    `create_index(..., unique=True)` fails if the collection already contains a duplicate on that key.
    That's now documented and wrapped with an actionable error instead of an opaque `OperationFailure`.
    """
    loader = MongoDBLoader(mongo_client=mock_mongo_client, db_name="test_db")

    class _FakeMongoDB:
        """A real object (not a MagicMock) so `__getitem__` is keyed by collection name, not a shared default."""

        def __init__(self):
            self._collections = {}

        def __getitem__(self, name):
            return self._collections.setdefault(name, MagicMock())

    fake_db = _FakeMongoDB()
    loader._py_client = MagicMock()
    loader._py_client.__getitem__.return_value = fake_db  # `_py_client[db_name]` -> the same fake db every time

    class_collection = fake_db["ontology_class_set"]
    class_collection.create_index.side_effect = OperationFailure(
        "E11000 duplicate key error collection: nmdc.ontology_class_set index: "
        'ontology_class_fast_initial_unique_id_index dup key: { id: "NCBITaxon:1" }'
    )

    with pytest.raises(OperationFailure, match="already contains duplicate"):
        loader.insert_ontology_data_fast_initial(mock_ontology_classes, mock_ontology_relations)
