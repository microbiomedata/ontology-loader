"""Live-Mongo regression tests for `insert_ontology_data_fast_initial`'s duplicate protection."""

import os

import pytest
from nmdc_schema.nmdc import OntologyClass, OntologyRelation
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from ontology_loader.mongodb_loader import MongoDBLoader

pytestmark = pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)


@pytest.fixture
def live_mongo_client():
    """Return a raw pymongo client against the same live Mongo the other live-DB tests use."""
    return MongoClient(
        host=os.environ.get("MONGO_HOST", "localhost"),
        port=int(os.environ.get("MONGO_PORT", 27022)),
        username=os.environ.get("MONGO_USERNAME", "admin"),
        password=os.environ["MONGO_PASSWORD"],
    )


def test_fast_initial_unique_index_coexists_with_meticulous_nonunique_index(live_mongo_client):
    """
    Verify the fast-initial unique index does not conflict with the meticulous path's index.

    Both declare an index on the same key pattern (`id` for classes, or the `(subject, predicate,
    object)` triple for relations) but differ in `unique`. A GitHub Copilot review comment on
    https://github.com/microbiomedata/ontology-loader/pull/60 claimed that MongoDB raises an
    index-options conflict when two indexes share a key pattern but differ in `unique`, "regardless
    of using a different name" — which would mean `insert_ontology_data_fast_initial` could crash on
    `create_index` for any collection previously touched by `upsert_ontology_data` (envo/uberon/po
    in production). Manually verified against real Mongo 2026-08-19: this claim is false, MongoDB
    allows separately-named indexes on the same key pattern with different `unique` settings. This
    test is that manual verification made permanent, so a future MongoDB version regressing this
    behavior would be caught here rather than only in a NCBITaxon production run.
    """
    db_name = f"test_fast_initial_index_coexist_{os.getpid()}"
    db = live_mongo_client[db_name]
    try:
        class_collection = db["ontology_class_set"]
        relation_collection = db["ontology_relation_set"]

        # Simulate the meticulous path's existing indexes (upsert_ontology_data).
        class_collection.create_index("id", unique=False, name="ontology_class_index")
        relation_collection.create_index(
            [("subject", 1), ("predicate", 1), ("object", 1)],
            unique=False,
            name="ontology_relation_index",
        )

        # This must not raise. If it does, insert_ontology_data_fast_initial is broken for any
        # collection previously loaded via the meticulous path.
        class_collection.create_index("id", unique=True, name="ontology_class_fast_initial_unique_id_index")
        relation_collection.create_index(
            [("subject", 1), ("predicate", 1), ("object", 1)],
            unique=True,
            name="ontology_relation_fast_initial_unique_spo_index",
        )

        class_index_names = {idx["name"] for idx in class_collection.list_indexes()}
        relation_index_names = {idx["name"] for idx in relation_collection.list_indexes()}
        assert {"ontology_class_index", "ontology_class_fast_initial_unique_id_index"} <= class_index_names
        assert {"ontology_relation_index", "ontology_relation_fast_initial_unique_spo_index"} <= relation_index_names
    finally:
        live_mongo_client.drop_database(db_name)


def test_fast_initial_rerun_without_clearing_does_not_duplicate(live_mongo_client):
    """
    Verify the bug this PR fixes, end to end through the real MongoDBLoader (not a mock).

    Rerunning `insert_ontology_data_fast_initial` against an already-populated collection must not
    change the document counts, and a direct duplicate insert must raise `DuplicateKeyError`.
    """
    db_name = f"test_fast_initial_rerun_{os.getpid()}"
    live_mongo_client.drop_database(db_name)
    try:
        classes = [OntologyClass(id=f"TEST:{i:04d}", name=f"Term{i}", type="nmdc:OntologyClass") for i in range(50)]
        relations = [
            OntologyRelation(
                subject=f"TEST:{i:04d}",
                predicate="rdfs:subClassOf",
                object=f"TEST:{i + 1:04d}",
                type="nmdc:OntologyRelation",
            )
            for i in range(49)
        ]

        def fresh_loader():
            return MongoDBLoader(mongo_client=live_mongo_client, db_name=db_name)

        fresh_loader().insert_ontology_data_fast_initial(classes, relations)
        db = live_mongo_client[db_name]
        n_classes_1 = db["ontology_class_set"].count_documents({})
        n_rel_1 = db["ontology_relation_set"].count_documents({})
        assert n_classes_1 == 50
        assert n_rel_1 == 49

        # The unique index built above rejects a direct duplicate insert with the real server
        # exception, distinct from insert_ontology_data_fast_initial's own duplicate-skipping.
        with pytest.raises(DuplicateKeyError):
            db["ontology_class_set"].insert_one({"id": "TEST:0000", "type": "nmdc:OntologyClass"})

        # Rerun WITHOUT clearing: the exact retry-after-apparent-failure scenario.
        fresh_loader().insert_ontology_data_fast_initial(classes, relations)
        n_classes_2 = db["ontology_class_set"].count_documents({})
        n_rel_2 = db["ontology_relation_set"].count_documents({})
        assert n_classes_2 == n_classes_1, "classes were duplicated on rerun"
        assert n_rel_2 == n_rel_1, "relations were duplicated on rerun"
    finally:
        live_mongo_client.drop_database(db_name)
