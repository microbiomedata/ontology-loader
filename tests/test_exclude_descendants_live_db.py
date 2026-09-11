"""
End-to-end check that an exclusion reaches MongoDB through the real controller.

Deliberately not a mock test. `AGENTS.md` prohibits them, and the mock version of
this that existed briefly could not fail: it replaced the controller itself, so
removing the controller's hand-off to the processor left the suite green. This
runs the real `OntologyLoaderController`, which builds a real `OntologyProcessor`
and writes real documents, and asserts on what actually landed in the database.
"""

import os
import uuid

import pytest
from pymongo import MongoClient

from ontology_loader.ontology_load_controller import OntologyLoaderController

pytestmark = pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None or os.getenv("ENABLE_DB_TESTS") != "true",
    reason="Skipping test: Requires MONGO_PASSWORD and ENABLE_DB_TESTS=true",
)

# ENVO:01000254 is `environmental system`. Chosen because it has a substantial but
# not enormous subtree, so the assertions below are about a real hierarchy rather
# than a handful of terms.
EXCLUDED_ROOT = "ENVO:01000254"


@pytest.fixture
def scratch_db():
    """Yield a throwaway database, dropped afterwards, never a shared collection."""
    client = MongoClient(
        host=os.environ.get("MONGO_HOST", "localhost"),
        port=int(os.environ.get("MONGO_PORT", 27017)),
        username=os.environ.get("MONGO_USERNAME", "admin"),
        password=os.environ["MONGO_PASSWORD"],
        authSource="admin",
    )
    name = f"ontology_loader_exclusion_{uuid.uuid4().hex[:8]}"

    # AGENTS.md rule 2 for DB-writing tests: verify the target does not already
    # exist before writing. A UUID collision is unlikely, but a leftover from an
    # interrupted run is not, and this fixture drops the database afterwards.
    # Fail loudly rather than overwrite and then delete someone's data.
    if name in client.list_database_names():
        client.close()
        pytest.fail(
            f"scratch database {name!r} already exists on the target MongoDB. "
            f"Refusing to run to avoid overwriting it. Investigate, then drop it "
            f"explicitly to re-enable this test."
        )

    try:
        yield client, name
    finally:
        client.drop_database(name)
        client.close()


def test_exclusion_reaches_mongodb_through_the_controller(scratch_db):
    """Excluded descendants must be absent from the loaded classes; the root must remain."""
    client, db_name = scratch_db
    controller = OntologyLoaderController(
        source_ontology=["envo"],
        mode="fast-initial",
        mongo_client=client,
        db_name=db_name,
        exclude_descendants_of=[EXCLUDED_ROOT],
    )
    controller.run_ontology_loader()

    classes = client[db_name]["ontology_class_set"]
    loaded = {doc["id"] for doc in classes.find({}, {"id": 1})}

    # Non-vacuity: the load has to have produced something, and the exclusion has
    # to have had something to exclude, or the assertions below prove nothing.
    assert len(loaded) > 1000, f"only {len(loaded)} classes loaded"
    from ontology_loader.ontology_processor import OntologyProcessor

    probe = OntologyProcessor("envo", force_refresh=False, exclude_descendants_of=[EXCLUDED_ROOT])
    excluded = probe.excluded_entities
    assert len(excluded) > 100, f"only {len(excluded)} terms excluded; pick a bigger subtree"

    assert EXCLUDED_ROOT in loaded, "the named root must be kept"
    leaked = loaded & excluded
    assert not leaked, f"{len(leaked)} excluded descendants reached MongoDB, e.g. {sorted(leaked)[:5]}"

    relations = client[db_name]["ontology_relation_set"]
    bad = relations.count_documents(
        {"$or": [{"subject": {"$in": sorted(excluded)[:500]}}, {"object": {"$in": sorted(excluded)[:500]}}]}
    )
    assert bad == 0, f"{bad} relations reference an excluded term"
