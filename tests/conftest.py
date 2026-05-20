"""Pytest configuration and session-scoped fixtures shared across the suite."""

from __future__ import annotations

import os
import sys

import pytest


@pytest.fixture(scope="session", autouse=True)
def _drop_leftover_test_db():
    """Session teardown: drop any `test_db` left behind by mock-using tests.

    Two tests pass ``db_name="test_db"`` when constructing the loader with a
    ``MagicMock`` client:

    - ``tests/test_mock_mongodb_loader.py::test_init_with_existing_client``
    - ``tests/test_ontology_load_controller.py::ontology_loader_with_client``
      fixture (used by ``test_obsolete_handling_with_mocks``).

    Despite the mock, ``MongoDBLoader.__init__`` instantiates a real
    ``linkml_store.Client`` and calls ``attach_database(...)`` with a handle
    f-string'd from the mock's ``.address`` attribute. That side effect ends
    up creating an empty ``test_db`` database in whatever MongoDB the
    ``MONGO_*`` env vars point at. The underlying design flaw is tracked in
    #44; this fixture is the immediate cleanup for #43.

    The fixture is autouse and session-scoped so it runs once at session
    teardown, regardless of which tests actually ran. It is a no-op when no
    MongoDB credentials are available.
    """
    yield
    # --- teardown ---
    if os.getenv("MONGO_PASSWORD") is None:
        return
    # Import here so a missing pymongo (impossible in this repo, but defensive)
    # doesn't break session collection.
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError

    try:
        client = MongoClient(
            host=os.environ.get("MONGO_HOST", "localhost"),
            port=int(os.environ.get("MONGO_PORT", "27017")),
            username=os.environ.get("MONGO_USERNAME", "admin"),
            password=os.environ["MONGO_PASSWORD"],
            authSource="admin",
            directConnection=True,
            serverSelectionTimeoutMS=2000,
        )
        if "test_db" in client.list_database_names():
            client.drop_database("test_db")
    except PyMongoError as e:
        # Best-effort: surface a warning to stderr but don't fail the session
        # over teardown. A persistent leak should be tracked via #43/#44.
        print(
            f"\n[conftest] could not drop leftover test_db ({type(e).__name__}: {e}); "
            "the database may need manual cleanup.",
            file=sys.stderr,
        )
