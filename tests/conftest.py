"""Pytest configuration and session-scoped fixtures shared across the suite."""

from __future__ import annotations

import os
import sys

import pytest

# Keep these in sync with ``ontology_loader.mongo_db_config.MongoDBConfig`` so
# that the cleanup fixture talks to the same MongoDB the loader does.
_DEFAULT_HOST = "localhost"
_DEFAULT_PORT = 27022
_DEFAULT_USERNAME = "admin"
_LEAKED_DB_NAME = "test_db"


@pytest.fixture(scope="session", autouse=True)
def _drop_leftover_test_db():
    """
    Drop `test_db` at session teardown, but only if this session created it.

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

    Safety: if ``test_db`` already exists when the session **starts**, this
    fixture leaves it alone (matching Mark's standing rule that tests must
    not drop databases they didn't create). It is a no-op when no MongoDB
    credentials are available.
    """
    preexisted = _test_db_exists_at_setup()
    yield
    if preexisted is None or preexisted is True:
        # No MongoDB available (None) or it was there before this session ran;
        # not ours to clean up.
        return
    _drop_test_db_quietly()


def _test_db_exists_at_setup() -> bool | None:
    """Return True/False if MongoDB is reachable, None if no credentials/server."""
    if os.getenv("MONGO_PASSWORD") is None:
        return None
    client = _open_mongo_client()
    if client is None:
        return None
    try:
        return _LEAKED_DB_NAME in client.list_database_names()
    except Exception as e:  # noqa: BLE001 — best-effort probe
        print(
            f"\n[conftest] couldn't probe for pre-existing {_LEAKED_DB_NAME!r} "
            f"({type(e).__name__}: {e}); skipping teardown cleanup.",
            file=sys.stderr,
        )
        return None
    finally:
        client.close()


def _drop_test_db_quietly() -> None:
    """Drop `test_db` best-effort; never raise out of teardown."""
    client = _open_mongo_client()
    if client is None:
        return
    try:
        if _LEAKED_DB_NAME in client.list_database_names():
            client.drop_database(_LEAKED_DB_NAME)
    except Exception as e:  # noqa: BLE001 — best-effort cleanup
        print(
            f"\n[conftest] could not drop leftover {_LEAKED_DB_NAME!r} "
            f"({type(e).__name__}: {e}); the database may need manual cleanup.",
            file=sys.stderr,
        )
    finally:
        client.close()


def _open_mongo_client():
    """Open a short-timeout MongoClient; return None on any failure."""
    try:
        # Imported here so a missing pymongo (impossible in this repo, but
        # defensive) doesn't break test collection.
        from pymongo import MongoClient
    except ImportError:
        return None

    try:
        port = int(os.environ.get("MONGO_PORT", str(_DEFAULT_PORT)))
    except ValueError as e:
        print(
            f"\n[conftest] MONGO_PORT is not an integer ({e}); skipping test_db cleanup.",
            file=sys.stderr,
        )
        return None

    try:
        return MongoClient(
            host=os.environ.get("MONGO_HOST", _DEFAULT_HOST),
            port=port,
            username=os.environ.get("MONGO_USERNAME", _DEFAULT_USERNAME),
            password=os.environ["MONGO_PASSWORD"],
            authSource="admin",
            directConnection=True,
            serverSelectionTimeoutMS=2000,
        )
    except Exception as e:  # noqa: BLE001 — best-effort connect
        print(
            f"\n[conftest] could not connect to MongoDB for test_db cleanup " f"({type(e).__name__}: {e}).",
            file=sys.stderr,
        )
        return None
