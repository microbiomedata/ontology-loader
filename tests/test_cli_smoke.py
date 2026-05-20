"""
Smoke tests for the ontology_loader CLI and end-to-end controller path.

These tests verify high-level plumbing rather than algorithm correctness:

- The `--help` output advertises every documented flag (catches Click
  decoration drift; would have caught the kind of plumbing bug Copilot
  flagged on PR #21 where a stored attribute wasn't passed through to
  the underlying loader call).
- An unknown flag is rejected with a non-zero exit code.
- The controller runs end-to-end against a live MongoDB (skips when
  `MONGO_PASSWORD` isn't set) using a stubbed `OntologyProcessor`, so no
  semsql download is required. A `mongomock` variant was attempted but
  mongomock's strict `create_index` handling conflicts with linkml_store's
  repeated `index()` calls during `upsert_ontology_data`.
"""

from __future__ import annotations

import os
import subprocess
import sys
from unittest.mock import patch

import pytest
from nmdc_schema.nmdc import OntologyClass
from pymongo import MongoClient

from ontology_loader.ontology_load_controller import OntologyLoaderController

# Flags the CLI must advertise. Update this set when a new --foo option is
# wired into `cli.py`. Tests fail loudly if a flag's @click.option is added
# without a corresponding parameter (or vice versa).
EXPECTED_FLAGS = {
    "--source-ontology",
    "--output-directory",
    "--generate-reports",
    "--help",
}


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    """Invoke the CLI as a subprocess and return the completed process."""
    # S603 is bandit's "untrusted input to subprocess" warning. All args are
    # under test control (sys.executable + a hardcoded module path + test
    # literals), so the warning doesn't apply here.
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "ontology_loader.cli", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_cli_help_lists_expected_flags():
    """`ontology_loader --help` advertises every documented option."""
    result = _run_cli("--help")
    assert result.returncode == 0, f"unexpected exit code; stderr:\n{result.stderr}"
    combined = result.stdout + result.stderr
    missing = sorted(flag for flag in EXPECTED_FLAGS if flag not in combined)
    assert not missing, f"missing flags in --help output: {missing}\n--help output was:\n{combined}"


def test_cli_unknown_option_rejected():
    """An unknown flag exits non-zero."""
    result = _run_cli("--this-flag-does-not-exist")
    assert result.returncode != 0
    combined = (result.stdout + result.stderr).lower()
    # Click error message format: "Error: No such option: --this-flag-does-not-exist"
    assert (
        "no such option" in combined or "this-flag-does-not-exist" in combined
    ), f"expected Click error referencing the unknown option; got:\n{result.stdout + result.stderr}"


class _FakeOntologyProcessor:
    """
    Minimal stand-in for `OntologyProcessor`.

    Returns canned class and relation lists. Avoids the heavy real `__init__`,
    which downloads a semsql sqlite from S3 and opens it with oaklib.
    """

    def __init__(self, source_ontology, *args, **kwargs):
        self.ontology = source_ontology

    def get_terms_and_metadata(self):
        return [
            OntologyClass(
                id="TEST:001",
                name="alpha",
                type="nmdc:OntologyClass",
                is_root=True,
                is_obsolete=False,
                relations=[],
            ),
            OntologyClass(
                id="TEST:002",
                name="beta",
                type="nmdc:OntologyClass",
                is_root=False,
                is_obsolete=False,
                relations=[],
            ),
        ]

    def get_relations_closure(self, predicates=None, ontology_terms=None):
        relations = [
            {
                "subject": "TEST:002",
                "predicate": "rdfs:subClassOf",
                "object": "TEST:001",
                "type": "nmdc:OntologyRelation",
            }
        ]
        return relations, list(ontology_terms or [])


@pytest.mark.skipif(
    os.getenv("MONGO_PASSWORD") is None,
    reason="Skipping test: requires a live MongoDB (set MONGO_PASSWORD and other MONGO_* env vars)",
)
def test_controller_end_to_end_against_live_mongo(tmp_path):
    """
    OntologyLoaderController runs end-to-end against a real MongoDB and lands documents.

    Uses a stubbed `OntologyProcessor` so the test doesn't need a real semsql
    sqlite (would require a multi-minute download). Real MongoDB is required
    because mongomock's strict ``create_index`` handling conflicts with
    linkml_store's repeated index declarations during ``upsert_ontology_data``.
    """
    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    user = os.environ["MONGO_USERNAME"] if "MONGO_USERNAME" in os.environ else "admin"
    pw = os.environ["MONGO_PASSWORD"]
    db_name = "ontology_loader_smoke_test"

    client = MongoClient(
        host=host,
        port=port,
        username=user,
        password=pw,
        authSource="admin",
        directConnection=True,
    )

    # Safety check: refuse to run if a database with our scratch name already
    # exists, so we never silently overwrite someone's real data. The dev (or
    # CI) must investigate and drop the leftover explicitly before retrying.
    if db_name in client.list_database_names():
        pytest.fail(
            f"scratch database {db_name!r} already exists on the target MongoDB "
            f"({host}:{port}). Refusing to run to avoid overwriting it. "
            f"Investigate, then drop it explicitly to re-enable this test."
        )

    try:
        with patch(
            "ontology_loader.ontology_load_controller.OntologyProcessor",
            _FakeOntologyProcessor,
        ):
            loader = OntologyLoaderController(
                source_ontology="test",
                output_directory=str(tmp_path),
                generate_reports=False,
                mongo_client=client,
                db_name=db_name,
            )
            loader.run_ontology_loader()

        db = client[db_name]
        assert db.ontology_class_set.count_documents({"id": "TEST:001"}) == 1
        assert db.ontology_class_set.count_documents({"id": "TEST:002"}) == 1
        assert (
            db.ontology_relation_set.count_documents(
                {"subject": "TEST:002", "predicate": "rdfs:subClassOf", "object": "TEST:001"}
            )
            == 1
        )
    finally:
        # Clean up unconditionally so reruns are deterministic and the dev's
        # MongoDB doesn't accumulate test leftovers across runs.
        client.drop_database(db_name)
