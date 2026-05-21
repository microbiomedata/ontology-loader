"""
Exercise the 0.2.x → 0.3.x kwarg migration paths on OntologyLoaderController.

These tests pin down the back-compat surface that the nmdc-runtime Dagster job depends on:
old kwargs must keep working with a DeprecationWarning, and explicit conflicts must raise.
"""

from __future__ import annotations

import os
import tempfile
import warnings

import pytest
from click.testing import CliRunner
from ontology_loader.cli import cli
from ontology_loader.ontology_load_controller import OntologyLoaderController
from ontology_loader.ontology_processor import _normalize_closure_spec

# Reuse the same temp dir across tests; bandit/ruff S108 would flag literal "/tmp/..." paths.
_TMP_DIR = tempfile.gettempdir()
_TMP_A = os.path.join(_TMP_DIR, "ontology_loader_test_a")
_TMP_B = os.path.join(_TMP_DIR, "ontology_loader_test_b")


# --- output_directory → report_directory ---------------------------------------------------


def test_output_directory_alias_works_with_warning():
    """``output_directory=`` keeps working but emits DeprecationWarning."""
    with pytest.warns(DeprecationWarning, match=r"output_directory.*deprecated"):
        controller = OntologyLoaderController(
            source_ontology="envo",
            output_directory=_TMP_A,
        )
    assert controller.report_directory == _TMP_A
    # The 0.2.x-shape accessor must keep returning the same value.
    assert controller.output_directory == _TMP_A


def test_passing_both_output_directory_and_report_directory_raises():
    """Conflicting old + new kwargs raises with a directive error message."""
    with pytest.raises(
        ValueError,
        match=r"(output_directory.*report_directory|report_directory.*output_directory)",
    ):
        OntologyLoaderController(
            source_ontology="envo",
            output_directory=_TMP_A,
            report_directory=_TMP_B,
        )


# --- generate_reports → mode='fast-initial' / no-op -----------------------------------------


def test_generate_reports_false_maps_to_fast_initial_with_warning():
    """``generate_reports=False`` → ``mode='fast-initial'`` + DeprecationWarning."""
    with pytest.warns(DeprecationWarning, match=r"generate_reports=False.*fast-initial"):
        controller = OntologyLoaderController(
            source_ontology="envo",
            generate_reports=False,
        )
    assert controller.mode == "fast-initial"


def test_generate_reports_true_is_noop_with_warning():
    """``generate_reports=True`` keeps the meticulous default but warns."""
    with pytest.warns(DeprecationWarning, match=r"generate_reports=True.*no-op"):
        controller = OntologyLoaderController(
            source_ontology="envo",
            generate_reports=True,
        )
    assert controller.mode == "meticulous"


def test_generate_reports_false_with_explicit_mode_raises():
    """generate_reports=False + explicit mode that isn't 'meticulous' is a conflict."""
    with pytest.raises(ValueError, match=r"generate_reports=False.*mode"):
        OntologyLoaderController(
            source_ontology="envo",
            generate_reports=False,
            mode="fast-initial",  # explicit, but conflicts under the migration rule
        )


def test_generate_reports_invalid_type_raises():
    """Non-bool value for generate_reports raises cleanly."""
    with pytest.raises(ValueError, match=r"generate_reports must be True or False"):
        OntologyLoaderController(source_ontology="envo", generate_reports="yes")


def test_no_warning_when_only_new_kwargs_are_passed():
    """The 'happy path' must not emit DeprecationWarning."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        OntologyLoaderController(
            source_ontology="envo",
            report_directory=_TMP_DIR,
            mode="meticulous",
            closure="combined",
        )


# --- 0.2.x exact Dagster call shape ---------------------------------------------------------


def test_dagster_0_2_x_call_shape_still_works():
    """Construct controller with the exact 0.2.2 kwarg shape used by nmdc-runtime's load_ontology op."""
    with pytest.warns(DeprecationWarning):
        controller = OntologyLoaderController(
            source_ontology="envo",
            output_directory=_TMP_DIR,
            generate_reports=True,
            mongo_client=None,  # the op passes a real client; None here just to construct
            db_name="nmdc",
        )
    assert controller.mode == "meticulous"
    assert controller.source_ontology == "envo"
    assert controller.db_name == "nmdc"


# --- source_ontology: scalar or list --------------------------------------------------------


def test_source_ontology_accepts_string():
    """A single string still works and is wrapped to a one-element list."""
    c = OntologyLoaderController(source_ontology="envo")
    assert c.source_ontologies == ["envo"]
    assert c.source_ontology == "envo"


def test_source_ontology_accepts_list():
    """A list is preserved in order; the scalar accessor returns the first element."""
    c = OntologyLoaderController(source_ontology=["envo", "po", "uberon"])
    assert c.source_ontologies == ["envo", "po", "uberon"]
    assert c.source_ontology == "envo"  # 0.2.x-shape accessor returns the first one


def test_source_ontology_empty_list_raises():
    """An empty list is a usage error."""
    with pytest.raises(ValueError, match=r"at least one ontology"):
        OntologyLoaderController(source_ontology=[])


# --- mode validation ------------------------------------------------------------------------


def test_unknown_mode_raises():
    """An unknown mode name is rejected at construction."""
    with pytest.raises(ValueError, match=r"Unknown mode 'nope'"):
        OntologyLoaderController(source_ontology="envo", mode="nope")


# --- closure spec ---------------------------------------------------------------------------


def test_closure_combined_single():
    """A single 'combined' string normalizes to a one-element tuple."""
    assert _normalize_closure_spec("combined") == ("combined",)


def test_closure_list_combines():
    """A list of distinct closure names is preserved in order."""
    assert _normalize_closure_spec(["isa", "partof"]) == ("isa", "partof")


def test_closure_dedupes_and_preserves_order():
    """Duplicates are dropped; first occurrence wins."""
    assert _normalize_closure_spec(["isa", "combined", "isa"]) == ("isa", "combined")


def test_closure_all_expands():
    """``'all'`` expands to the three concrete closure names."""
    assert _normalize_closure_spec("all") == ("combined", "isa", "partof")


def test_closure_all_is_exclusive():
    """Combining 'all' with any other value raises."""
    with pytest.raises(ValueError, match=r"closure='all' is exclusive"):
        _normalize_closure_spec(["all", "isa"])


def test_closure_none_is_exclusive():
    """Combining 'none' with any other value raises."""
    with pytest.raises(ValueError, match=r"closure='none' is exclusive"):
        _normalize_closure_spec(["none", "isa"])


def test_closure_unknown_raises():
    """An unknown closure name is rejected."""
    with pytest.raises(ValueError, match=r"Unknown closure 'bogus'"):
        _normalize_closure_spec("bogus")


def test_closure_empty_raises():
    """An empty list is rejected with a directive error."""
    with pytest.raises(ValueError, match=r"at least one value"):
        _normalize_closure_spec([])


# --- CLI surface ---------------------------------------------------------------------------


def test_cli_help_lists_four_flags():
    """`--help` must surface the four flags and not the old ones."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    out = result.output

    # New flags are present.
    assert "--source-ontology" in out
    assert "--report-directory" in out
    assert "--mode" in out
    assert "--closure" in out

    # Old flags are NOT in the CLI surface (deprecation lives at the Python kwarg layer).
    assert "--output-directory" not in out
    assert "--generate-reports" not in out


def test_cli_requires_source_ontology():
    """Click must reject an invocation with no --source-ontology."""
    runner = CliRunner()
    result = runner.invoke(cli, [])
    assert result.exit_code != 0
    assert "source-ontology" in result.output.lower() or "source_ontologies" in result.output.lower()
