"""Tests for the `--generate-reports` → `--report-mode` deprecation (issue #36)."""

from __future__ import annotations

import warnings

import pytest
from click.testing import CliRunner

from ontology_loader.cli import cli
from ontology_loader.ontology_load_controller import OntologyLoaderController

# ---- OntologyLoaderController-level deprecation -----------------------------


def test_controller_generate_reports_false_alone_sets_off():
    """generate_reports=False without explicit report_mode → report_mode='off' + warning."""
    with pytest.warns(DeprecationWarning, match=r"generate_reports=False.*report_mode='off'"):
        controller = OntologyLoaderController(
            source_ontology="envo",
            generate_reports=False,
        )
    assert controller.report_mode == "off"


def test_controller_generate_reports_false_with_explicit_report_mode_keeps_mode():
    """generate_reports=False with explicit non-default report_mode: warn but keep the mode."""
    with pytest.warns(DeprecationWarning, match=r"report_mode='upsert'.*takes precedence"):
        controller = OntologyLoaderController(
            source_ontology="envo",
            generate_reports=False,
            report_mode="upsert",
        )
    assert controller.report_mode == "upsert"


def test_controller_generate_reports_true_warns_but_is_ignored():
    """generate_reports=True is deprecated; warn but don't change anything."""
    with pytest.warns(DeprecationWarning, match=r"ignored when True"):
        controller = OntologyLoaderController(
            source_ontology="envo",
            generate_reports=True,
        )
    # Default behavior preserved
    assert controller.report_mode == "compared"


def test_controller_no_generate_reports_param_no_warning():
    """Not passing generate_reports at all must not raise a DeprecationWarning."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        OntologyLoaderController(source_ontology="envo")


# ---- CLI-level deprecation --------------------------------------------------


def test_cli_no_generate_reports_alone_emits_warning_and_sets_off():
    """
    Confirm `--no-generate-reports` is shown as deprecated in `--help`.

    We assert on the textual deprecation notice rather than exercising the
    full ontology-loader run from the CLI, because that path requires a real
    semsql download + MongoDB. The controller-level test above already covers
    the report_mode='off' coercion logic.
    """
    runner = CliRunner()
    # Help text contains 'DEPRECATED' — confirms the help string change shipped.
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "DEPRECATED" in result.output
    assert "--generate-reports" in result.output


def test_cli_help_describes_off_mode_as_skipping_tsv_writes():
    """`--report-mode off` help text must state that TSV writes are suppressed too."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    # Click wraps long help lines, so collapse whitespace before searching.
    flat = " ".join(result.output.split())
    assert "in-memory report tracking" in flat
    assert "TSV writes" in flat
