"""Cli methods for ontology loading from the command line."""

import logging
import warnings

import click
from click.core import ParameterSource

from ontology_loader.ontology_load_controller import OntologyLoaderController

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command()
@click.option("--source-ontology", default="envo", help="Lowercase ontology prefix, e.g., envo, go, uberon, etc.")
@click.option("--output-directory", default=None, help="Output directory for reporting, default is /tmp")
@click.option(
    "--generate-reports/--no-generate-reports",
    default=True,
    show_default=True,
    help=(
        "DEPRECATED: prefer --report-mode. "
        "--no-generate-reports (without an explicit --report-mode) is treated as --report-mode off. "
        "If --report-mode is given explicitly, it wins and --generate-reports is ignored."
    ),
)
@click.option(
    "--report-mode",
    type=click.Choice(["compared", "upsert", "off"], case_sensitive=False),
    default="compared",
    show_default=True,
    help=(
        "How upsert reports are produced. "
        "'compared' batches a pre-read and only reports docs that actually changed (preserves prior fidelity); "
        "'upsert' skips the pre-read and reports every existing doc as updated (max throughput); "
        "'off' suppresses both in-memory report tracking and TSV writes (lowest memory, fastest)."
    ),
)
@click.pass_context
def cli(ctx, source_ontology, output_directory, generate_reports, report_mode):
    """
    CLI entry point for the ontology loader.

    Set the parameters for the connection to mongodb in the environment variables MONGO_HOST, MONGO_PORT,
    MONGO_USER, MONGO_PASSWORD, MONGO_DB.
    """
    report_mode_explicit = ctx.get_parameter_source("report_mode") == ParameterSource.COMMANDLINE

    if not generate_reports:
        if report_mode_explicit:
            warnings.warn(
                f"--no-generate-reports is deprecated; --report-mode={report_mode!r} (explicit) takes precedence. "
                "Drop --generate-reports from your invocation.",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            warnings.warn(
                "--no-generate-reports is deprecated; use --report-mode off instead. "
                "Treating this run as --report-mode off.",
                DeprecationWarning,
                stacklevel=2,
            )
            report_mode = "off"

    logger.info(f"Processing ontology: {source_ontology}")

    loader = OntologyLoaderController(
        source_ontology=source_ontology,
        output_directory=output_directory,
        report_mode=report_mode,
    )
    loader.run_ontology_loader()


if __name__ == "__main__":
    cli()
