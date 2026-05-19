"""Cli methods for ontology loading from the command line."""

import logging

import click

from ontology_loader.ontology_load_controller import OntologyLoaderController

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command()
@click.option("--source-ontology", default="envo", help="Lowercase ontology prefix, e.g., envo, go, uberon, etc.")
@click.option("--output-directory", default=None, help="Output directory for reporting, default is /tmp")
@click.option("--generate-reports", default=True, help="Generate reports")
@click.option(
    "--report-mode",
    type=click.Choice(["compared", "upsert", "off"], case_sensitive=False),
    default="compared",
    show_default=True,
    help=(
        "How upsert reports are produced. "
        "'compared' batches a pre-read and only reports docs that actually changed (preserves prior fidelity); "
        "'upsert' skips the pre-read and reports every existing doc as updated (max throughput); "
        "'off' suppresses report tracking entirely (lowest memory)."
    ),
)
def cli(source_ontology, output_directory, generate_reports, report_mode):
    """
    CLI entry point for the ontology loader.

    Set the parameters for the connection to mongodb in the environment variables MONGO_HOST, MONGO_PORT,
    MONGO_USER, MONGO_PASSWORD, MONGO_DB.
    """
    logger.info(f"Processing ontology: {source_ontology}")

    loader = OntologyLoaderController(
        source_ontology=source_ontology,
        output_directory=output_directory,
        generate_reports=generate_reports,
        report_mode=report_mode,
    )
    loader.run_ontology_loader()


if __name__ == "__main__":
    cli()
