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
    "--force-refresh/--no-force-refresh",
    default=True,
    show_default=True,
    help=(
        "Whether to wipe the cached pystow directory and re-download the semsql sqlite from S3. "
        "Default (true) preserves prior behavior: every run starts fresh. "
        "Use --no-force-refresh for fast local iteration when the cached artifact is acceptable."
    ),
)
def cli(source_ontology, output_directory, generate_reports, force_refresh):
    """
    CLI entry point for the ontology loader.

    :param source_ontology: Lowercase ontology prefix, e.g., envo, go, uberon, etc.
    :param output_directory: Output directory for reporting, default is /tmp
    :param generate_reports: Generate reports or not, default is True
    :param force_refresh: If True (default), wipe the cached pystow directory and re-download
        the semsql sqlite from S3; if False, reuse the cached artifact when present.

    Set the parameters for the connection to mongodb in the environment variables MONGO_HOST, MONGO_PORT,
    MONGO_USER, MONGO_PASSWORD, MONGO_DB.
    """
    logger.info(f"Processing ontology: {source_ontology}")

    loader = OntologyLoaderController(
        source_ontology=source_ontology,
        output_directory=output_directory,
        generate_reports=generate_reports,
        force_refresh=force_refresh,
    )
    loader.run_ontology_loader()


if __name__ == "__main__":
    cli()
