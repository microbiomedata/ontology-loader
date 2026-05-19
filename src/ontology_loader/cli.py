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
    "--emit-combined-closure/--no-emit-combined-closure",
    default=True,
    show_default=True,
    help="Emit `entailed_isa_partof_closure` (combined transitive closure over rdfs:subClassOf ∪ BFO:0000050).",
)
@click.option(
    "--emit-isa-closure/--no-emit-isa-closure",
    default=False,
    show_default=True,
    help="Emit `entailed_isa_closure` (transitive closure over rdfs:subClassOf only).",
)
@click.option(
    "--emit-partof-closure/--no-emit-partof-closure",
    default=False,
    show_default=True,
    help="Emit `entailed_partof_closure` (transitive closure over BFO:0000050 only).",
)
def cli(
    source_ontology,
    output_directory,
    generate_reports,
    emit_combined_closure,
    emit_isa_closure,
    emit_partof_closure,
):
    """
    CLI entry point for the ontology loader.

    :param source_ontology: Lowercase ontology prefix, e.g., envo, go, uberon, etc.
    :param output_directory: Output directory for reporting, default is /tmp
    :param generate_reports: Generate reports or not, default is True
    :param emit_combined_closure: Emit the combined `entailed_isa_partof_closure`. Default True.
    :param emit_isa_closure: Emit the `rdfs:subClassOf`-only `entailed_isa_closure`. Default False.
    :param emit_partof_closure: Emit the `BFO:0000050`-only `entailed_partof_closure`. Default False.

    Set the parameters for the connection to mongodb in the environment variables MONGO_HOST, MONGO_PORT,
    MONGO_USER, MONGO_PASSWORD, MONGO_DB.
    """
    logger.info(f"Processing ontology: {source_ontology}")

    loader = OntologyLoaderController(
        source_ontology=source_ontology,
        output_directory=output_directory,
        generate_reports=generate_reports,
        emit_combined_closure=emit_combined_closure,
        emit_isa_closure=emit_isa_closure,
        emit_partof_closure=emit_partof_closure,
    )
    loader.run_ontology_loader()


if __name__ == "__main__":
    cli()
