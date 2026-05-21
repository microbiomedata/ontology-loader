"""Command-line interface for the ontology loader."""

import logging

import click

from ontology_loader.ontology_load_controller import VALID_MODES, OntologyLoaderController
from ontology_loader.ontology_processor import VALID_CLOSURES

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--source-ontology",
    "source_ontologies",
    multiple=True,
    required=True,
    help=(
        "Lowercase ontology prefix (e.g., envo, po, uberon, ncbitaxon). "
        "Pass repeatedly to load multiple ontologies in one invocation: "
        "--source-ontology envo --source-ontology po --source-ontology uberon. "
        "Processed sequentially in the given order; fail-fast on per-ontology error."
    ),
)
@click.option(
    "--report-directory",
    default=None,
    help=("Directory for TSV reports (only used when --mode=meticulous). " "Defaults to the system temp directory."),
)
@click.option(
    "--mode",
    type=click.Choice(VALID_MODES, case_sensitive=False),
    default="meticulous",
    show_default=True,
    help=(
        "meticulous: pure linkml-store, per-item upsert, TSV reports; matches 0.2.x behavior. "
        "fast-initial: raw pymongo insert_many, no upsert, no reports; for first-time installs "
        "of large ontologies."
    ),
)
@click.option(
    "--closure",
    "closures",
    type=click.Choice(VALID_CLOSURES, case_sensitive=False),
    multiple=True,
    default=("combined",),
    show_default=True,
    help=(
        "Which ancestry closures to emit. Repeatable; values combine. "
        "combined = rdfs:subClassOf + BFO:0000050 (entailed_isa_partof_closure); "
        "isa = rdfs:subClassOf only; partof = BFO:0000050 only; "
        "all = shorthand for combined + isa + partof (exclusive); "
        "none = no ancestry closure, direct relationships only (exclusive). "
        "Example: --closure isa --closure partof emits both."
    ),
)
def cli(source_ontologies, report_directory, mode, closures):
    r"""
    Load one or more ontologies into MongoDB.

    Set MongoDB connection details via MONGO_HOST, MONGO_PORT, MONGO_USERNAME, MONGO_PASSWORD, MONGO_DB.

    Behavior contract:

    \b
    - --mode meticulous (default): preserves Sierra/0.2.x behavior — pure linkml-store
      per-item upsert, force-refresh of the pystow cache, TSV reports written to
      --report-directory.
    - --mode fast-initial: maximum-throughput first-time install — raw pymongo
      insert_many, no upsert, no reporting, reuses the pystow cache if present.
    """
    closure_arg = list(closures)
    logger.info(f"Processing ontologies: {list(source_ontologies)} (mode={mode}, closure={closure_arg})")

    loader = OntologyLoaderController(
        source_ontology=list(source_ontologies),
        report_directory=report_directory,
        mode=mode,
        closure=closure_arg,
    )
    loader.run_ontology_loader()


if __name__ == "__main__":
    cli()
