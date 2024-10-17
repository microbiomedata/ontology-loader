import json
import logging
import os
import sys

import click
import yaml

@click.group()
@click.option("-v", "--verbose", count=True)
@click.option("-q", "--quiet/--no-quiet")
@click.option(
    "--stacktrace/--no-stacktrace",
    default=False,
    show_default=True,
    help="If set then show full stacktrace on error",
)
@click.version_option(yaml.__version__)
def cli(verbose: int, quiet: bool, stacktrace: bool):
    """A CLI for interacting with ontology loader."""
    if not stacktrace:
        sys.tracebacklimit = 0

    logger = logging.getLogger()
    # Set handler for the root logger to output to the console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )

    # Clear existing handlers to avoid duplicate messages if function runs multiple times
    logger.handlers = []

    # Add the newly created console handler to the logger
    logger.addHandler(console_handler)
    if verbose >= 2:
        logger.setLevel(logging.DEBUG)
    elif verbose == 1:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARNING)
    if quiet:
        logger.setLevel(logging.ERROR)


@cli.command()
@click.argument("ontology", nargs=-1)
def load(ontology):
    """Fetch ontology details using oaklib"""


@cli.command()
@click.option(
    "--input-format",
    "-I",
    type=click.Choice(["json", "yaml"]),
    help="Input format. Not required unless reading from stdin.",
)
@click.option("--output-format", "-O", type=click.Choice(["cx2"]), required=True)
@click.option("--output", "-o", type=click.File("w"), default="-")
@click.option("--ndex-upload", is_flag=True, help="Upload to NDEx")
@click.argument("model", type=click.File("r"), default="-")
def convert(model, input_format, output_format, output, ndex_upload):
    return None

if __name__ == "__main__":
    cli()