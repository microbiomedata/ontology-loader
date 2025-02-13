"""Cli methods for ontology loading from the command line."""

import logging
import os
import click
from src.ontology_loader.mongodb_loader import MongoDBLoader
from src.ontology_loader.loader import OntologyLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command()
@click.option("--db-host", default=os.getenv("MONGO_HOST", "localhost"), help="MongoDB connection URL")
@click.option("--db-port", default=int(os.getenv("MONGO_PORT", 27018)), help="MongoDB connection port")
@click.option("--db-name", default=os.getenv("MONGO_DB", "nmdc"), help="Database name")
@click.option("--db-user", default=os.getenv("MONGO_USER", "admin"), help="Database user")
@click.option("--db-password", default=os.getenv("MONGO_PASSWORD", ""), help="Database password")
@click.option("--source-ontology", default="envo", help="Lowercase ontology prefix, e.g., envo, go, uberon, etc.")
@click.option("--output-directory", default=None, help="Output directory for reporting, default is /tmp")
@click.option("--generate-reports", default=True, help="Generate reports")
def cli(db_host, db_port, db_name, db_user, db_password, source_ontology, output_directory, generate_reports):
    """CLI entry point for the ontology loader.

    :param db_host: MongoDB connection URL, default is localhost
    :param db_port: MongoDB connection port, default is 27018
    :param db_name: Database name, default is nmdc
    :param db_user: Database user, default is admin
    :param db_password: Database password, default is blank
    :param source_ontology: Lowercase ontology prefix, e.g., envo, go, uberon, etc.
    :param output_directory: Output directory for reporting, default is /tmp
    :param generate_reports: Generate reports or not, default is True
    """
    logger.info(f"Processing ontology: {source_ontology}")

    # Initialize the MongoDB Loader
    loader = OntologyLoader(db_host=db_host,
                            db_port=db_port,
                            db_name=db_name,
                            db_user=db_user,
                            db_password=db_password,
                            source_ontology=source_ontology,
                            output_directory=output_directory,
                            generate_reports=generate_reports)
    loader.run_ontology_loader()

if __name__ == "__main__":
    cli()
