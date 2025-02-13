"""Cli methods for ontology loading from the command line."""

import logging
import os
import click
from utils import load_yaml_from_package

from src.ontology_loader.mongodb_loader import MongoDBLoader
from src.ontology_loader.ontology_processor import OntologyProcessor

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@click.command()
@click.option("--db-host", default=os.getenv("MONGO_HOST", "localhost"), help="MongoDB connection URL")
@click.option("--db-port", default=int(os.getenv("MONGO_PORT", 27018)), help="MongoDB connection port")
@click.option("--db-name", default=os.getenv("MONGO_DB", "nmdc"), help="Database name")
@click.option("--db-user", default=os.getenv("MONGO_USER", "admin"), help="Database user")
@click.option("--db-password", default=os.getenv("MONGO_PASSWORD", ""), help="Database password")
@click.option("--source-ontology", default="envo", help="Lowercase ontology prefix, e.g., envo, go, uberon, etc.")
def main(db_host, db_port, db_name, db_user, db_password, source_ontology):
    """
    CLI entry point for the ontology loader.
    """
    logging.info(f"Processing ontology: {source_ontology}")

    # Load Schema View
    nmdc_sv = load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")
    # Initialize the Ontology Processor
    processor = OntologyProcessor(source_ontology)

    # Process ontology terms and return a list of OntologyClass dicts produced by linkml json dumper as dict
    ontology_classes = processor.get_terms_and_metadata()

    logger.info(f"Extracted {len(ontology_classes)} ontology classes.")

    # Process ontology relations and create OntologyRelation objects
    ontology_relations = processor.get_relations_closure()

    logger.info(f"Extracted {len(ontology_relations)} ontology relations.")

    # Connect to MongoDB
    db_manager = MongoDBLoader(
        schema_view=nmdc_sv,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        db_user=db_user,
        db_password=db_password
    )

    # Insert data into MongoDB
    db_manager.upsert_ontology_classes(ontology_classes)
    db_manager.insert_ontology_relations(ontology_relations)

    logging.info("Processing complete. Data inserted into MongoDB.")


if __name__ == "__main__":
    main()
