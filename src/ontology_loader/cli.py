from typing import List
from linkml_runtime import SchemaView
from nmdc_schema.nmdc import OntologyClass
import click
from linkml_store import Client
import importlib.resources
import logging
from prefixmaps import load_context
from curies import Converter
from src.ontology_loader.ontology_processor import OntologyProcessor

@click.command()
@click.option('--db-url', default='mongodb://localhost:27017', help='MongoDB connection URL')
@click.option('--db-name', default='nmdc', help='Database name')
@click.option('--source-ontology', default='envo', help='Lowercase ontology prefix, e.g., envo, go, uberon, etc.')
def main(db_url, db_name, source_ontology):
    """Main function to process ontology and store metadata, ensuring the ontology database is available."""

    processor = OntologyProcessor(source_ontology)
    processor.get_terms_and_metadata()
    processor.get_relations_closure()

    # Connect to the database
    # db_client = connect_to_destination_store()

    # # Insert OntologyClass objects into MongoDB using linkml-store
    # insert_ontology_classes_into_db(db_client, term_dicts)
    #
    # # Print the completion message
    # print("Processing complete. Data inserted into the database.")


if __name__ == "__main__":
    main()
