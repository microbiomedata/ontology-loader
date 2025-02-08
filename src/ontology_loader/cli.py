import click
import logging
from src.ontology_loader.ontology_processor import OntologyProcessor
from src.ontology_loader.mongodb_loader import MongoDBLoader
from linkml_runtime import SchemaView
from nmdc_schema.nmdc import OntologyClass, OntologyRelation

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


@click.command()
@click.option('--db-url', default='mongodb://localhost:27017', help='MongoDB connection URL')
@click.option('--db-name', default='nmdc', help='Database name')
@click.option('--source-ontology', default='envo', help='Lowercase ontology prefix, e.g., envo, go, uberon, etc.')
def main(db_url, db_name, source_ontology):
    """
    Main function to process ontology and store metadata, ensuring the ontology database is available.
    """

    logging.info(f"Processing ontology: {source_ontology}")

    # Initialize the Ontology Processor
    processor = OntologyProcessor(source_ontology)

    # Process ontology terms and return a list of OntologyClass dicts produced by linkml json dumper as dict
    ontology_classes = processor.get_terms_and_metadata()

    logging.info(f"Extracted {len(ontology_classes)} ontology classes.")

    # Process ontology relations and create OntologyRelation objects
    ontology_relations = processor.get_relations_closure()

    logging.info(f"Extracted {len(ontology_relations)} ontology relations.")

    # Connect to MongoDB
    schema_view = SchemaView("nmdc_materialized_patterns.yaml")
    db_manager = MongoDBLoader(schema_view)

    # Insert data into MongoDB
    db_manager.insert_ontology_classes(ontology_classes)
    db_manager.insert_ontology_relations(ontology_relations)

    logging.info("Processing complete. Data inserted into MongoDB.")


if __name__ == "__main__":
    main()
