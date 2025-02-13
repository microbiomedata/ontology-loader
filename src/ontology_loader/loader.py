"""Cli methods for ontology loading from the command line."""

import logging
import os
from src.ontology_loader.utils import load_yaml_from_package
from src.ontology_loader.mongodb_loader import MongoDBLoader
from src.ontology_loader.ontology_processor import OntologyProcessor
from src.ontology_loader.ontology_report import ReportWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class OntologyLoader:
    """OntologyLoader runner class for MongoDBLoader.

    This class is responsible for running the MongoDBLoader with the given parameters from code other than
    the command line support offered through the cli.py click interface.

    :param db_host: MongoDB connection URL, default is value of MONGO_HOST environment variable or "localhost"
    :param db_port: MongoDB connection port, default is value of MONGO_PORT environment variable or 27018
    :param db_name: Database name, default is value of MONGO_DB environment variable or "nmdc"
    :param db_user: Database user, default is value of MONGO_USER environment variable or "admin"
    :param db_password: Database password, default is value of MONGO_PASSWORD environment variable or blank
    :param source_ontology: Lowercase ontology prefix, e.g., envo, go, uberon, etc.
    :param output_directory: Output directory for reporting, default is system temp directory
    :param generate_reports: Generate reports or not, default is True
    """

    def __init__(self,
                 db_host=os.getenv("MONGO_HOST", "localhost"),
                 db_port=int(os.getenv("MONGO_PORT", 27018)),
                 db_name=os.getenv("MONGO_DB", "nmdc"),
                 db_user=os.getenv("MONGO_USER", "admin"),
                 db_password=os.getenv("MONGO_PASSWORD", ""),
                 source_ontology="envo",
                 output_directory=None,
                 generate_reports=True):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.source_ontology = source_ontology
        self.output_directory = output_directory
        self.generate_reports = generate_reports

    def run_ontology_loader(self):
        logging.info(f"Processing ontology: {self.source_ontology}")

        # Load Schema View
        nmdc_sv = load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")
        # Initialize the Ontology Processor
        processor = OntologyProcessor(self.source_ontology)

        # Process ontology terms and return a list of OntologyClass dicts produced by linkml json dumper as dict
        ontology_classes = processor.get_terms_and_metadata()

        logger.info(f"Extracted {len(ontology_classes)} ontology classes.")

        # Process ontology relations and create OntologyRelation objects
        ontology_relations = processor.get_relations_closure()

        logger.info(f"Extracted {len(ontology_relations)} ontology relations.")

        # Connect to MongoDB
        db_manager = MongoDBLoader(
            schema_view=nmdc_sv,
            db_host=self.db_host,
            db_port=self.db_port,
            db_name=self.db_name,
            db_user=self.db_user,
            db_password=self.db_password
        )

        # Insert data into MongoDB
        updates_report, insertions_report = db_manager.upsert_ontology_classes(ontology_classes)
        db_manager.insert_ontology_relations(ontology_relations)
        db_manager.insert_ontology_relations(ontology_relations)

        # Optionally write job reports
        if self.generate_reports:
            ReportWriter.write_reports(reports=[updates_report, insertions_report],
                                       output_format="tsv",
                                       output_directory=self.output_directory)

        logger.info("Processing complete. Data inserted into MongoDB.")


if __name__ == "__main__":
    OntologyLoader().run_ontology_loader()
