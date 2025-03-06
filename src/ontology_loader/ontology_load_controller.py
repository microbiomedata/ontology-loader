"""Cli methods for ontology loading from the command line."""

import logging
import tempfile

from ontology_loader.mongodb_loader import MongoDBLoader
from ontology_loader.ontology_processor import OntologyProcessor
from ontology_loader.reporter import ReportWriter
from ontology_loader.utils import load_yaml_from_package

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class OntologyLoaderController:

    """
    OntologyLoader runner class for MongoDBLoader.

    This class is responsible for running the MongoDBLoader with the given parameters from code other than
    the command line support offered through the cli.py click interface.
    """

    def __init__(
        self,
        source_ontology: str = "envo",
        output_directory: str = tempfile.gettempdir(),
        generate_reports: bool = True,
    ):
        """Set the parameters for the OntologyLoader."""
        self.source_ontology = source_ontology
        self.output_directory = output_directory
        self.generate_reports = generate_reports

    def run_ontology_loader(self):
        """Run the OntologyLoader and insert data into MongoDB."""
        logging.info(f"Processing ontology: {self.source_ontology}")

        # Load Schema View
        nmdc_sv = load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")
        # Initialize the Ontology Processor
        processor = OntologyProcessor(self.source_ontology)

        # Process ontology terms and return a list of OntologyClass dicts produced by linkml json dumper as dict
        ontology_classes = processor.get_terms_and_metadata()

        logger.info(f"Extracted {len(ontology_classes)} ontology classes.")

        # Process ontology relations and create OntologyRelation objects
        ontology_relations, ontology_classes_relations = processor.get_relations_closure(
            ontology_terms=ontology_classes
        )

        logger.info(f"Extracted {len(ontology_relations)} ontology relations.")

        # Connect to MongoDB
        db_manager = MongoDBLoader(schema_view=nmdc_sv)
        logger.info(f"Db port {db_manager.db_port}")
        logger.info(f"MongoDB host {db_manager.db_host}")

        # Insert data into MongoDB
        updates_report, insertions_report = db_manager.upsert_ontology_classes(ontology_classes_relations)
        # Delete obsolete relations and insert new relations
        db_manager.delete_obsolete_relations()
        db_manager.upsert_ontology_relations(ontology_relations)

        # Optionally write job reports
        if self.generate_reports:
            ReportWriter.write_reports(
                reports=[updates_report, insertions_report], output_format="tsv", output_directory=self.output_directory
            )

        logger.info("Processing complete. Data inserted into MongoDB.")


if __name__ == "__main__":
    """Run the OntologyLoader."""
    OntologyLoaderController().run_ontology_loader()
