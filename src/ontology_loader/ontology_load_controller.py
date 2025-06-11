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
        mongo_client=None,
        db_name: str = None,
    ):
        """
        Set the parameters for the OntologyLoader.

        Args:
            source_ontology: Name of the ontology to load (default: "envo")
            output_directory: Directory where reports will be written (default: temp directory)
            generate_reports: Whether to generate reports (default: True)
            mongo_client: Optional existing MongoDB client to use instead of creating a new connection
            db_name: Database name to use with existing client (required when mongo_client is provided)

        """
        self.source_ontology = source_ontology
        self.output_directory = output_directory
        self.generate_reports = generate_reports
        self.mongo_client = mongo_client
        self.db_name = db_name

        # Validate that db_name is provided when mongo_client is provided
        if self.mongo_client and not self.db_name:
            raise ValueError("Database name (db_name) is required when providing a MongoDB client")

    def run_ontology_loader(self):
        """Run the OntologyLoader and insert data into MongoDB."""
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

        # Connect to MongoDB, using the existing client if provided
        db_manager = MongoDBLoader(schema_view=nmdc_sv, mongo_client=self.mongo_client, db_name=self.db_name)

        # Only log connection details if we're using our own connection
        if not self.mongo_client:
            logger.info(f"Db port {db_manager.db_config.db_port}")
            logger.info(f"MongoDB host {db_manager.db_config.db_host}")

        # Update data
        updates_report, insertions_report, insert_relations_report = db_manager.upsert_ontology_data(
            ontology_classes_relations, ontology_relations
        )

        # Optionally write job reports
        if self.generate_reports:
            ReportWriter.write_reports(
                reports=[updates_report, insertions_report, insert_relations_report],
                output_format="tsv",
                output_directory=self.output_directory,
            )

        logger.info("Processing complete. Data inserted into MongoDB.")


if __name__ == "__main__":
    """Run the OntologyLoader."""
    OntologyLoaderController().run_ontology_loader()
