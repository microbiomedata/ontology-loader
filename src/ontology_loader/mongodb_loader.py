"""Load and process ontology terms and relations into MongoDB."""

import logging
from dataclasses import asdict, fields
from typing import List, Optional

from linkml_runtime import SchemaView
from linkml_store import Client
from nmdc_schema.nmdc import OntologyClass

from src.ontology_loader.mongo_db_config import MongoDBConfig
from src.ontology_loader.reporter import Report

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class MongoDBLoader:

    """MongoDB Loader class to upsert OntologyClass objects and insert OntologyRelation objects into MongoDB."""

    def __init__(self, schema_view: Optional[SchemaView] = None):
        """
        Initialize MongoDB using LinkML-store's client.

        :param schema_view: LinkML SchemaView for ontology
        :param db_config: Singleton configuration for MongoDB connection
        """
        db_config = MongoDBConfig()
        self.schema_view = schema_view
        self.db_host = db_config.db_host
        self.db_port = db_config.db_port
        self.db_name = db_config.db_name
        self.db_user = db_config.db_user
        self.db_password = db_config.db_password

        # TODO: it might be that we are providing the connection string "incorrectly" (or differently) in linkml-store
        # this exists so that the default env parameters in nmdc-runtime can be used as they are currently
        # specified.
        if self.db_host.startswith("mongodb://"):
            # mongodb://mongo:27017
            self.db_host = self.db_host.replace("mongodb://", "")
            self.db_port = int(self.db_host.split(":")[1])
            self.db_host = self.db_host.split(":")[0]

        self.handle = (
            f"mongodb://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}?authSource=admin"
        )

        logger.info(self.handle)
        self.client = Client(handle=self.handle)

        # Explicitly set the correct database
        self.db = self.client.attach_database(
            handle=self.handle,  # Ensure correct database is used
        )
        logger.info(f"Connected to MongoDB: {self.db}")

    def upsert_ontology_classes(
        self, ontology_classes: List[OntologyClass], collection_name: str = "ontology_class_set"
    ):
        """
        Upsert each OntologyClass object into the 'ontology_class_set' collection and return reports.

        :param ontology_classes: A list of OntologyClass objects to upsert
        :param collection_name: The name of the MongoDB collection to upsert into.
        :return: A tuple of two Report objects: one for updates and one for insertions.
        """
        collection = self.db.create_collection(collection_name, recreate_if_exists=False)
        collection.index("id", unique=False)
        logging.info(collection_name)

        if not ontology_classes:
            logging.info("No OntologyClass objects to upsert.")
            return Report("update", [], []), Report("insert", [], [])

        updates_report = []
        insertions_report = []
        ontology_fields = [field.name for field in fields(OntologyClass)]

        for obj in ontology_classes:
            filter_criteria = {"id": obj.id}
            query_result = collection.find(filter_criteria)
            existing_doc = query_result.rows[0] if query_result.num_rows > 0 else None

            if existing_doc:
                updated_fields = {
                    key: getattr(obj, key) for key in ontology_fields if getattr(obj, key) != existing_doc.get(key)
                }
                if updated_fields:
                    collection.upsert([asdict(obj)], filter_fields=["id"], update_fields=list(updated_fields.keys()))
                    logging.debug(f"Updated existing OntologyClass (id={obj.id}): {updated_fields}")
                    updates_report.append([obj.id] + [getattr(obj, field, "") for field in ontology_fields])
                else:
                    logging.debug(f"No changes detected for OntologyClass (id={obj.id}). Skipping update.")
            else:
                collection.upsert([asdict(obj)], filter_fields=["id"], update_fields=ontology_fields)
                logging.debug(f"Inserted new OntologyClass (id={obj.id}).")
                insertions_report.append([obj.id] + [getattr(obj, field, "") for field in ontology_fields])

        logging.info(f"Finished upserting {len(ontology_classes)} OntologyClass objects into MongoDB.")
        return Report("update", updates_report, ontology_fields), Report("insert", insertions_report, ontology_fields)

    def insert_ontology_relations(self, ontology_relations, collection_name: str = "ontology_relation_set"):
        """
        Insert each OntologyClass object into the 'ontology_class_set' collection.

        :param ontology_relations: A list of OntologyRelation objects to insert
        :param collection_name: The name of the MongoDB collection to insert into.

        """
        collection = self.db.create_collection(collection_name, recreate_if_exists=False)
        if ontology_relations:
            for relation in ontology_relations:
                collection.insert(relation)
        else:
            logger.info("No OntologyRelation objects to insert.")
