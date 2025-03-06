"""Load and process ontology terms and relations into MongoDB."""

import logging
import os
from dataclasses import asdict, fields
from typing import List, Optional

from linkml_runtime import SchemaView
from linkml_store import Client
from nmdc_schema.nmdc import OntologyClass, OntologyRelation

from ontology_loader.mongo_db_config import MongoDBConfig
from ontology_loader.reporter import Report, ReportWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class MongoDBLoader:

    """MongoDB Loader class to upsert OntologyClass objects and insert OntologyRelation objects into MongoDB."""

    def __init__(self, schema_view: Optional[SchemaView] = None):
        """
        Initialize MongoDB using LinkML-store's client, prioritizing environment variables for connection details.

        :param schema_view: LinkML SchemaView for ontology
        """
        db_config = MongoDBConfig()
        self.schema_view = schema_view

        # Get database config from environment variables or fallback to MongoDBConfig defaults
        self.db_host = os.getenv("MONGO_HOST", db_config.db_host)
        self.db_port = int(os.getenv("MONGO_PORT", db_config.db_port))
        self.db_name = os.getenv("MONGO_DB", db_config.db_name)
        self.db_user = os.getenv("MONGO_USER", db_config.db_user)
        self.db_password = os.getenv("MONGO_PASSWORD", db_config.db_password)

        # Handle MongoDB connection string variations
        if self.db_host.startswith("mongodb://"):
            self.db_host = self.db_host.replace("mongodb://", "")
            self.db_port = int(self.db_host.split(":")[1])
            self.db_host = self.db_host.split(":")[0]

        self.handle = (
            f"mongodb://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}?authSource=admin"
        )

        logger.info(f"MongoDB connection string: {self.handle}")
        self.client = Client(handle=self.handle)
        self.db = self.client.attach_database(handle=self.handle)

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
            return (Report("update", [], []), Report("insert", [], []))

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

    def upsert_ontology_relations(
        self, ontology_relations: List[OntologyRelation], collection_name: str = "ontology_relation_set"
    ):
        """
        Upsert each OntologyRelation object into the 'ontology_relation_set' collection.

        :param ontology_relations: A list of OntologyRelation objects to upsert.
        :param collection_name: The name of the MongoDB collection to upsert into.
        :return: A Report object for insertions.
        """
        collection = self.db.create_collection(collection_name, recreate_if_exists=False)
        collection.index(["subject", "predicate", "object"], unique=False)

        if not ontology_relations:
            logging.info("No OntologyRelation objects to upsert.")
            return Report("insert", [], [])

        insertions_report = []

        # Ensure all relations are OntologyRelation instances
        processed_relations = [
            OntologyRelation(**relation) if isinstance(relation, dict) else relation for relation in ontology_relations
        ]

        for relation in processed_relations:
            filter_criteria = {"subject": relation.subject, "predicate": relation.predicate, "object": relation.object}
            if collection.find(filter_criteria).num_rows == 0:  # Only insert if it doesn't already exist
                collection.upsert([asdict(relation)], filter_fields=["subject", "predicate", "object"])
                logging.debug(
                    f"Inserted new OntologyRelation (subject={relation.subject}, "
                    f"predicate={relation.predicate}, "
                    f"object={relation.object})."
                )
                insertions_report.append([relation.subject, relation.predicate, relation.object])

        logging.info(
            f"Finished processing {len(ontology_relations)} OntologyRelation objects. "
            f"Upserted {len(insertions_report)} relations."
        )
        return Report("insert", insertions_report, ["subject", "predicate", "object"])

    def delete_obsolete_relations(
        self,
        relation_collection: str = "ontology_relation_set",
        class_collection: str = "ontology_class_set",
        output_directory: Optional[str] = None,
    ):
        """
        Delete relations where the subject or object is an OntologyClass with is_obsolete set to True.

        :param relation_collection: The name of the MongoDB collection storing ontology relations.
        :param class_collection: The name of the MongoDB collection storing ontology classes.
        :param output_directory: Directory where deletion report will be saved (optional).
        """
        relation_coll = self.db.create_collection(relation_collection, recreate_if_exists=False)
        class_coll = self.db.create_collection(class_collection, recreate_if_exists=False)

        # Find all ontology classes marked as obsolete
        obsolete_classes = class_coll.find({"is_obsolete": True})
        obsolete_ids = {doc["id"] for doc in obsolete_classes.rows}

        if not obsolete_ids:
            logger.info("No obsolete ontology classes found. No relations deleted.")
            return

        # Fetch relations to be deleted
        relations_to_delete = relation_coll.find(
            {"$or": [{"subject": {"$in": list(obsolete_ids)}}, {"object": {"$in": list(obsolete_ids)}}]}
        ).rows

        if not relations_to_delete:
            logger.info("No relations referencing obsolete classes found. No deletions performed.")
            return

        # Generate report data
        report_records = [
            [rel.get("subject", ""), rel.get("predicate", ""), rel.get("object", "")] for rel in relations_to_delete
        ]
        report_headers = ["subject", "predicate", "object"]

        # Write deletion report
        deletion_report = Report(report_type="deleted_relations", records=report_records, headers=report_headers)
        ReportWriter.write_reports([deletion_report], output_directory=output_directory)

        # Perform deletion
        delete_count = relation_coll.delete_where(
            {"$or": [{"subject": {"$in": list(obsolete_ids)}}, {"object": {"$in": list(obsolete_ids)}}]}
        )

        logger.info(f"{delete_count} relations deleted. Report saved to {output_directory or 'temporary directory'}.")
