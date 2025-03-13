"""Load and process ontology terms and relations into MongoDB."""

import logging
import os
from dataclasses import asdict, fields
from typing import List, Optional

from linkml_runtime import SchemaView
from linkml_store import Client
from nmdc_schema.nmdc import OntologyClass, OntologyRelation

from ontology_loader.mongo_db_config import MongoDBConfig
from ontology_loader.reporter import Report

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _handle_obsolete_terms(obsolete_terms, class_collection, relation_collection):
    """
    Handle obsolete ontology terms by updating their status and removing relations.

    :param obsolete_terms: List of obsolete term IDs
    :param class_collection: MongoDB collection for classes
    :param relation_collection: MongoDB collection for relations
    """
    if not obsolete_terms:
        return

    for term_id in obsolete_terms:
        term = class_collection.find({"id": term_id}).rows
        if term:
            term_obj = term[0]
            term_obj.relations = []
            term_obj.is_obsolete = True
            class_collection.upsert([asdict(term_obj)], filter_fields=["id"])
            logging.debug(f"Marked OntologyClass {term_id} as obsolete and cleared relations.")

    relation_collection.delete(
        {"$or": [{"subject": {"$in": obsolete_terms}}, {"object": {"$in": obsolete_terms}}]})
    logging.debug("Removed relations referencing obsolete terms.")


def _upsert_relation(relation, collection):
    """
    Upsert a single relation and return report data if valid.

    :param relation: OntologyRelation object to upsert
    :param collection: MongoDB collection for relations
    :return: List with relation data or None if invalid
    """
    if type(relation) is OntologyRelation:
        relation = asdict(relation)

    if not relation.get("subject") or not relation.get("predicate") or not relation.get("object"):
        logging.warning(f"Skipping invalid relation: {relation}")
        return None

    collection.upsert([relation], filter_fields=["subject", "predicate", "object"])
    logging.debug(f"Inserted OntologyRelation: {relation}")
    return [relation.get("subject"), relation.get("predicate"), relation.get("object")]


def _upsert_ontology_class(obj, collection, ontology_fields):
    """
    Upsert a single ontology class and return update report data.

    :param obj: OntologyClass object to upsert
    :param collection: MongoDB collection for classes
    :param ontology_fields: List of field names for OntologyClass
    :return: Tuple of (was_updated, report_row)
    """
    filter_criteria = {"id": obj.id}
    query_result = collection.find(filter_criteria)
    existing_doc = query_result.rows[0] if query_result.num_rows > 0 else None
    report_row = [obj.id] + [getattr(obj, field, "") for field in ontology_fields]

    if existing_doc:
        updated_fields = {
            key: getattr(obj, key) for key in ontology_fields if getattr(obj, key) != existing_doc.get(key)
        }
        if updated_fields:
            collection.upsert(
                [asdict(obj)], filter_fields=["id"], update_fields=list(updated_fields.keys())
            )
            logging.debug(f"Updated OntologyClass (id={obj.id}): {updated_fields}")
            return True, report_row
    else:
        collection.upsert([asdict(obj)], filter_fields=["id"], update_fields=ontology_fields)
        logging.debug(f"Inserted OntologyClass (id={obj.id}).")
        return False, report_row

    return None, None


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

    def upsert_ontology_data(
        self,
        ontology_classes: List[OntologyClass],
        ontology_relations: List[OntologyRelation],
        class_collection_name: str = "ontology_class_set",
        relation_collection_name: str = "ontology_relation_set",
    ):
        """
        Upsert ontology terms, clear/re-populate ontology relations, handle obsolescence, and manage hierarchy changes.

        :param ontology_classes: A list of OntologyClass objects to upsert.
        :param ontology_relations: A list of OntologyRelation objects to upsert.
        :param class_collection_name: MongoDB collection name for ontology classes.
        :param relation_collection_name: MongoDB collection name for ontology relations.
        :return: A tuple of three reports: class updates, class insertions, and relation insertions.
        """
        class_collection = self.db.create_collection(class_collection_name, recreate_if_exists=False)
        relation_collection = self.db.create_collection(relation_collection_name, recreate_if_exists=False)
        class_collection.index("id", unique=False)
        relation_collection.index(["subject", "predicate", "object"], unique=False)

        # Step 1: Upsert ontology terms
        updates_report, insertions_report = [], []
        ontology_fields = [field.name for field in fields(OntologyClass)]

        for obj in ontology_classes:
            was_updated, report_row = _upsert_ontology_class(obj, class_collection, ontology_fields)
            if was_updated:
                updates_report.append(report_row)
            elif was_updated is False:  # Not None, but False (new insertion)
                insertions_report.append(report_row)

        # Step 2: Clear ontology term relations for each term 
        for obj in ontology_classes:
            relation_collection.delete({"subject": obj.id})

        # Step 3: Handle obsolete ontology terms
        obsolete_terms = [obj.id for obj in ontology_classes if obj.is_obsolete]
        _handle_obsolete_terms(obsolete_terms, class_collection, relation_collection)

        # Step 4: Re-populate relations
        insertions_report_relations = []
        for relation in ontology_relations:
            relation_data = _upsert_relation(relation, relation_collection)
            if relation_data:
                insertions_report_relations.append(relation_data)

        logging.info(
            f"Finished upserting ontology data: {len(ontology_classes)} classes, {len(ontology_relations)} relations."
        )
        return (
            Report("update", updates_report, ontology_fields),
            Report("insert", insertions_report, ontology_fields),
            Report("insert", insertions_report_relations, ["subject", "predicate", "object"]),
        )
