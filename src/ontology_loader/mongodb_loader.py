"""Load and process ontology terms and relations into MongoDB."""

import logging
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
        if len(class_collection.find({"id": term_id}).rows) > 1:
            logging.warning(f"Multiple entries found for OntologyClass {term_id}.")

        if len(class_collection.find({"id": term_id}).rows) == 1:
            term = class_collection.find({"id": term_id}).rows[0]
            if type(term) is OntologyClass:
                term = asdict(term)
            term["relations"] = []
            term["is_obsolete"] = True
            class_collection.upsert([term], filter_fields=["id"], update_fields=["is_obsolete", "relations"])
            logging.debug(f"Marked OntologyClass {term_id} as obsolete and cleared relations.")

    relation_collection.delete({"$or": [{"subject": {"$in": obsolete_terms}}, {"object": {"$in": obsolete_terms}}]})
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

    # Get all relation fields to use as update_fields
    update_fields = list(relation.keys())
    collection.upsert([relation], filter_fields=["subject", "predicate", "object"], update_fields=update_fields)
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
            collection.upsert([asdict(obj)], filter_fields=["id"], update_fields=list(updated_fields.keys()))
            logging.debug(f"Updated OntologyClass (id={obj.id}): {updated_fields}")
            return True, report_row
    else:
        collection.upsert([asdict(obj)], filter_fields=["id"], update_fields=ontology_fields)
        logging.debug(f"Inserted OntologyClass (id={obj.id}).")
        return False, report_row

    return None, None


def get_mongo_connection_string(db_config) -> str:
    """
    Generate a formatted MongoDB connection string from a db_config object.

    Args:
        db_config: An object containing MongoDB connection parameters.

    Returns:
        str: A properly formatted MongoDB connection string.

    """
    # Handle MongoDB connection string variations
    if db_config.db_host.startswith("mongodb://"):
        parts = db_config.db_host.replace("mongodb://", "").split(":")
        db_config.db_host = parts[0]
        if len(parts) > 1 and ":" in db_config.db_host + ":" + parts[1]:
            port_part = parts[1].split("/")[0]
            if port_part.isdigit():
                db_config.db_port = int(port_part)

    connection_string = (
        f"mongodb://{db_config.db_user}:{db_config.db_password}@"
        f"{db_config.db_host}:{db_config.db_port}/"
        f"{db_config.db_name}?{db_config.auth_params}"
    )
    return connection_string


class MongoDBLoader:

    """MongoDB Loader class to upsert OntologyClass objects and insert OntologyRelation objects into MongoDB."""

    def __init__(self, schema_view: Optional[SchemaView] = None):
        """
        Initialize MongoDB using LinkML-store's client, prioritizing environment variables for connection details.

        :param schema_view: LinkML SchemaView for ontology
        """
        # Get database config from environment variables or fallback to MongoDBConfig defaults
        self.db_config = MongoDBConfig()
        self.schema_view = schema_view

        self.handle = get_mongo_connection_string(self.db_config)

        logger.info(f"MongoDB connection string: {self.handle}")
        self.client = Client(handle=self.handle)
        self.db = self.client.attach_database(handle=self.handle)

        # Default collection names
        self.class_collection_name = "ontology_class_set"
        self.relation_collection_name = "ontology_relation_set"

        class_collection = self.db.create_collection(self.class_collection_name, recreate_if_exists=False)
        relation_collection = self.db.create_collection(self.relation_collection_name, recreate_if_exists=False)

        # Create indexes after bulk data loading is complete
        # try:
        #     class_collection.index("id", unique=False)
        #     relation_collection.index(["subject", "object", "predicate"], unique=False)
        #     logger.info("Successfully created indexes after bulk data loading")
        # except Exception as e:
        #     logger.warning(f"Error creating indexes: {str(e)}")

        logger.info(f"Connected to MongoDB: {self.db}")

    def upsert_ontology_data(
        self,
        ontology_classes: List[OntologyClass],
        ontology_relations: List[OntologyRelation],
        class_collection_name: str = None,
        relation_collection_name: str = None,
    ):
        """
        Upsert ontology terms, clear/re-populate ontology relations, handle obsolescence, and manage hierarchy changes.

        :param ontology_classes: A list of OntologyClass objects to upsert.
        :param ontology_relations: A list of OntologyRelation objects to upsert.
        :param class_collection_name: MongoDB collection name for ontology classes.
        :param relation_collection_name: MongoDB collection name for ontology relations.
        :return: A tuple of three reports: class updates, class insertions, and relation insertions.
        """
        # Use default collection names if not specified
        class_collection_name = class_collection_name or self.class_collection_name
        relation_collection_name = relation_collection_name or self.relation_collection_name

        # Get the collections (they should already exist and have indexes from initialization)
        class_collection = self.db.create_collection(class_collection_name, recreate_if_exists=False)
        relation_collection = self.db.create_collection(relation_collection_name, recreate_if_exists=False)

        # Step 1: Upsert ontology terms
        updates_report, insertions_report, insertions_report_relations = [], [], []
        ontology_fields = [field.name for field in fields(OntologyClass)]

        # Step 1.1: Handle obsolete terms
        obsolete_terms = [obj.id for obj in ontology_classes if getattr(obj, "is_obsolete", False)]
        _handle_obsolete_terms(obsolete_terms, class_collection, relation_collection)

        # Step 1.2: Upsert ontology classes
        for obj in ontology_classes:
            was_updated, report_row = _upsert_ontology_class(obj, class_collection, ontology_fields)
            if was_updated and report_row:
                updates_report.append(report_row)
            elif not was_updated and report_row:
                insertions_report.append(report_row)

        # Step 2: Upsert relations
        for relation in ontology_relations:
            report_data = _upsert_relation(relation, relation_collection)
            if report_data:
                insertions_report_relations.append(report_data)

        logging.info(
            f"Finished upserting ontology data: {len(ontology_classes)} classes, {len(ontology_relations)} relations."
        )
        return (
            Report("update", updates_report, ontology_fields),
            Report("insert", insertions_report, ontology_fields),
            Report("insert", insertions_report_relations, ["subject", "predicate", "object"]),
        )
