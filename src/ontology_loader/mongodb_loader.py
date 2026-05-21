"""Load and process ontology terms and relations into MongoDB."""

import logging
from dataclasses import asdict, fields
from typing import List, Optional

from linkml_runtime import SchemaView
from linkml_store import Client
from nmdc_schema.nmdc import OntologyClass, OntologyRelation
from pymongo import MongoClient
from tqdm import tqdm

from ontology_loader.mongo_db_config import MongoDBConfig
from ontology_loader.reporter import Report

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Batch size for the fast-initial path's pymongo.insert_many calls. Tuned to keep memory
# bounded for >50M relation docs while still amortizing the round-trip cost.
_FAST_INITIAL_BATCH_SIZE = 5000


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
        # Ensure boolean fields are explicitly set to avoid null values in MongoDB
        doc = asdict(obj)
        if doc.get("is_root") is None:
            doc["is_root"] = False
        if doc.get("is_obsolete") is None:
            doc["is_obsolete"] = False

        collection.upsert([doc], filter_fields=["id"], update_fields=ontology_fields)
        logging.debug(f"Inserted OntologyClass (id={obj.id}).")
        return False, report_row

    return None, None


def get_mongo_connection_string(db_config) -> str:
    """
    Generate a formatted MongoDB connection string from a db_config object.

    Args:
    ----
        db_config: An object containing MongoDB connection parameters.

    Returns:
    -------
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

    def __init__(self, schema_view: Optional[SchemaView] = None, mongo_client=None, db_name: Optional[str] = None):
        """
        Initialize MongoDB using LinkML-store's client, prioritizing environment variables for connection details.

        :param schema_view: LinkML SchemaView for ontology
        :param mongo_client: Optional existing MongoDB client to use instead of creating a new connection
        :param db_name: Required database name when using an existing client
        """
        # Get database config from environment variables or fallback to MongoDBConfig defaults
        self.db_config = MongoDBConfig()
        self.schema_view = schema_view

        # If a MongoDB client was provided
        if mongo_client:
            # Database name is required when passing a client
            if not db_name:
                raise ValueError("Database name (db_name) is required when providing an existing MongoDB client")

            # Set the database name and client in config
            self.db_config.db_name = db_name
            self.db_config.set_existing_client(mongo_client)

        # Set up the database connection
        if self.db_config.has_existing_client():
            # Use the existing MongoDB client
            logger.info("Using existing MongoDB client")

            # Extract the connection details from the existing client
            existing_client = self.db_config.existing_client
            # The host_string should contain the actual host and port
            host_string = existing_client.address[0]
            port = existing_client.address[1]

            # Create a handle using the actual connection details and the provided db_name
            self.handle = f"mongodb://{host_string}:{port}/{self.db_config.db_name}"
            logger.info(f"Using existing client connection: {self.handle}")

            # Create a Client using the handle
            self.client = Client(handle=self.handle)

            # Access the mongodb database implementation
            db = self.client.attach_database(handle=self.handle)

            # Replace the native client with our existing one
            # This will make all MongoDB operations use our existing client
            mongodb_db = db
            mongodb_db._native_client = self.db_config.existing_client
            mongodb_db._native_db = self.db_config.existing_client[self.db_config.db_name]

            self.db = db
            # Raw pymongo handle for the fast-initial write path: reuse the caller's client.
            self._py_client = self.db_config.existing_client
        else:
            # Create a new connection using the connection string
            self.handle = get_mongo_connection_string(self.db_config)
            logger.info(
                f"Connecting to mongodb://{self.db_config.db_host}:{self.db_config.db_port}/{self.db_config.db_name}"
            )
            self.client = Client(handle=self.handle)
            self.db = self.client.attach_database(handle=self.handle)
            # Raw pymongo client is constructed lazily — see the `_py_db` property below.
            # The meticulous path never needs it, and instantiating MongoClient(handle) eagerly
            # blows up under mock-only test runs where MONGO_PASSWORD is unset.
            self._py_client = None

        logger.info(f"Connected to MongoDB: {self.db}")

    @property
    def _py_db(self):
        """Lazily-built raw pymongo database handle for the fast-initial write path."""
        if self._py_client is None:
            self._py_client = MongoClient(self.handle)
        return self._py_client[self.db_config.db_name]

    def upsert_ontology_data(
        self,
        ontology_classes: List[OntologyClass],
        ontology_relations: List[OntologyRelation],
        class_collection_name: str = "ontology_class_set",
        relation_collection_name: str = "ontology_relation_set",
    ):
        """
        Meticulous-mode load: upsert via linkml-store per-item.

        Upsert ontology terms and relations incrementally, handle obsolescence, and manage hierarchy changes.
        Relations are upserted individually; the collection is not cleared before loading, allowing multiple
        ontologies to be loaded sequentially. This is the path that produces the TSV reports and matches
        Sierra's 0.2.x behavior. For maximum-throughput first-time loads, see
        :meth:`insert_ontology_data_fast_initial`.

        :param ontology_classes: A list of OntologyClass objects to upsert.
        :param ontology_relations: A list of OntologyRelation objects to upsert.
        :param class_collection_name: MongoDB collection name for ontology classes.
        :param relation_collection_name: MongoDB collection name for ontology relations.
        :return: A tuple of three reports: class updates, class insertions, and relation insertions.
        """
        # Use default collection names if not specified

        # Get the collections (they should already exist and have indexes from initialization)
        class_collection = self.db.create_collection(class_collection_name, recreate_if_exists=False)
        relation_collection = self.db.create_collection(relation_collection_name, recreate_if_exists=False)

        class_collection.index("id", unique=False, name="ontology_class_index")
        relation_collection.index(["subject", "predicate", "object"], unique=False, name="ontology_relation_index")

        # Step 1: Upsert ontology terms
        updates_report, insertions_report, insertions_report_relations = [], [], []
        ontology_fields = [field.name for field in fields(OntologyClass)]

        # Step 1.1: Handle obsolete terms
        obsolete_terms = [obj.id for obj in ontology_classes if getattr(obj, "is_obsolete", False)]
        _handle_obsolete_terms(obsolete_terms, class_collection, relation_collection)

        # Step 1.2: Upsert ontology classes
        for obj in tqdm(ontology_classes, desc="Upserting ontology classes", unit="class"):
            was_updated, report_row = _upsert_ontology_class(obj, class_collection, ontology_fields)
            if was_updated and report_row:
                updates_report.append(report_row)
            elif not was_updated and report_row:
                insertions_report.append(report_row)

        # Step 2: Upsert relations
        for relation in tqdm(ontology_relations, desc="Upserting ontology relations", unit="rel"):
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

    def insert_ontology_data_fast_initial(
        self,
        ontology_classes: List[OntologyClass],
        ontology_relations: List[OntologyRelation],
        class_collection_name: str = "ontology_class_set",
        relation_collection_name: str = "ontology_relation_set",
        batch_size: int = _FAST_INITIAL_BATCH_SIZE,
    ):
        """
        Fast-initial mode: raw pymongo ``insert_many`` with no upsert and no reporting.

        Intended for first-time installs of large ontologies (e.g. NCBITaxon, 2.7M classes + 54.7M relations)
        where pre-read + upsert overhead dominates wall-clock. Caller is asserting the target collections are
        either empty or that duplicate-key errors are acceptable — this method does not pre-clear, does not
        deduplicate, and does not produce TSV reports.

        :param ontology_classes: A list of OntologyClass objects to insert.
        :param ontology_relations: A list of OntologyRelation objects to insert.
        :param class_collection_name: MongoDB collection name for ontology classes.
        :param relation_collection_name: MongoDB collection name for ontology relations.
        :param batch_size: Documents per ``insert_many`` call. Default 5000.
        """
        py_class = self._py_db[class_collection_name]
        py_relation = self._py_db[relation_collection_name]

        class_count = _bulk_insert_iter(
            py_class,
            (_class_to_doc(obj) for obj in ontology_classes),
            batch_size=batch_size,
            label="classes",
        )

        relation_count = _bulk_insert_iter(
            py_relation,
            (doc for rel in ontology_relations if (doc := _relation_to_doc(rel)) is not None),
            batch_size=batch_size,
            label="relations",
        )

        logging.info(f"Finished fast-initial insert: {class_count} classes, {relation_count} relations.")


def _class_to_doc(obj):
    """
    Convert an OntologyClass to a Mongo document, guaranteeing non-null is_root / is_obsolete.

    Matches the same shape ``_upsert_ontology_class`` writes in the meticulous path.
    """
    doc = asdict(obj)
    if doc.get("is_root") is None:
        doc["is_root"] = False
    if doc.get("is_obsolete") is None:
        doc["is_obsolete"] = False
    return doc


def _relation_to_doc(relation):
    """
    Convert an OntologyRelation to a Mongo document, dropping malformed ones.

    Matches the validity check in ``_upsert_relation``.
    """
    if type(relation) is OntologyRelation:
        relation = asdict(relation)
    if not relation.get("subject") or not relation.get("predicate") or not relation.get("object"):
        logging.warning(f"Skipping invalid relation: {relation}")
        return None
    return relation


def _bulk_insert_iter(py_collection, docs_iter, batch_size, label):
    """Stream ``insert_many(batch, ordered=False)`` from an iterator; returns the total inserted."""
    total = 0
    batch: list = []
    for doc in docs_iter:
        batch.append(doc)
        if len(batch) >= batch_size:
            py_collection.insert_many(batch, ordered=False)
            total += len(batch)
            batch = []
    if batch:
        py_collection.insert_many(batch, ordered=False)
        total += len(batch)
    if total == 0:
        logging.info(f"No {label} to insert.")
    else:
        logging.info(f"Inserted {total} {label} via pymongo.insert_many.")
    return total
