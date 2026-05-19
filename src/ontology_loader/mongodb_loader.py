"""Load and process ontology terms and relations into MongoDB."""

import logging
from dataclasses import asdict, fields
from typing import List, Optional

from linkml_runtime import SchemaView
from linkml_store import Client
from nmdc_schema.nmdc import OntologyClass, OntologyRelation
from pymongo import MongoClient as PyMongoClient
from pymongo import UpdateOne

from ontology_loader.mongo_db_config import MongoDBConfig
from ontology_loader.reporter import Report

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Default batch size for `bulk_write` calls. Larger batches are faster up to
# pymongo's server-side ~100k op limit, but cost memory per batch.
DEFAULT_BULK_BATCH_SIZE = 1000


VALID_REPORT_MODES = ("compared", "upsert", "off")


def _handle_obsolete_terms(obsolete_terms, class_collection, relation_collection):
    """
    Handle obsolete ontology terms by updating their status and removing relations.

    Uses the linkml_store collection API. The obsolete subset is always small
    enough that per-item work is cheap; this path is unchanged from the
    pre-bulk-write implementation.
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


def _class_to_doc(obj):
    """Convert an OntologyClass dataclass to a dict with required boolean defaults."""
    doc = asdict(obj)
    if doc.get("is_root") is None:
        doc["is_root"] = False
    if doc.get("is_obsolete") is None:
        doc["is_obsolete"] = False
    return doc


def _normalize_relation(relation):
    """Normalize an OntologyRelation (dataclass or dict) to a dict; return None if invalid."""
    if type(relation) is OntologyRelation:
        relation = asdict(relation)
    if not relation.get("subject") or not relation.get("predicate") or not relation.get("object"):
        logging.warning(f"Skipping invalid relation: {relation}")
        return None
    return relation


def _bulk_upsert_classes(
    py_collection,
    ontology_classes,
    ontology_fields,
    report_mode,
    batch_size=DEFAULT_BULK_BATCH_SIZE,
):
    """
    Bulk-upsert OntologyClass documents.

    Returns (insertions_report, updates_report) — both lists of report rows.

    `report_mode` semantics:
        - 'compared': one pre-read per batch (find with $in), only write+report
          docs that are new or whose fields actually changed. Preserves the
          pre-bulk-write behavior modulo per-doc → per-batch read.
        - 'upsert': skip the pre-read; bulk_write every doc. New docs
          (BulkWriteResult.upserted_ids) go to inserts, the rest to updates.
        - 'off': bulk_write every doc; do not track reports.
    """
    insertions_report: list = []
    updates_report: list = []

    for batch_start in range(0, len(ontology_classes), batch_size):
        batch = ontology_classes[batch_start : batch_start + batch_size]
        if not batch:
            continue

        if report_mode == "compared":
            ids = [obj.id for obj in batch]
            existing = {doc["id"]: doc for doc in py_collection.find({"id": {"$in": ids}})}
            ops = []
            classification: list = []
            for obj in batch:
                doc = _class_to_doc(obj)
                row = [obj.id] + [getattr(obj, field, "") for field in ontology_fields]
                existing_doc = existing.get(obj.id)
                if existing_doc is None:
                    ops.append(UpdateOne({"id": obj.id}, {"$set": doc}, upsert=True))
                    classification.append(("insert", row))
                else:
                    changed = any(doc.get(f) != existing_doc.get(f) for f in ontology_fields)
                    if changed:
                        ops.append(UpdateOne({"id": obj.id}, {"$set": doc}, upsert=True))
                        classification.append(("update", row))
            if ops:
                py_collection.bulk_write(ops, ordered=False)
                for kind, row in classification:
                    (insertions_report if kind == "insert" else updates_report).append(row)

        elif report_mode == "upsert":
            ops = []
            rows = []
            for obj in batch:
                ops.append(UpdateOne({"id": obj.id}, {"$set": _class_to_doc(obj)}, upsert=True))
                rows.append([obj.id] + [getattr(obj, field, "") for field in ontology_fields])
            result = py_collection.bulk_write(ops, ordered=False)
            upserted_idx = set(result.upserted_ids.keys()) if result.upserted_ids else set()
            for i, row in enumerate(rows):
                (insertions_report if i in upserted_idx else updates_report).append(row)

        else:  # "off"
            ops = [UpdateOne({"id": obj.id}, {"$set": _class_to_doc(obj)}, upsert=True) for obj in batch]
            py_collection.bulk_write(ops, ordered=False)

    return insertions_report, updates_report


def _bulk_upsert_relations(
    py_collection,
    ontology_relations,
    report_mode,
    batch_size=DEFAULT_BULK_BATCH_SIZE,
):
    """
    Bulk-upsert OntologyRelation documents.

    Returns the insertions report (newly inserted relations only, or empty
    when `report_mode='off'`). Updates of existing relations are not reported
    here — relations are immutable in the loader's current model.
    """
    insertions_report: list = []
    track = report_mode in ("compared", "upsert")

    for batch_start in range(0, len(ontology_relations), batch_size):
        batch = ontology_relations[batch_start : batch_start + batch_size]
        ops = []
        rows = []
        for relation in batch:
            d = _normalize_relation(relation)
            if d is None:
                continue
            ops.append(
                UpdateOne(
                    {"subject": d["subject"], "predicate": d["predicate"], "object": d["object"]},
                    {"$set": d},
                    upsert=True,
                )
            )
            if track:
                rows.append([d["subject"], d["predicate"], d["object"]])
        if not ops:
            continue
        result = py_collection.bulk_write(ops, ordered=False)
        if track:
            upserted_idx = set(result.upserted_ids.keys()) if result.upserted_ids else set()
            for i, row in enumerate(rows):
                if i in upserted_idx:
                    insertions_report.append(row)

    return insertions_report


def get_mongo_connection_string(db_config) -> str:
    """Generate a formatted MongoDB connection string from a db_config object."""
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
        """Initialize MongoDB using LinkML-store's client, prioritizing env vars."""
        self.db_config = MongoDBConfig()
        self.schema_view = schema_view

        if mongo_client:
            if not db_name:
                raise ValueError("Database name (db_name) is required when providing an existing MongoDB client")
            self.db_config.db_name = db_name
            self.db_config.set_existing_client(mongo_client)

        # The raw pymongo Database used by the bulk-write hot path. Created
        # lazily when `_py_db` is first accessed unless an existing client was
        # provided, in which case we set it up-front. Tests can inject a mock
        # by assigning `loader._py_db = mock`.
        self._py_db_value = None
        self._py_client: Optional[PyMongoClient] = None

        if self.db_config.has_existing_client():
            logger.info("Using existing MongoDB client")
            existing_client = self.db_config.existing_client
            host_string = existing_client.address[0]
            port = existing_client.address[1]
            self.handle = f"mongodb://{host_string}:{port}/{self.db_config.db_name}"
            logger.info(f"Using existing client connection: {self.handle}")
            self.client = Client(handle=self.handle)
            db = self.client.attach_database(handle=self.handle)
            mongodb_db = db
            mongodb_db._native_client = self.db_config.existing_client
            mongodb_db._native_db = self.db_config.existing_client[self.db_config.db_name]
            self.db = db
            self._py_db_value = self.db_config.existing_client[self.db_config.db_name]
        else:
            self.handle = get_mongo_connection_string(self.db_config)
            logger.info(f"MongoDB connection string: {self.handle}")
            self.client = Client(handle=self.handle)
            self.db = self.client.attach_database(handle=self.handle)

        logger.info(f"Connected to MongoDB: {self.db}")

    @property
    def _py_db(self):
        """
        Raw pymongo Database for hot-path bulk operations (lazily constructed).

        We bypass linkml_store for the class/relation bulk-upsert path because
        ``linkml_store.api.stores.mongodb.mongodb_collection.upsert`` iterates
        per-item with ``find_one`` followed by ``update_one`` or ``insert_one``,
        costing 2N round trips for N documents. That dominates the NCBITaxon
        load (~2.7M classes + ~55M relations would take hours). Upstream issue
        tracking a possible bulk-write path: https://github.com/linkml/linkml-store/issues/77 .
        If linkml_store later grows a ``bulk_write``-backed upsert, the bypass
        becomes a candidate for removal.
        """
        if self._py_db_value is None:
            self._py_client = PyMongoClient(
                host=self.db_config.db_host,
                port=self.db_config.db_port,
                username=self.db_config.db_user,
                password=self.db_config.db_password,
                authSource="admin",
                directConnection=True,
            )
            self._py_db_value = self._py_client[self.db_config.db_name]
        return self._py_db_value

    @_py_db.setter
    def _py_db(self, value):
        """Allow tests (or callers reusing an external pymongo client) to inject a Database."""
        self._py_db_value = value

    def upsert_ontology_data(
        self,
        ontology_classes: List[OntologyClass],
        ontology_relations: List[OntologyRelation],
        class_collection_name: str = "ontology_class_set",
        relation_collection_name: str = "ontology_relation_set",
        report_mode: str = "compared",
    ):
        """
        Bulk-upsert ontology classes and relations.

        :param report_mode: One of 'compared' (default; preserves no-change skip
            via batched pre-reads), 'upsert' (no pre-read, max throughput),
            or 'off' (no report tracking; lowest memory).
        :return: tuple of three `Report` objects (class updates, class inserts, relation inserts).
        """
        if report_mode not in VALID_REPORT_MODES:
            raise ValueError(f"Unknown report_mode {report_mode!r}; expected one of {VALID_REPORT_MODES}")

        # Schema-aware setup via linkml_store; also ensures indexes exist.
        class_collection = self.db.create_collection(class_collection_name, recreate_if_exists=False)
        relation_collection = self.db.create_collection(relation_collection_name, recreate_if_exists=False)
        class_collection.index("id", unique=False, name="ontology_class_index")
        relation_collection.index(["subject", "predicate", "object"], unique=False, name="ontology_relation_index")

        ontology_fields = [field.name for field in fields(OntologyClass)]

        # Obsolete subset is small; keep the pre-existing linkml_store path.
        obsolete_terms = [obj.id for obj in ontology_classes if getattr(obj, "is_obsolete", False)]
        _handle_obsolete_terms(obsolete_terms, class_collection, relation_collection)

        # Hot-path bulk upserts via raw pymongo collections.
        py_class = self._py_db[class_collection_name]
        py_relation = self._py_db[relation_collection_name]

        insertions_report, updates_report = _bulk_upsert_classes(
            py_class, ontology_classes, ontology_fields, report_mode
        )
        insertions_report_relations = _bulk_upsert_relations(py_relation, ontology_relations, report_mode)

        logging.info(
            f"Finished upserting ontology data: {len(ontology_classes)} classes, "
            f"{len(ontology_relations)} relations (report_mode={report_mode!r})."
        )
        return (
            Report("update", updates_report, ontology_fields),
            Report("insert", insertions_report, ontology_fields),
            Report("insert", insertions_report_relations, ["subject", "predicate", "object"]),
        )
