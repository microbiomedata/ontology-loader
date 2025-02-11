"""Load and process ontology terms and relations into MongoDB."""

import csv
import logging
from dataclasses import asdict, fields
from pathlib import Path
from typing import List

from linkml_runtime import SchemaView
from linkml_store import Client
from nmdc_schema.nmdc import OntologyClass

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class MongoDBLoader:

    """MongoDB Loader class to upsert OntologyClass objects and insert OntologyRelation objects into MongoDB."""

    def __init__(self, schema_view: SchemaView):
        """
        Initialize MongoDB using LinkML-store's client.

        :param schema_view: LinkML SchemaView for ontology

        """
        self.client = Client()
        self.db = self.client.attach_database("mongodb", alias="nmdc", schema_view=schema_view)

    def upsert_ontology_classes(
        self, ontology_classes: List[OntologyClass], collection_name: str = "ontology_class_set"
    ):
        """
        Upsert each OntologyClass object into the 'ontology_class_set' collection and reports based on detected updates.

        :param ontology_classes: A list of OntologyClass objects to upsert.
        :param collection_name: The name of the MongoDB collection to upsert into.

        """
        collection = self.db.create_collection(collection_name, recreate_if_exists=False)
        # Ensure an index on the 'id' field for efficient lookups
        collection.index("id", unique=False)

        if not ontology_classes:
            logging.info("No OntologyClass objects to upsert.")
            return

        updates_report = []
        insertions_report = []

        # Extract OntologyClass field names dynamically
        ontology_fields = [field.name for field in fields(OntologyClass)]

        for obj in ontology_classes:
            filter_criteria = {"id": obj.id}
            query_result = collection.find(filter_criteria)
            existing_doc = query_result.rows[0] if query_result.num_rows > 0 else None

            if existing_doc:
                # Identify fields that have changed
                updated_fields = {
                    key: getattr(obj, key) for key in ontology_fields if getattr(obj, key) != existing_doc.get(key)
                }

                if updated_fields:
                    collection.upsert([asdict(obj)], filter_fields=["id"], update_fields=list(updated_fields.keys()))
                    logging.debug(f"Updated existing OntologyClass (id={obj.id}): {updated_fields}")

                    # Add to updates report
                    updates_report.append([obj.id] + [getattr(obj, field, "") for field in ontology_fields])
                else:
                    logging.debug(f"No changes detected for OntologyClass (id={obj.id}). Skipping update.")
            else:
                # New insert
                collection.upsert([asdict(obj)], filter_fields=["id"], update_fields=ontology_fields)
                logging.debug(f"Inserted new OntologyClass (id={obj.id}).")

                # Add to insertions report
                insertions_report.append([obj.id] + [getattr(obj, field, "") for field in ontology_fields])

        logging.info(f"Finished upserting {len(ontology_classes)} OntologyClass objects into MongoDB.")

        # Reporting changes
        updates_report_path = Path("ontology_updates.tsv")
        with updates_report_path.open(mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["id"] + ontology_fields)  # Dynamic header
            writer.writerows(updates_report)

        insertions_report_path = Path("ontology_insertions.tsv")
        with insertions_report_path.open(mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["id"] + ontology_fields)  # Dynamic header
            writer.writerows(insertions_report)

        logging.info(f"Reports generated: {updates_report_path}, {insertions_report_path}")

    def insert_ontology_relations(self, ontology_relations, collection_name: str = "ontology_relation_set"):
        """
        Insert each OntologyClass object into the 'ontology_class_set' collection.

        :param ontology_relations: A list of OntologyRelation objects to insert
        :param collection_name: The name of the MongoDB collection to insert into.

        """
        collection = self.db.create_collection(collection_name, recreate_if_exists=False)

        if ontology_relations:
            for relation in ontology_relations:
                collection.insert(asdict(relation))
            logging.info(f"Inserted {len(ontology_relations)} OntologyRelations objects into MongoDB.")
        else:
            logging.info("No OntologyRelation objects to insert.")
