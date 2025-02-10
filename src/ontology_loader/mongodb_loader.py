from dataclasses import asdict, fields
from typing import List

from linkml_store import Client
from linkml_runtime import SchemaView
from nmdc_schema.nmdc import OntologyClass, OntologyRelation
import logging
import csv
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class MongoDBLoader:
    def __init__(self, schema_view: SchemaView):
        """
        Initialize MongoDB using LinkML-store's client.

        :param schema_view: LinkML SchemaView for ontology
        """
        self.client = Client()
        self.db = self.client.attach_database("mongodb", alias="nmdc", schema_view=schema_view)

    def upsert_ontology_classes(self, ontology_classes: List["OntologyClass"]):
        """
        Upsert each OntologyClass object into the 'ontology_class_set' collection
        and generate dynamic TSV reports based on detected updates.

        :param ontology_classes: A list of OntologyClass objects to upsert.
        """
        collection = self.db.create_collection("ontology_class_set", recreate_if_exists=False)

        if not ontology_classes:
            logging.info("No OntologyClass objects to upsert.")
            return

        updates_report = []
        insertions_report = []

        # Dynamically extract OntologyClass field names
        ontology_fields = [field.name for field in fields(ontology_classes[0])]

        for obj in ontology_classes:

            obj_dict = asdict(obj)  # Convert the dataclass object to a dictionary
            filter_criteria = {"id": obj_dict["id"]}

            # Query the collection using LinkML-store Client wrapper
            query_result = collection.find(filter_criteria)

            # Extract the first document from QueryResult.rows
            existing_doc = query_result.rows[0] if query_result.num_rows > 0 else None

            if existing_doc:
                # Identify fields that have changed
                updated_fields = {
                    key: obj_dict[key] for key in ontology_fields
                    if key in obj_dict and obj_dict[key] != existing_doc.get(key)
                }

                if updated_fields:
                    collection.upsert([obj_dict], filter_fields=["id"], update_fields=list(updated_fields.keys()))
                    logging.debug(f"Updated existing OntologyClass (id={obj.id}): {updated_fields}")

                    # Add to updates report
                    updates_report.append([obj.id] + [obj_dict.get(field, "") for field in ontology_fields])
                else:
                    logging.debug(f"No changes detected for OntologyClass (id={obj.id}). Skipping update.")
            else:
                # New insert
                collection.upsert([obj_dict], filter_fields=["id"], update_fields=list(obj_dict.keys()))
                logging.debug(f"Inserted new OntologyClass (id={obj.id}).")

                # Add to insertions report
                insertions_report.append([obj.id] + [obj_dict.get(field, "") for field in ontology_fields])

        logging.info(f"Finished upserting {len(ontology_classes)} OntologyClass objects into MongoDB.")

        # Write updates report dynamically
        updates_report_path = Path("ontology_updates.tsv")
        with updates_report_path.open(mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["id"] + ontology_fields)  # Dynamic header
            writer.writerows(updates_report)

        # Write insertions report dynamically
        insertions_report_path = Path("ontology_insertions.tsv")
        with insertions_report_path.open(mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["id"] + ontology_fields)  # Dynamic header
            writer.writerows(insertions_report)

        logging.info(f"Reports generated: {updates_report_path}, {insertions_report_path}")

    def insert_ontology_relations(self, ontology_relations):
        """
        Insert each OntologyClass object into the 'ontology_class_set' collection.

        :param ontology_relations: A list of OntologyClass objects to insert
        """
        collection = self.db.create_collection("ontology_relation_set", recreate_if_exists=False)

        if ontology_relations:
            collection.insert(ontology_relations)
            logging.info(f"Inserted {len(ontology_relations)} OntologyRelations objects into MongoDB.")
        else:
            logging.info("No OntologyRelation objects to insert.")