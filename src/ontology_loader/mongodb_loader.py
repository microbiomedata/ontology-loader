from linkml_store import Client
from linkml_runtime import SchemaView
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

    import uuid
    import logging

    def upsert_ontology_classes(self, ontology_classes):
        """
        Upsert each OntologyClass object into the 'ontology_class_set' collection and generate TSV reports.

        :param ontology_classes: A list of OntologyClass objects to upsert.
        """
        collection = self.db.create_collection("ontology_class_set", recreate_if_exists=False)

        if not ontology_classes:
            logging.info("No OntologyClass objects to upsert.")
            return

        updates_report = []
        insertions_report = []

        for obj in ontology_classes:
            filter_criteria = {"id": obj["id"]}

            # Query the collection using the LinkML-store Client wrapper
            query_result = collection.find(filter_criteria)

            # Extract the first document from QueryResult.rows
            existing_doc = query_result.rows[0] if query_result.num_rows > 0 else None

            if existing_doc:
                # Check for actual changes
                updated_fields = {key: obj[key] for key in ["name", "definition", "alternate_identifiers"]
                                  if key in obj and obj[key] != existing_doc.get(key)}

                if updated_fields:
                    collection.upsert([obj], filter_fields=["id"],
                                      update_fields=["name", "definition", "alternate_identifiers"])
                    logging.debug(f"Updated existing OntologyClass (id={obj['id']}): {updated_fields}")

                    # Add to updates report
                    updates_report.append([obj["id"], obj.get("name", ""), ", ".join(updated_fields.keys())])
                else:
                    logging.debug(f"No changes detected for OntologyClass (id={obj['id']}). Skipping update.")
            else:
                # New insert
                collection.upsert([obj], filter_fields=["id"],
                                  update_fields=["name", "definition", "alternate_identifiers"])
                logging.debug(f"Inserted new OntologyClass (id={obj['id']}).")

                # Add to insertions report
                insertions_report.append([obj["id"], obj.get("name", ""), obj.get("definition", "")])

        logging.info(f"Finished upserting {len(ontology_classes)} OntologyClass objects into MongoDB.")

        # Write updates report to TSV
        updates_report_path = Path("ontology_updates.tsv")
        with updates_report_path.open(mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["id", "name", "updated_fields"])  # Header
            writer.writerows(updates_report)

        # Write insertions report to TSV
        insertions_report_path = Path("ontology_insertions.tsv")
        with insertions_report_path.open(mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["id", "name", "definition"])  # Header
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