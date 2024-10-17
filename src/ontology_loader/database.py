from linkml_store import Client

class MongoDB:
    def __init__(self, db_url: str, db_name: str):
        self.client = Client()
        self.db_url = db_url
        self.db_name = db_name
        self.db = None

    def connect(self):
        self.db = self.client.attach_database(self.db_url, self.db_name)
        self.db.set_schema_view("")  # You can specify your schema view here
        print(f"Connected to database: {self.db.handle}")

    def insert_ontology_classes(self, ontology_classes):
        if not self.db:
            raise ValueError("Database not connected. Call connect() first.")
        collection = self.db.create_collection("OntologyClass", recreate_if_exists=True)
        collection.insert(ontology_classes)
        print(f"Inserted {len(ontology_classes)} ontology classes into the collection.")

# Usage example:
db_url = "mongodb://localhost:27017"
db_name = "test"