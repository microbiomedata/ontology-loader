"""Test MongoDB client connections from pymongo and linkml-store."""

import os

from linkml_store.api.client import Client
from pymongo import MongoClient
from sqlalchemy.testing import skip_if

# MongoDB Connection Parameters
MONGO_HOST = "localhost"
MONGO_PORT = 27018
MONGO_DB = "nmdc"  # Database where you want to insert/update
MONGO_USER = "admin"
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "")
AUTH_DB = "admin"  # Authentication database


@skip_if(os.getenv("MONGO_PASSWORD") is None, reason="Skipping test: MONGO_PASSWORD is not set")
def test_mongo_client():
    """Test MongoDB client connection from pymongo."""
    # Initialize the MongoDB client
    client = MongoClient(
        host=f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={AUTH_DB}"
    )

    # Select the target database for insert/update operations
    db = client[MONGO_DB]

    print("Connected to MongoDB:", db)
    # Verify connection (List collections)
    print("Collections in database:", db.list_collection_names())

    # Example Insert
    collection = db["ontology_class_set"]
    collection.insert_one({"id": "test_id", "name": "Test Ontology Class"})

    # Fetch inserted data
    result = collection.find_one({"id": "test_id"})
    print("Inserted Record:", result)


@skip_if(os.getenv("MONGO_PASSWORD") is None, reason="Skipping test: MONGO_PASSWORD is not set")
def test_linkmlstore_client():
    """Test the MongoDB client connection from linkml-store."""
    client = Client(
        handle=f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={AUTH_DB}"
    )
    db = client.attach_database(
        handle=f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={AUTH_DB}",
        alias=MONGO_DB,
    )
    print("Connected to MongoDB:", db.metadata)
    # Verify connection (List collections)
    print("Collections in database:", db.list_collection_names())
