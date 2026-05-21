"""Test MongoDB client connections from pymongo and linkml-store."""

import os

import pytest
from linkml_store.api.client import Client
from pymongo import MongoClient

# MongoDB connection parameters — read from environment to match the rest of
# the test suite (e.g. tests/test_ontology_class_null_values.py:12-16) and
# the loader itself (src/ontology_loader/mongo_db_config.py:23-27). See #22.
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB = os.getenv("MONGO_DBNAME", "nmdc")
MONGO_USER = os.getenv("MONGO_USERNAME", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "")
AUTH_DB = "admin"  # Authentication database


@pytest.mark.skipif(os.getenv("MONGO_PASSWORD") is None, reason="Skipping test: MONGO_PASSWORD is not set")
def test_mongo_client():
    """Test MongoDB client connection from pymongo."""
    # Initialize the MongoDB client
    print(f"Connecting to MongoDB at {MONGO_HOST}:{MONGO_PORT} with user {MONGO_USER}")
    client = MongoClient(
        host=f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={AUTH_DB}&directConnection=true"
    )

    # Select the target database for insert/update operations
    db = client[MONGO_DB]

    # Example Insert
    collection = db["ontology_class_set"]

    collection.insert_one({"id": "test_id", "name": "Test Ontology Class", "is_root": False, "is_obsolete": False})

    # Fetch inserted data
    result = collection.find_one({"id": "test_id"})
    assert result is not None, "Failed to insert data into MongoDB"
    collection.delete_many({"id": "test_id"})
    assert collection.find_one({"id": "test_id"}) is None, "Failed to delete test data from MongoDB"


@pytest.mark.skipif(os.getenv("MONGO_PASSWORD") is None, reason="Skipping test: MONGO_PASSWORD is not set")
def test_linkmlstore_client():
    """Test the MongoDB client connection from linkml-store."""
    client = Client(
        handle=f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={AUTH_DB}&directConnection=true"
    )
    db = client.attach_database(
        handle=f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={AUTH_DB}&directConnection=true",
        alias=MONGO_DB,
    )
    assert db is not None, "Failed to connect to MongoDB using linkml-store client"
