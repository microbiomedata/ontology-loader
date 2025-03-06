"""Singleton class to store default parameters accessed from client environment or sensible defaults."""

import os


class MongoDBConfig:

    """Singleton class to store default parameters accessed from client environment or sensible defaults."""

    _instance = None

    def __new__(cls):
        """Create a new instance of MongoDBConfig if it does not exist."""
        if cls._instance is None:
            cls._instance = super(MongoDBConfig, cls).__new__(cls)
            cls._instance.db_name = os.getenv("MONGO_DB", "nmdc")
            cls._instance.db_user = os.getenv("MONGO_USER", "admin")
            cls._instance.db_password = os.getenv("MONGO_PASSWORD", "")
            cls._instance.db_host = os.getenv("MONGO_HOST", "localhost")
            cls._instance.db_port = int(os.getenv("MONGO_PORT", 27018))
        return cls._instance
