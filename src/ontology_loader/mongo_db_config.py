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
            cls._instance.db_user = os.getenv("MONGO_USERNAME", "admin")
            cls._instance.db_password = os.getenv("MONGO_PASSWORD", "")
            cls._instance.db_host = os.getenv("MONGO_HOST", "localhost")
            cls._instance.db_port = int(os.getenv("MONGO_PORT", 27018))
            cls._instance.replica_set = os.getenv("MONGO_REPLICA_SET", "")
            # Build connection parameters based on whether replica set is defined
            if cls._instance.replica_set:
                # Replica set parameters
                cls._instance.connection_params = [
                    "authSource=admin",
                    "replicaSet=" + cls._instance.replica_set,
                    # Connect directly to the server, don't attempt replica set discovery
                    # which would fail due to port forwarding
                    "directConnection=true",
                    # With directConnection=true, we're using a direct connection
                    "connect=true",
                    # Use local threshold for best performance
                    "localThresholdMS=1000",
                    # Set connect timeout shorter to fail faster
                    "connectTimeoutMS=5000",
                    # Don't retry writes in this scenario
                    "retryWrites=false",
                ]
            else:
                # Standalone parameters
                cls._instance.connection_params = [
                    "authSource=admin",
                    "directConnection=true",
                    "connectTimeoutMS=5000",
                ]
            cls._instance.auth_params = "&".join(cls._instance.connection_params)
        return cls._instance
