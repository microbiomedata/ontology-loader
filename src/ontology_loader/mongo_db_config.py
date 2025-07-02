"""Singleton class to store default parameters accessed from client environment or sensible defaults."""

import logging
import os

logger = logging.getLogger(__name__)


class MongoDBConfig:

    """MongoDB configuration class - minimal by default, only reads environment variables when no client is provided."""

    def __init__(self, mongo_client=None, db_name=None):
        """Initialize the MongoDBConfig instance."""
        self.existing_client = mongo_client
        
        # Only read environment variables if no client is provided
        if not mongo_client:
            self.db_name = os.getenv("MONGO_DB", "nmdc")
            self.db_user = os.getenv("MONGO_USERNAME", "admin")
            self.db_password = os.getenv("MONGO_PASSWORD", "")
            self.db_host = os.getenv("MONGO_HOST", "localhost")
            # Defensive parsing of MONGO_PORT - handle various formats
            mongo_port_env = os.getenv("MONGO_PORT", "27022")
            try:
                # Try to parse as integer first
                self.db_port = int(mongo_port_env)
            except ValueError:
                # If that fails, try to extract port from URL formats
                if ":" in mongo_port_env:
                    # Handle formats like "tcp://host:port", "mongodb://host:port", etc.
                    try:
                        self.db_port = int(mongo_port_env.split(":")[-1])
                        logger.info(f"Extracted port {self.db_port} from {mongo_port_env}")
                    except (ValueError, IndexError):
                        logger.warning(f"Could not parse port from {mongo_port_env}, using default 27022")
                        self.db_port = 27022
                else:
                    logger.warning(f"Could not parse port from {mongo_port_env}, using default 27022")
                    self.db_port = 27022
            self.replica_set = os.getenv("MONGO_REPLICA_SET", "")
            # Build connection parameters based on whether replica set is defined
            if self.replica_set:
                # Replica set parameters
                self.connection_params = [
                    "authSource=admin",
                    "replicaSet=" + self.replica_set,
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
                self.connection_params = [
                    "authSource=admin",
                    "directConnection=true",
                    "connectTimeoutMS=5000",
                ]
            self.auth_params = "&".join(self.connection_params)
        else:
            # Client provided - use the provided db_name
            self.db_name = db_name

    def set_existing_client(self, client):
        """
        Set an existing MongoDB client instance.

        When set, this client will be used instead of creating a new connection.

        Args:
            client: An existing pymongo.MongoClient instance

        """
        self.existing_client = client

    def has_existing_client(self):
        """
        Check if an existing MongoDB client has been set.

        Returns:
            bool: True if an existing client is available, False otherwise

        """
        return self.existing_client is not None
