## ontology_loader

Suite of tools to configure and load an ontology from the OboFoundary into the data object for OntologyClass as 
specified by NMDC schema.

## Architecture: MongoDB access patterns

`MongoDBLoader` reaches MongoDB through two paths simultaneously — a deliberate hybrid, not an oversight.

**linkml-store (the "dog food" path).** Used for schema-aware setup and for any path where per-document work is acceptable:

- `Client(handle=...)` / `attach_database(...)` — declarative connection that integrates with NMDC's LinkML schema tooling.
- `db.create_collection(name, recreate_if_exists=False)` — idempotent collection setup.
- `collection.index(...)` — idempotent index declaration on `id`, `is_obsolete`, `name` (class collection) and `(subject, predicate, object)` (relation collection).
- `_handle_obsolete_terms` — per-item processing of the small obsolete subset.

**Raw pymongo (the hot-path bypass).** Used only for the bulk-upsert phase, exposed via the lazy `MongoDBLoader._py_db` property:

- `py_collection.bulk_write([UpdateOne(...upsert=True), ...], ordered=False)` in batches of 1,000 — turns 2N round trips per N documents into roughly 2 per 1,000.

### Why both?

`linkml_store.api.stores.mongodb.mongodb_collection.upsert` (as of the version pinned here) iterates per-item with `find_one` followed by `update_one`/`insert_one`. For ENVO/UBERON/PO that's fine. For NCBITaxon (~2.7M classes + ~55M closure relations) it's prohibitive — measured at ~1,000 ops/sec, extrapolating to tens of hours of wall time. The raw-pymongo bulk path measured **~15× faster on average** with peaks ~32× faster, completing the same NCBITaxon load in ~62 minutes instead.

The pymongo path is a *bypass*, not a permanent split. Upstream issue [`linkml/linkml-store#77`](https://github.com/linkml/linkml-store/issues/77) tracks adding `bulk_write` support to linkml-store. When that lands, the bypass becomes redundant and the loader should migrate back to a single linkml-store-only code path.

The bypass is gated behind the lazy `MongoDBLoader._py_db` property so existing-client paths (e.g., a Dagster job passing in its own `MongoClient`) reuse the supplied client rather than opening a separate connection. The property's docstring re-states this rationale in code.

## Development Environment

#### Pre-requisites

- >=Python 3.9
- Poetry
- Docker
- MongoDB
- NMDC materialized schema
- ENV variable for MONGO_PASSWORD (or pass it in via the cli/runner itself directly)

```bash
% docker pull mongo
% docker run -d --name mongodb-container -p 27018:27017 mongo
```

#### MongoDB Connection Settings

When connecting to MongoDB, you need to set the correct environment variables depending on where your code is running:

1. When running from your local machine (CLI or tests):
   ```bash
   export MONGO_HOST=localhost
   export MONGO_PORT=27018
   export ENABLE_DB_TESTS=true
   export MONGO_PASSWORD="your_valid_password"
   ```

2. When running inside Docker containers:
   ```bash
   export MONGO_HOST=mongo
   export MONGO_PORT=27017
   ```

The Docker container networking uses container names (like 'mongo') for internal communication, while your host machine must use 'localhost' with the mapped port (27018).

#### Basic mongosh commands
```bash
% docker ps
% docker exec -it [mongodb-container-id] bash
% mongosh mongodb://admin:root@mongo:27017/nmdc?authSource=admin
% show dbs
% use nmdc
% db.ontology_class_set.find().pretty()
% db.ontology_relation_set.find().pretty()
% db.ontology_class_set.find( { id: { $regex: /^PO/ } } ).pretty()
% db.ontology_class_set.find( { id: { $regex: /^UBERON/ } } ).pretty()
% db.ontology_class_set.find( { id: { $regex: /^ENVO/ } } ).pretty()
``` 

#### Command line
```bash
% poetry install
% poetry run ontology_loader --help
% poetry run ontology_loader --source-ontology "envo"
% poetry run ontology_loader --source-ontology "uberon"
```

#### Running the tests
```bash
% make test
```

#### Running the linter
```bash
% make lint
```

#### Python example usage
```bash
pip install nmdc-ontology-loader
```

```python
from ontology_loader.ontology_load_controller import OntologyLoaderController
import tempfile

def load_ontology():
    """Load an ontology using the default MongoDB connection."""
    loader = OntologyLoaderController(
        source_ontology="envo",
        output_directory=tempfile.gettempdir(),
        generate_reports=True,
    )
    loader.run_ontology_loader()
```

#### Using with an existing MongoDB connection

If you already have a MongoDB connection established (e.g., in a Dagster/Dagit job), you can pass it directly to the OntologyLoaderController:

```python
from pymongo import MongoClient
from ontology_loader.ontology_load_controller import OntologyLoaderController
import tempfile

# Use an existing MongoDB client
mongo_client = MongoClient("mongodb://admin:password@localhost:27018/nmdc?authSource=admin")

# Pass the client and database name to OntologyLoaderController
loader = OntologyLoaderController(
    source_ontology="envo",
    output_directory=tempfile.gettempdir(),
    generate_reports=True,
    mongo_client=mongo_client,  # Pass the existing client
    db_name="nmdc",  # Required when passing an existing client
)

# The loader will use the provided client instead of creating a new connection
loader.run_ontology_loader()
```

This approach is particularly useful when:
- You're running in a job scheduler like Dagster/Dagit
- You want to reuse an existing connection pool
- You have custom MongoDB connection settings that are managed externally
- You need to use a connection with specific authentication or configuration

> **Note**: When passing an existing MongoDB client, you must also provide the `db_name` parameter to specify which database to use. This is required as the database name cannot be automatically determined from a MongoDB client instance.

### Testing CRUD operations in a live MongoDB

If you want to test the CRUD operations in a live MongoDB instance, you need to set two environment variables:
MONGO_PASSWORD="your_valid_password"
ENABLE_DB_TESTS=true

This will allow you to run tests to actually insert/update/delete records in your MongoDB tests instance instead
of simply mocking the calls. You can then run the tests with the following command:

```bash
make test
```
 
The same test command will run without the environment variables, but it will only mock the calls to the database.
This is intended to help prevent accidental data loss or corruption in a live database environment and to 
ensure that MONGO_PASSWORD is not hardcoded in the codebase.

### Reset collections in dev

```bash
docker exec -it nmdc-runtime-test-mongo-1 bash
```
```bash
mongosh mongodb://admin:root@mongo:27017/nmdc?authSource=admin
db.ontology_class_set.find({}).pretty()
db.ontology_relation_set.find({}).pretty()
db.biosample_set.find({}).pretty()
db.ontology_class_set.drop()
db.ontology_relation_set.drop()
db.ontology_class_set.countDocuments()
db.ontology_relation_set.countDocuments()
```
