## ontology_loader

Suite of tools to configure and load an ontology from the OboFoundary into the data object for OntologyClass as 
specified by NMDC schema.

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

The test suite follows a single convention: **tests that need MongoDB run automatically when MongoDB and credentials are available; they skip gracefully when not.**

In practice:

- **Mock-only tests** (e.g. `tests/test_mock_mongodb_loader.py`) run unconditionally — no MongoDB or credentials needed.
- **Tests that exercise a live MongoDB** are gated by `MONGO_PASSWORD` (and a few additionally require `ENABLE_DB_TESTS=true` as an extra safety check against accidental writes against unintended databases). When the gating env vars are unset, those tests skip with a clear reason; when they are set, the tests connect to the MongoDB pointed at by the rest of the `MONGO_*` env vars.

Required env vars when running the live-DB tests:

```bash
export MONGO_HOST=localhost
export MONGO_PORT=27017            # or whatever your local Mongo listens on
export MONGO_USERNAME=admin
export MONGO_PASSWORD="your_valid_password"
export MONGO_DB=nmdc               # read by the loader (see src/ontology_loader/mongo_db_config.py)
export MONGO_DBNAME=nmdc           # read by tests/test_ontology_class_null_values.py — currently a separate name from MONGO_DB
export ENABLE_DB_TESTS=true        # required by tests/test_ontology_load_controller.py
```

Then:

```bash
make test
```

Same command runs without the env vars; the DB-gated tests just skip. Mock-only tests still run either way. This is intended both to prevent accidental writes against a live database when env vars aren't deliberately set, and to make sure `MONGO_PASSWORD` is never hardcoded in the codebase.

> **Known inconsistencies (separate PRs in flight):**
>
> - `tests/test_linkml_store_client_connections.py` still hardcodes `MONGO_PORT = 27022` (and host / user / db). PR #23 makes it read from env vars to match the rest of the suite.
> - GitHub Actions CI doesn't yet spin up a MongoDB service; the DB-gated tests skip in CI. PR #39 adds a `services: mongo:` block and sets the env vars on the test step.

#### Safety rules for DB-writing tests

Any test that **writes or modifies** MongoDB documents must follow these rules:

1. **Use a dedicated scratch database or collection name** — never the production names (`nmdc`, `ontology_class_set`, `ontology_relation_set`). The scratch name should be specific enough that it can't collide with real data (e.g. `ontology_loader_smoke_test`).
2. **Verify the target does not already exist before writing** — if it does, the test must fail loudly with a clear message so the developer investigates rather than silently overwriting unrelated data.
3. **Clean up unconditionally at the end** — wrap the test in `try` / `finally` so the cleanup runs even when assertions fail.

The smoke test `tests/test_cli_smoke.py::test_controller_end_to_end_against_live_mongo` shows the pattern.

#### What each live-DB test does

| File | What it touches |
|---|---|
| `tests/test_linkml_store_client_connections.py` | Verifies that both raw `pymongo` and linkml-store's `Client` can establish a connection. |
| `tests/test_ontology_class_null_values.py` | Inserts and reads ontology class docs to confirm boolean/text fields don't store `null`. |
| `tests/test_ontology_load_controller.py` | Runs `OntologyLoaderController.run_ontology_loader()` against a small live ENVO load. |
| `tests/test_cli_smoke.py::test_controller_end_to_end_against_live_mongo` | Stubs the heavy semsql step, runs the controller end-to-end against MongoDB, and verifies the expected documents land. |

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
