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
% poetry run ontology_loader --source-ontology envo
% poetry run ontology_loader --source-ontology envo --source-ontology po --source-ontology uberon
```

Four flags:

- `--source-ontology <name>` — required, repeatable. Lowercase prefix (envo, po, uberon, ncbitaxon, …). Multiple ontologies are processed sequentially in the given order.
- `--report-directory <dir>` — TSV report destination (only used in `meticulous` mode). Defaults to a fresh temp directory.
- `--mode {meticulous|fast-initial}` — default `meticulous`. See "Modes" below.
- `--closure {combined|isa|partof|all|none}` — default `combined`. Repeatable; values combine. `all` and `none` are exclusive.

##### Modes

- **`meticulous`** (default): Preserves 0.2.x behavior — pure linkml-store, per-item upsert, force-refresh of the pystow cache on every run, TSV reports (`ontology_updates.tsv`, `ontology_inserts.tsv`, `ontology_relation_inserts.tsv`) written to `--report-directory`. Use this for incremental updates of an already-loaded ontology.
- **`fast-initial`**: Maximum-throughput first-time install. Raw pymongo `insert_many(ordered=False)`, no upsert, no pre-read, no report tracking, no TSV writes. Reuses the pystow cache if present (downloads only when missing). Use this when the target collections are empty or duplicate-key errors are acceptable. Expected ~3-5x faster than `meticulous` on large ontologies (e.g. NCBITaxon's 2.7M classes + 54.7M relations).

##### Closure shorthands

- `--closure combined` (default): emits `entailed_isa_partof_closure` (rdfs:subClassOf ∪ BFO:0000050).
- `--closure isa`: emits `entailed_isa_closure` (rdfs:subClassOf only).
- `--closure partof`: emits `entailed_partof_closure` (BFO:0000050 only).
- `--closure all`: shorthand for `--closure combined --closure isa --closure partof`. Exclusive.
- `--closure none`: emit no ancestry closure, only direct relationships. Exclusive.

Repeat the flag to combine specific closures: `--closure isa --closure partof` emits both `entailed_isa_closure` and `entailed_partof_closure`.

#### Running the tests
```bash
% make test
```

#### Running the linter
```bash
% make lint
```

#### Python API

```bash
pip install nmdc-ontology-loader
```

```python
from ontology_loader.ontology_load_controller import OntologyLoaderController
import tempfile

# Default: pure linkml-store + TSV reports (preserves 0.2.x behavior)
OntologyLoaderController(
    source_ontology="envo",                          # str or list[str]
    report_directory=tempfile.gettempdir(),          # only used in 'meticulous' mode
    mode="meticulous",                               # or 'fast-initial'
    closure="combined",                              # str or list[str]
).run_ontology_loader()
```

##### Fast first-time install of a large ontology

```python
OntologyLoaderController(
    source_ontology="ncbitaxon",
    mode="fast-initial",        # raw pymongo insert_many, no upsert, no reports
    closure="isa",              # is_a only; combined closure is too large for NCBITaxon
).run_ontology_loader()
```

##### Multiple ontologies in one invocation

```python
OntologyLoaderController(
    source_ontology=["envo", "po", "uberon"],   # processed sequentially in given order
    mode="meticulous",
).run_ontology_loader()
```

##### Using with an existing MongoDB connection

If you already have a MongoDB connection (e.g., in a Dagster/Dagit job), pass it directly:

```python
from pymongo import MongoClient
from ontology_loader.ontology_load_controller import OntologyLoaderController

mongo_client = MongoClient("mongodb://admin:password@localhost:27018/nmdc?authSource=admin")

OntologyLoaderController(
    source_ontology="envo",
    mode="meticulous",
    mongo_client=mongo_client,   # Pass the existing client
    db_name="nmdc",              # Required when passing an existing client
).run_ontology_loader()
```

> **Note**: When passing an existing MongoDB client, you must also provide `db_name`. The database name cannot be auto-determined from a MongoClient instance.

#### Migrating from 0.2.x

The 0.2.x constructor signature (`source_ontology`, `output_directory`, `generate_reports`, `mongo_client`, `db_name`) continues to work as deprecated aliases. The exact call site in nmdc-runtime's Dagster `load_ontology` op runs unchanged under 0.3.0; two `DeprecationWarning` lines appear in the logs as a nudge.

| old kwarg | new kwarg | behavior |
|---|---|---|
| `source_ontology=<str>` | `source_ontology=<str \| list[str]>` | unchanged; now also accepts a list |
| `output_directory=<str>` | `report_directory=<str>` | renamed; old kwarg works as alias with `DeprecationWarning`. Passing both raises. |
| `generate_reports=True` | (gone — implicit) | no-op with `DeprecationWarning` (True was always the default) |
| `generate_reports=False` | `mode='fast-initial'` | mapped with `DeprecationWarning`. If `mode` was also passed and isn't `'meticulous'`, raises. |
| (none) | `mode='meticulous'` (default) | new; default preserves 0.2.x write path |
| (none) | `closure='combined'` (default) | new; default preserves 0.2.x ancestry behavior |

See `CHANGELOG.md` for the full release note and a side-by-side migration code sample.

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
