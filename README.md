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
