# AGENTS.md

## Project overview

`ontology-loader` is a suite of tools that configures and loads ontologies from the OboFoundry into MongoDB, 
storing them as NMDC schema-compliant `OntologyClass` and `OntologyRelation` documents. It supports both incremental 
updates and large-scale complex ontologies such as NCBITaxon (2.7M classes + 54.7M relations).

It is published to PyPI as `nmdc-ontology-loader` and is consumed by `nmdc-runtime` (a Dagster/Dagit job that 
calls `OntologyLoaderController`).

## Architecture: MongoDB access patterns

- **linkml-store** — schema-aware setup (declarative connection, idempotent collection/index creation) and per-item
work where that's acceptable (e.g. obsolete-term handling).
- **Raw pymongo** — used only for the bulk-upsert phase, via the lazy `MongoDBLoader._py_db` property, 
because `linkml-store`'s `upsert` iterates per-item (`find_one` + `update_one`/`insert_one`), which is too slow for large ontologies.

The pymongo path is a *bypass*, not a permanent split. Upstream issue [`linkml/linkml-store#77`](https://github.com/linkml/linkml-store/issues/77) tracks adding `bulk_write` support to linkml-store.

## Repo management

This repo uses `poetry` for managing dependencies. Never use commands like `pip` to add or manage dependencies.

Note the pinned constraint `setuptools = "<80"` (issue #42): `eutils` (transitive via `oaklib`) imports `pkg_resources` at module load and breaks on setuptools >=80.

## Repository structure

- `src/ontology_loader/` — Application code.
  - `cli.py` — Click CLI entry point (`ontology_loader`).
  - `ontology_load_controller.py` — `OntologyLoaderController`, the main orchestrator.
  - `ontology_processor.py` — Ontology extraction and closure computation.
  - `mongodb_loader.py` — Core MongoDB interface (hybrid linkml-store + pymongo).
  - `mongo_db_config.py` — `MONGO_*` env var parsing / connection config.
  - `reporter.py` — TSV report generation.
  - `utils.py` — Utilities.
- `tests/` — Pytest test suite (`conftest.py` holds fixtures and DB cleanup).
- `docs/` — Sphinx documentation.
- `.github/workflows/` — CI/CD:
  - `main.yaml` — lint + test matrix (Python 3.10/3.11/3.12, MongoDB 7 service) on push/PR.
  - `publish.yaml` — build and publish to PyPI on GitHub release.
  - `codespell.yaml` — spell checking.
  - `deploy-docs.yaml` — Sphinx docs deploy to gh-pages.
- `pyproject.toml`, `poetry.lock`, `Makefile`, `tox.ini` — build/dependency/test config.

## CLI usage

The CLI exposes four flags (see README for full detail):

- `--source-ontology <name>` — required, repeatable. Lowercase prefix (envo, po, uberon, ncbitaxon, …). Processed sequentially in the given order.
- `--report-directory <dir>` — TSV report destination (only used in `meticulous` mode). Defaults to a fresh temp directory.
- `--mode {meticulous|fast-initial}` — default `meticulous` (per-item upsert, preserves 0.2.x behavior); `fast-initial` is a max-throughput first-time install via raw pymongo `insert_many`.
- `--closure {combined|isa|partof|all|none}` — default `combined`. Repeatable; `all` and `none` are exclusive.

## Best practice

* write pytest tests
* always write pytest functional style rather than unittest OO style
* use modern pytest idioms, including `@pytest.mark.parametrize` to test for combinations of inputs
* NEVER write mock tests unless requested. I need to rely on tests to know if something breaks
* For tests that have external dependencies, gate them on the `MONGO_*` env vars (see below) so they skip gracefully when MongoDB is unavailable
* Do not "fix" issues by changing or weakening test conditions. Try harder, or ask questions if a test fails.
* Avoid try/except blocks, these can mask bugs (except in test cleanup `try`/`finally`, see safety rules below)
* Fail fast is a good principle
* Follow the DRY principle
* Avoid repeating chunks of code, but also avoid premature over-abstraction
* Declarative principles are favored
* Always use type hints, always document methods and classes

## Build and test

Local development uses Poetry:
```
poetry install
```

Run via Make:
- `make install` — `poetry install`
- `make test` — `poetry run pytest tests`
- `make lint` — `poetry run tox -e lint-fix` (ruff format + ruff check --fix)

Pytest auto-enables coverage (`pytest-cov`) with an 80% threshold (configured in `pyproject.toml`).

## Testing against a live MongoDB

The test suite follows a single convention: **tests that need MongoDB run automatically when MongoDB and credentials are available; they skip gracefully when not.**

- **Mock-only tests** (e.g. `tests/test_mock_mongodb_loader.py`) run unconditionally — no MongoDB or credentials needed.
- **Live-DB tests** are gated by `MONGO_PASSWORD` (some additionally require `ENABLE_DB_TESTS=true` as an extra safety check). When the gating env vars are unset, those tests skip with a clear reason.

Required env vars for live-DB tests:
```bash
export MONGO_HOST=localhost
export MONGO_PORT=27017            # or whatever your local Mongo listens on
export MONGO_USERNAME=admin
export MONGO_PASSWORD="your_valid_password"
export MONGO_DB=nmdc               # read by the loader (src/ontology_loader/mongo_db_config.py)
export MONGO_DBNAME=nmdc           # read by tests/test_ontology_class_null_values.py
export ENABLE_DB_TESTS=true        # required by tests/test_ontology_load_controller.py
```

A local Mongo for development:
```bash
docker pull mongo
docker run -d --name mongodb-container -p 27018:27017 mongo
```

### Safety rules for DB-writing tests

Any test that **writes or modifies** MongoDB documents must:

1. **Use a dedicated scratch database or collection name** — never the production names (`nmdc`, `ontology_class_set`, `ontology_relation_set`). Use something that can't collide with real data (e.g. `ontology_loader_smoke_test`).
2. **Verify the target does not already exist before writing** — if it does, fail loudly so the developer investigates rather than silently overwriting data.
3. **Clean up unconditionally** — wrap the test in `try` / `finally` so cleanup runs even when assertions fail.

The smoke test `tests/test_cli_smoke.py::test_controller_end_to_end_against_live_mongo` shows the pattern.

## Releasing

Versioning is git-based via `poetry-dynamic-versioning` (PEP440). `pyproject.toml` carries `version = "0.0.0"` and the build backend is `poetry-dynamic-versioning.backend`.

1. Decide the new version following semver (e.g. `v0.3.0`). Previous releases: https://github.com/microbiomedata/ontology-loader/releases.
2. Create the GitHub release from `main`:
   ```
   gh release create vX.Y.Z --target main --generate-notes --repo microbiomedata/ontology-loader
   ```
3. This triggers `.github/workflows/publish.yaml`, which runs `poetry build` and publishes `nmdc-ontology-loader` to PyPI.

## Related repositories

- [nmdc-runtime](https://github.com/microbiomedata/nmdc-runtime) — Dagster/Dagit runtime whose `load_ontology` op calls `OntologyLoaderController` (0.2.x backwards compatibility is maintained).
- [nmdc-schema](https://github.com/microbiomedata/nmdc-schema) — Defines the `OntologyClass` / `OntologyRelation` models.
- [linkml-store](https://github.com/linkml/linkml-store) — Schema-aware MongoDB wrapper; issue #77 tracks the bulk-write support this repo currently bypasses.

## Important conventions

- Requires Python >=3.10.
- Production collection names are `ontology_class_set` and `ontology_relation_set` in the `nmdc` database — never write to these from tests.
- When passing an existing `MongoClient` to `OntologyLoaderController`, you must also provide `db_name` (it cannot be auto-determined from the client).
- The 0.2.x constructor kwargs (`output_directory`, `generate_reports`) continue to work as deprecated aliases — see the migration table in `README.md`. Do not break that backwards compatibility without explicit discussion.
- Never hardcode `MONGO_PASSWORD` in the codebase.
