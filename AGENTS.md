# AGENTS.md

## Project overview

`ontology-loader` is a suite of tools that configures and loads ontologies from the OboFoundry into MongoDB, 
storing them as NMDC schema-compliant `OntologyClass` and `OntologyRelation` documents. It supports both incremental 
updates and large-scale complex ontologies such as NCBITaxon (2.7M classes + 54.7M relations).

## Architecture: MongoDB access patterns

- **linkml-store** — schema-aware setup (declarative connection, idempotent collection/index creation) and per-item
work where that's acceptable (e.g. obsolete-term handling).
- **Raw pymongo** — used only for the fast-initial bulk-insert path (`insert_ontology_data_fast_initial`, no
upsert, no per-item find), via the lazy `MongoDBLoader._py_db` property, because `linkml-store`'s `upsert`
iterates per-item (`find_one` + `update_one`/`insert_one`), which is too slow for large ontologies.

## Best practice

* This repo uses `poetry` for managing dependencies. Never use commands like `pip` to add or manage dependencies.
* write pytest tests
* always write pytest functional style rather than unittest OO style
* use modern pytest idioms, including `@pytest.mark.parametrize` to test for combinations of inputs
* NEVER write mock tests unless requested. I need to rely on tests to know if something breaks
* For tests that have external dependencies, gate them on the `MONGO_*` env vars (see below) so they skip gracefully 
when MongoDB is unavailable
* Do not "fix" issues by changing or weakening test conditions. Try harder, or ask questions if a test fails.
* Avoid try/except blocks, these can mask bugs (except in test cleanup `try`/`finally`, see safety rules below)
* Fail fast is a good principle
* Follow the DRY principle
* Avoid repeating chunks of code, but also avoid premature over-abstraction
* Declarative principles are favored
* Always use type hints, always document methods and classes
* Write in clear, concise tone.  Code docs should be clear and to the point.  No flowery language about why something is fixed or not.
* Avoid jargon and tech-bro speak like "when this lands" or "in flight."
* Production collection names are `ontology_class_set` and `ontology_relation_set` in the `nmdc` database — never write to these from tests. 
* When passing an existing `MongoClient` to `OntologyLoaderController`, you must also provide `db_name` (it cannot be auto-determined from the client). 
* The 0.2.x constructor kwargs (`output_directory`, `generate_reports`) continue to work as deprecated aliases — see the migration table in `README.md`. Do not break that backwards compatibility without explicit discussion. 
* Never hardcode `MONGO_PASSWORD` in the codebase.

## Build and test

Local development uses Poetry:
```
poetry install
```

Run via Make:
- `make install` — `poetry install`
- `make test` — `poetry run pytest tests`
- `make lint` — `poetry run tox -e lint-fix` (ruff format + ruff check --fix)

### Safety rules for DB-writing tests

Any test that **writes or modifies** MongoDB documents must:

1. **Use a dedicated scratch database or collection name** — never the production names (`nmdc`, `ontology_class_set`, `ontology_relation_set`). Use something that can't collide with real data (e.g. `ontology_loader_smoke_test`).
2. **Verify the target does not already exist before writing** — if it does, fail loudly so the developer investigates rather than silently overwriting data.
3. **Clean up**.

The smoke test `tests/test_cli_smoke.py::test_controller_end_to_end_against_live_mongo` shows the pattern.
