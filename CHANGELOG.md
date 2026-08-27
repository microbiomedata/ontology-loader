# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **`mode='fast-initial'` silently duplicated data on rerun instead of raising `DuplicateKeyError` as documented.** Released in 0.2.3 with no unique index on either target collection, so `insert_many` had nothing to detect a duplicate against: rerunning a `fast-initial` load against an already-populated `ontology_class_set`/`ontology_relation_set` doubled every class and relation with no error, no warning beyond a log line, and no indication in the run's final status. Reproduced directly: a 12,000-class/11,999-relation load, rerun without clearing, doubled to 24,000/23,998 before this fix; after, counts are unchanged on rerun and the underlying unique index rejects direct duplicate inserts with a real `DuplicateKeyError`.
  - `insert_ontology_data_fast_initial` now declares a unique index on `id` (classes) and `(subject, predicate, object)` (relations) before inserting — separately named from the non-unique indexes `upsert_ontology_data` declares on the same fields, so this is purely additive and does not touch, rebuild, or risk the meticulous path's existing indexes on these shared collections.
  - `_bulk_insert_iter` now catches `BulkWriteError` per batch: genuine duplicate-key rejections (code `11000`) are logged and skipped, and the loop continues to the next batch — a rerun is now idempotent instead of either silently duplicating (no index) or aborting every batch after the first collision (index alone, without this). Any non-duplicate-key write error still propagates; it is not something this method can safely recover from.

### Added
- `OntologyLoaderController(mode=...)` kwarg accepting `'meticulous'` (default) or `'fast-initial'`.
  - `meticulous` preserves 0.2.x behavior exactly: pure linkml-store, per-item upsert, force-refresh of the pystow cache, TSV reports written to `report_directory`.
  - `fast-initial` is the new maximum-throughput first-time-install path: raw pymongo `insert_many(ordered=False)`, no pre-read, no upsert, no report tracking, no TSV writes. Reuses the pystow cache if present.
- `OntologyLoaderController(closure=...)` kwarg accepting a string or list. Values: `combined` (default), `isa`, `partof`, `all` (exclusive shorthand for combined+isa+partof), `none` (exclusive — emit no ancestry closure).
- `OntologyLoaderController(report_directory=...)` kwarg — renamed from `output_directory`.
- `OntologyLoaderController(source_ontology=...)` now accepts a list of strings as well as a single string. Multiple ontologies are processed sequentially in the given order; failure on one halts the run.
- New CLI surface — four flags total: `--source-ontology` (repeatable, required), `--report-directory`, `--mode`, `--closure` (repeatable).
- `MongoDBLoader.insert_ontology_data_fast_initial(...)` — the raw-pymongo write method used by `mode='fast-initial'`.
- `OntologyProcessor(force_refresh=...)` constructor kwarg — `True` (default) preserves 0.2.x cache-wiping behavior; `False` reuses the cached pystow artifact when present.

### Changed
- `OntologyProcessor.get_relations_closure()` signature: `predicates=` removed in favor of `closure=` (string or list of strings). The old hardcoded ancestry-relation name (`entailed_isa_partof_closure`) is now selected per-closure: `entailed_isa_partof_closure`, `entailed_isa_closure`, or `entailed_partof_closure`.
- CLI no longer takes `--generate-reports`. Equivalents under the new design:
  - Old `--generate-reports true` (the implicit default) → new `--mode meticulous` (the default).
  - Old `--generate-reports false` → new `--mode fast-initial`.

### Deprecated
- `OntologyLoaderController(output_directory=...)` — use `report_directory=` instead. The old kwarg is an alias and emits `DeprecationWarning`. Passing both raises `ValueError`.
- `OntologyLoaderController(generate_reports=True)` — no-op with `DeprecationWarning`. (True was always the default.)
- `OntologyLoaderController(generate_reports=False)` — maps to `mode='fast-initial'` with `DeprecationWarning`. If `mode` was also passed and isn't `'meticulous'`, raises `ValueError`.

### Removed
- (nothing removed in this release — all 0.2.x kwargs continue to work as deprecated aliases. Removal slated for the next major release after downstream callers — notably the `nmdc-runtime` Dagster job — migrate.)

### Migration

The exact 0.2.x call site in `nmdc-runtime`'s Dagster op (`nmdc_runtime/site/ops.py`, `load_ontology`) — `OntologyLoaderController(source_ontology=str, output_directory=str, generate_reports=True, mongo_client=..., db_name=...)` — runs **unchanged** under this release. Two `DeprecationWarning` lines appear in the Dagster logs as a nudge to update.

To migrate when convenient:

```python
# 0.2.x
OntologyLoaderController(
    source_ontology="envo",
    output_directory="/tmp/ontology_reports",
    generate_reports=True,
    mongo_client=client,
    db_name="nmdc",
)

# 0.3.0+
OntologyLoaderController(
    source_ontology="envo",        # or ["envo", "po", "uberon"]
    report_directory="/tmp/ontology_reports",
    mode="meticulous",             # default; or "fast-initial"
    closure="combined",            # default; or "isa", "partof", "all", "none", or a list
    mongo_client=client,
    db_name="nmdc",
)
```
