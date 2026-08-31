# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Every fresh ontology download was broken: the raw `bbop-sqlite` S3 bucket's public access has been retired.** `download_and_prepare_ontology` hardcoded `https://s3.amazonaws.com/bbop-sqlite/`, which now returns a genuine `403 AccessDenied` (verified directly, not a transient blip). Since `OntologyProcessor` defaults to `force_refresh=True`, and any environment with no existing cache has nothing to fall back on, this broke every first-time load of every ontology, not just NCBITaxon. Fixed by repointing to the SemanticSQL CDN (`https://semanticsql.berkeleybop.io/`, identical object paths) and switching pystow's download backend to `requests` with an explicit non-default `User-Agent` — the CDN sits behind a Browser Integrity Check that 403s default client User-Agents (`Python-urllib`, bare `curl`/`wget`), and pystow's default `urllib` backend has no clean way to set request headers at all. Verified end-to-end: a genuinely fresh, never-cached download (PATO) now succeeds. See https://github.com/microbiomedata/ontology-loader/issues/59.
- **`mode='fast-initial'` crashed on its first run against a collection with a pre-existing unique index on the same key, under a different name.** 0.3.0's fix below said the fast-initial unique index is "purely additive and does not touch, rebuild, or risk the meticulous path's existing indexes" — true for the data, not for MongoDB's index-name rule: it refuses to create a second index with an identical key pattern under a different name, even when the existing one is already unique. NMDC prod's `ontology_class_set` has exactly this: a unique index on `id` named `id_1` from the meticulous path (added 2025-05-13), so fast-initial's own `ontology_class_fast_initial_unique_id_index` would have crashed before loading any data. Verified against a local MongoDB reproducing prod's actual index, then confirmed prod's index directly. `insert_ontology_data_fast_initial` now checks for a pre-existing index on the same key spec before creating its own; if one already exists and is unique, it's reused instead of duplicated. See https://github.com/microbiomedata/ontology-loader/issues/68.
- **The reuse check above would have silently accepted a sparse or partial unique index as equivalent to a real one.** Both report `unique: true` while admitting duplicates outside their own scope: a sparse unique index allows unlimited documents missing the field entirely, and a partial unique index allows unlimited duplicates outside its filter. Verified empirically against a real MongoDB that both shapes accept a second document a plain unique index rejects. Confirmed prod's actual `id_1` index is plain (neither sparse nor partial) as of 2026-08-28, so this was a latent gap, not a live one. `_ensure_fast_initial_unique_index` now raises instead of reusing a same-key unique index that is sparse or carries a `partialFilterExpression`.
- **NCBITaxon-scale ancestry closure computation took ~29 minutes for one closure type, dominated by one `adapter.ancestors()` SQL traversal per entity (~2.7M individual calls).** `get_relations_closure`'s ancestry phase now reads directly from semsql's own `entailed_edge` table, which already materializes the full transitive closure, instead of recomputing it one entity at a time. Measured against the real NCBITaxon semsql database and a real prior run's own log: 29m9s (the old per-entity loop) versus 13.0s for a bulk scan of all 51,991,442 rows — about 134x faster for that phase. Streamed via a generator rather than `fetchall()`, since NCBITaxon-scale results are tens of millions of rows. Two correctness gaps found by review and fixed before this landed: `entailed_edge` is not reliably reflexive per predicate (`rdfs:subClassOf` mostly is; `BFO:0000050`/part_of almost never is — 2 self-loops out of 36,857 envo edges, verified directly), so self-pairs are now unioned in explicitly rather than relied on from the table; and a bare id-prefix match doesn't exclude deprecated nodes the way the old `adapter.entities()`-based start set did (envo's `entailed_edge` has 453 rows for deprecated subjects on `rdfs:subClassOf` alone), so results are now filtered against the same `relevant_entities` set the direct-relationship phase already used. A third gap found on the next review round: the explicit reflexive-self-pair union could double up with a self-pair `entailed_edge` already had natively, producing a real duplicate relation in the plain list the caller appends to — verified directly against envo (25,120 pairs with 3,872 duplicates before the fix; 21,248 with zero after). A fourth gap: the old per-entity loop only prefix-checked ancestors (objects), not deprecation, so a non-obsolete entity could have a deprecated ancestor; the earlier fix applied `relevant_entities` (deprecation-excluding) to both sides, which is stricter than the old behavior on the object side. Restored the old asymmetry exactly -- subjects checked against `relevant_entities`, objects against the id prefix alone -- though checked exhaustively against envo's `entailed_edge` and found zero rows where this actually differs from the stricter version today. All four covered by new regression tests against real envo data, including one that would have caught the duplicate by asserting uniqueness before deduplicating into a set. See https://github.com/microbiomedata/ontology-loader/issues/18.

## [0.3.0] - 2026-08-28

### Fixed
- **`mode='fast-initial'` silently duplicated data on rerun instead of raising `DuplicateKeyError` as documented.** Released in 0.2.3 with no unique index on either target collection, so `insert_many` had nothing to detect a duplicate against: rerunning a `fast-initial` load against an already-populated `ontology_class_set`/`ontology_relation_set` doubled every class and relation with no error, no warning beyond a log line, and no indication in the run's final status. Reproduced directly: a 12,000-class/11,999-relation load, rerun without clearing, doubled to 24,000/23,998 before this fix; after, counts are unchanged on rerun and the underlying unique index rejects direct duplicate inserts with a real `DuplicateKeyError`.
  - `insert_ontology_data_fast_initial` now declares a unique index on `id` (classes) and `(subject, predicate, object)` (relations) before inserting — separately named from the non-unique indexes `upsert_ontology_data` declares on the same fields, so this is purely additive and does not touch, rebuild, or risk the meticulous path's existing indexes on these shared collections.
  - `_bulk_insert_iter` now catches `BulkWriteError` per batch: genuine duplicate-key rejections (code `11000`) are logged and skipped, and the loop continues to the next batch — a rerun is now idempotent instead of either silently duplicating (no index) or aborting every batch after the first collision (index alone, without this). Any non-duplicate-key write error still propagates; it is not something this method can safely recover from.
- **Relation-insert reports overwrote class-insert reports.** Both were constructed with `report_type="insert"`, so both wrote to `ontology_inserts.tsv`, and the relation write silently overwrote the class one on every meticulous run. Relation inserts now use `report_type="relation_insert"`, writing to the separate `ontology_relation_inserts.tsv` filename the README already documented but the code never produced.
- **Loading more than one ontology in a single invocation overwrote earlier ontologies' reports.** All ontologies wrote to the same flat `report_directory` with the same fixed filenames, so only the last ontology's reports survived the run. A single `source_ontology` keeps the existing flat layout unchanged (no path change for current callers, including nmdc-runtime's Dagster op); two or more ontologies each get their own subdirectory under `report_directory`.
- **`ontology_relation_inserts.tsv` shipped a 4-column header over 3-column data rows.** `write_reports` unconditionally prepended an `id` column to every report's header, but the relation report has no `id` field — only `subject`/`predicate`/`object`. The `id` prefix moved to the two class-report call sites instead of being hardcoded in the writer, so each report's declared header now matches its actual data shape.
- **A repeated name in `source_ontology=[...]` silently collided.** Each ontology in a multi-ontology run gets a report subdirectory named after it; a duplicate name resolved both iterations to the same subdirectory, and the second run's reports overwrote the first's. `OntologyLoaderController.__init__` now rejects duplicate names with `ValueError` before any processing starts.
- **A runtime dependency on `pytest` blocked downstream consumers from resolving [CVE-2025-71176](https://nvd.nist.gov/vuln/detail/CVE-2025-71176).** `pytest`, `tox`, and `pytest-cov` were declared under `[tool.poetry.dependencies]` (forcing them onto every downstream install) despite being imported only from `tests/` and `tox.ini`, and were duplicated with a separate, looser constraint under the dev group. Moved to dev-only dependencies, consolidated to one declaration each, with `pytest` raised to `>=9.0.3` (the actual CVE-fixed floor, not just an arbitrary point in the still-vulnerable 8.x line). `mongomock` also dropped — confirmed unused anywhere in `src/` or `tests/`, only mentioned in comments explaining why it was tried and abandoned. Note: `pytest` still reaches downstream installs transitively (`prefixcommons` requires `pytest-logging`, which requires `pytest>=2.8.1`), but that floor no longer conflicts with resolving `pytest>=9.0.3`.

## [0.2.3] - 2026-06-03

Retroactively documented: this content sat under "Unreleased" and was never moved to a dated section when 0.2.3 actually shipped. All of it was live in the 0.2.3 tag.

### Added
- `OntologyLoaderController(mode=...)` kwarg accepting `'meticulous'` (default) or `'fast-initial'`.
  - `meticulous` preserves pre-0.2.3 behavior exactly: pure linkml-store, per-item upsert, force-refresh of the pystow cache, TSV reports written to `report_directory`.
  - `fast-initial` is the new maximum-throughput first-time-install path: raw pymongo `insert_many(ordered=False)`, no pre-read, no upsert, no report tracking, no TSV writes. Reuses the pystow cache if present.
- `OntologyLoaderController(closure=...)` kwarg accepting a string or list. Values: `combined` (default), `isa`, `partof`, `all` (exclusive shorthand for combined+isa+partof), `none` (exclusive — emit no ancestry closure).
- `OntologyLoaderController(report_directory=...)` kwarg — renamed from `output_directory`.
- `OntologyLoaderController(source_ontology=...)` now accepts a list of strings as well as a single string. Multiple ontologies are processed sequentially in the given order; failure on one halts the run.
- New CLI surface — four flags total: `--source-ontology` (repeatable, required), `--report-directory`, `--mode`, `--closure` (repeatable).
- `MongoDBLoader.insert_ontology_data_fast_initial(...)` — the raw-pymongo write method used by `mode='fast-initial'`.
- `OntologyProcessor(force_refresh=...)` constructor kwarg — `True` (default) preserves pre-0.2.3 cache-wiping behavior; `False` reuses the cached pystow artifact when present.

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
- (nothing removed in this release — all pre-0.2.3 kwargs continue to work as deprecated aliases. Removal slated for the next major release after downstream callers — notably the `nmdc-runtime` Dagster job — migrate.)

### Migration

The exact pre-0.2.3 call site in `nmdc-runtime`'s Dagster op (`nmdc_runtime/site/ops.py`, `load_ontology`) — `OntologyLoaderController(source_ontology=str, output_directory=str, generate_reports=True, mongo_client=..., db_name=...)` — runs **unchanged** under this release. Two `DeprecationWarning` lines appear in the Dagster logs as a nudge to update.

To migrate when convenient:

```python
# 0.2.2 and earlier
OntologyLoaderController(
    source_ontology="envo",
    output_directory="/tmp/ontology_reports",
    generate_reports=True,
    mongo_client=client,
    db_name="nmdc",
)

# 0.2.3
OntologyLoaderController(
    source_ontology="envo",        # or ["envo", "po", "uberon"]
    report_directory="/tmp/ontology_reports",
    mode="meticulous",             # default; or "fast-initial"
    closure="combined",            # default; or "isa", "partof", "all", "none", or a list
    mongo_client=client,
    db_name="nmdc",
)
```
