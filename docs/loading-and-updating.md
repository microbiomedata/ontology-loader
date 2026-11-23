# Loading and updating ontologies

The loader reads a semsql SQLite build of an ontology and writes NMDC schema `OntologyClass` and `OntologyRelation` documents to MongoDB's `ontology_class_set` and `ontology_relation_set`. It does not copy semsql tables into MongoDB.

## Choose a mode and closure

| Operation | `--source-ontology` | `--mode` | `--closure` |
| --- | --- | --- | --- |
| First NCBITaxon load, or a retry of the same snapshot | `ncbitaxon` | `fast-initial` | `isa` |
| Replace NCBITaxon with a new snapshot | `ncbitaxon` | `fast-initial`, after removing the old contents | `isa` |
| First load or routine update of ENVO, UBERON or PO | `envo`, `uberon`, `po` | `meticulous` | `combined` |

`isa` writes `entailed_isa_closure`. NCBITaxon has no `part_of` edges in the snapshot tested. `combined` writes `entailed_isa_partof_closure`, which follows both `rdfs:subClassOf` and `BFO:0000050`. Direct relations are written in every case. `all` writes several closure predicates and isn't needed for these loads.

Single-predicate closures such as `isa` skip SQLite's temporary deduplication B-tree. Multi-predicate closures such as `combined` still need it, so the NCBITaxon memory figures below don't bound `combined` on other large ontologies.

`--exclude-descendants-of <CURIE>`, repeatable, leaves out the `rdfs:subClassOf` descendants of a term and keeps the term itself. Excluding `NCBITaxon:6656` (Arthropoda) cuts an NCBITaxon load from 4.34 GB to 2.12 GB on disk, because arthropods are 49.6 percent of the ancestry closure.

## Load into a local MongoDB

Set the connection explicitly. The loader's default port is 27022 and its default database is `nmdc`, and neither is right for a local trial. Set the password through your credential manager, not in a committed file or on the command line.

After checking that `ontology_ncbitaxon_trial` doesn't already exist:

```bash
export MONGO_HOST=localhost
export MONGO_PORT=27017
export MONGO_USERNAME=admin
export MONGO_DB=ontology_ncbitaxon_trial
# Set MONGO_PASSWORD before running the loader.
poetry run ontology_loader --source-ontology ncbitaxon --mode fast-initial --closure isa
```

Fast-initial streams documents in batches of 5,000. Its class documents have `relations=[]`, so read relations from `ontology_relation_set`. Meticulous mode embeds relations in class documents and writes TSV reports.

ENVO, UBERON and PO can run in one command because they use the same mode and closure:

```bash
poetry run ontology_loader --source-ontology envo --source-ontology uberon \
  --source-ontology po --mode meticulous --closure combined \
  --report-directory ./ontology-reports
```

Each ontology gets its own report subdirectory. Run NCBITaxon separately.

## Retry, update and replace

- Retry a snapshot with `fast-initial`. It skips class IDs and relation triples that already exist and inserts the missing ones, so it can finish a partial load without duplicates. It doesn't update existing labels or definitions, so if the source changed between runs the result mixes snapshots.
- Update with `meticulous`. It inserts and updates class documents and relation triples, handles obsolete classes, and writes reports. It doesn't delete a relation that disappeared from the new source, so MongoDB won't exactly mirror the source.
- Replace by removing the old contents and then running `fast-initial`. The loader doesn't clear collections itself. https://github.com/microbiomedata/nmdc-runtime/pull/1562 proposes a scoped delete followed by a fast-initial reload. The data is missing or partial until the reload finishes. The NCBITaxon prefix has been checked for this. Check other ontologies before deleting by prefix, because their terms can reference other ontologies.

For a local trial, load into a new database and check it before switching readers to it. That keeps the previous snapshot. The loader doesn't switch readers.

## Source freshness depends on the mode

Meticulous deletes the cached source and downloads it again. Fast-initial reuses the cached source and downloads only when it's missing. There's no separate refresh option, so deleting the MongoDB documents and running fast-initial can reload the same old snapshot. Download the new source before deleting anything.

## Known gaps

- Nothing records which ontology release a load came from, or whether it was subsetted. A subsetted load and one that failed partway look the same. https://github.com/microbiomedata/ontology-loader/issues/81
- Refreshing the source is tied to the mode. https://github.com/microbiomedata/ontology-loader/issues/13
- Class documents differ by mode: meticulous embeds relations and fast-initial doesn't. https://github.com/microbiomedata/ontology-loader/issues/78
- The deprecated `generate_reports=False` argument now selects fast-initial, which skips updates to existing documents. https://github.com/microbiomedata/ontology-loader/issues/96
- Peak memory isn't explained, so the headroom under the production worker's 12 GiB limit is unknown. https://github.com/microbiomedata/ontology-loader/issues/87
- How nmdc-runtime should order and retry fast-initial reloads isn't settled. https://github.com/microbiomedata/nmdc-runtime/issues/1565

## Reference NCBITaxon loads, September 2026

Two complete loads on an Apple M5 laptop with a local MongoDB, from NCBITaxon semsql release 2025-12-03, both excluding the descendants of `NCBITaxon:6656` (Arthropoda). The second run included the change that computes `is_root` in SQL.

| | before | after |
|---|---:|---:|
| class documents | 1,742,956 | 1,742,956 |
| relation documents | 27,958,594 | 27,958,594 |
| on disk, both collections | 2.12 GB | 2.12 GB |
| wall time | 30.7 min | 29.0 min |
| peak RSS | 6.22 GB | 7.01 GB |

A full load with no exclusion is 2,708,804 classes, 54,700,052 relations, and 4.34 GB on disk, of which 2.79 GB is indexes.

Peak memory rose after a change that should have lowered it (https://github.com/microbiomedata/ontology-loader/issues/87), so treat the peak RSS figures as unexplained. Wall times are from a laptop and won't match the production worker. Disk sizes should.
