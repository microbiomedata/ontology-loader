# Loading and updating ontologies

The loader reads a semsql SQLite ontology and writes NMDC-schema `OntologyClass` and
`OntologyRelation` documents to MongoDB's `ontology_class_set` and `ontology_relation_set`.
It does not copy semsql tables verbatim into MongoDB.

The streaming implementation described here is proposed in
https://github.com/microbiomedata/ontology-loader/pull/75. Release 0.3.1 does not contain it.
The reference run below tested commit `35850848d934f6a8511000ef859012c7e257db53`.

## Select a mode, closure, and which taxa to load

Since https://github.com/microbiomedata/ontology-loader/pull/85 the loader also
takes `--exclude-descendants-of <CURIE>`, repeatable, which omits the proper
`rdfs:subClassOf` descendants of a term while keeping the term itself. For
NCBITaxon this is the difference between 4.34 GB and 2.12 GB, because arthropods
alone are 49.6 percent of the ancestry closure.

A subsetted load and a load that failed halfway produce collections that look the
same, and nothing currently records which happened. See
https://github.com/microbiomedata/ontology-loader/issues/81.

## Select a mode and closure

| Operation | `source_ontology` | `mode` | `closure` |
| --- | --- | --- | --- |
| Initial NCBITaxon load | `ncbitaxon` | `fast-initial` | `isa` |
| Retry the same NCBITaxon snapshot | `ncbitaxon` | `fast-initial` | `isa` |
| Initial load or routine upsert of ENVO | `envo` | `meticulous` | `combined` |
| Initial load or routine upsert of UBERON | `uberon` | `meticulous` | `combined` |
| Initial load or routine upsert of PO | `po` | `meticulous` | `combined` |
| Replace NCBITaxon with a prepared snapshot | `ncbitaxon` | `fast-initial`, after explicitly replacing old contents | `isa` |

`isa` emits `entailed_isa_closure`. NCBITaxon has no `part_of` edges in the tested snapshot.
`combined` emits `entailed_isa_partof_closure`, preserving ancestry through both
`rdfs:subClassOf` and `BFO:0000050` for ENVO, UBERON, and PO. Direct relationships are
also written. Choosing `all` produces multiple closure predicates; it is not necessary
for these configurations.

On the streaming branch, single-predicate ancestry queries avoid SQLite's temporary
deduplication B-tree. Multi-predicate queries retain `DISTINCT` to eliminate duplicate
pairs reached through different predicates. The single-predicate memory measurement below
does not establish a memory bound for `combined` or `all` on other large ontologies.

## Initial load on a local MongoDB

Set the host, port, username, password, and database explicitly. In particular, the loader's
default port is 27022 and its default database is `nmdc`; neither should be assumed correct
for a local experiment. Supply the password through your credential manager and environment,
not a committed file or command-line connection string.

After confirming that `ontology_ncbitaxon_trial` is a new, unused database:

```bash
export MONGO_HOST=localhost
export MONGO_PORT=27017
export MONGO_USERNAME=admin
export MONGO_DB=ontology_ncbitaxon_trial
# Set MONGO_PASSWORD securely before running the loader.
poetry run ontology_loader --source-ontology ncbitaxon --mode fast-initial --closure isa
```

The branch streams documents into batches of 5,000 by default. Classes have `relations=[]`;
read complete relation documents from `ontology_relation_set`. This is an intentional
fast-initial behavior change for review in PR 75. Meticulous mode still embeds relations
inside classes and writes TSV reports.

For ENVO, UBERON, and PO, a single invocation can process them sequentially with the same
mode and closure:

```bash
poetry run ontology_loader --source-ontology envo --source-ontology uberon \
  --source-ontology po --mode meticulous --closure combined \
  --report-directory ./ontology-reports
```

Each ontology gets its own report subdirectory in a multi-ontology invocation. Use a separate
invocation for NCBITaxon because its mode and closure differ.

## Retry, update, and replace are different operations

**Retry a fixed snapshot.** `fast-initial` skips existing class IDs and relation triples and
inserts missing ones. A retry can finish a partially completed load without duplicating rows.
It does not update an existing class label or definition. If the input changes between runs,
the result can mix snapshots and retain old relations.

**Upsert a new snapshot.** `meticulous` updates and inserts class documents and relation triples,
handles explicitly obsolete classes, and writes reports. It does not generally delete a relation
merely because that relation disappeared from the new source. Use this as an upsert workflow,
not a guarantee that MongoDB exactly mirrors every source snapshot.

**Replace a snapshot.** The loader does not pre-clear collections. The runtime branch in
https://github.com/microbiomedata/nmdc-runtime/pull/1562 supplies a manual scoped delete followed
by a fast-initial reload. That sequence exposes missing or partial data until it finishes;
it is not an atomic replacement. NCBITaxon prefix separation has been checked, but the same
assumption must be verified before applying a prefix-based replacement to another ontology
with references crossing ontology boundaries.

For a local trial, loading into a new database and verifying it before choosing it as the
replacement preserves the previous snapshot. Reader switching is an operator step, not a
feature of the current loader. Do not delete the completed reference database merely to test a retry.

## Source freshness is currently coupled to insertion mode

The controller sets `force_refresh=True` for `meticulous` and `False` for `fast-initial`.
Meticulous removes the ontology's existing cache directory and downloads the source again.
Fast-initial reuses an existing cached source and downloads only when it is missing.
The public controller and CLI do not currently provide an independent refresh option.

Consequently, deleting MongoDB documents and launching fast-initial can rebuild the same old
snapshot. Preparing a newer snapshot must happen before destructive replacement begins.
The follow-up in https://github.com/microbiomedata/ontology-loader/issues/13 should separate
source acquisition from insertion mode and record source identity, such as an artifact URL,
checksum, and acquisition time. This is proposed work, not an available CLI flag.
Runtime ordering and failure handling are tracked in
https://github.com/microbiomedata/nmdc-runtime/issues/1565.

## Reference NCBITaxon runs, September 2026

Two complete loads on the same machine, an Apple M5 with a local MongoDB, from a
previously cached NCBITaxon semsql build (release 2025-12-03). Both excluded the
proper descendants of `NCBITaxon:6656` (Arthropoda) and wrote identical document
counts. They differ only in what had merged to `main` in between.

| | before is_root by SQL | after |
|---|---:|---:|
| class documents | 1,742,956 | 1,742,956 |
| relation documents | 27,958,594 | 27,958,594 |
| on disk, both collections | 2.12 GB | 2.12 GB |
| wall time | 30.7 min | 29.0 min |
| peak RSS | 6.22 GB | 7.01 GB |

For contrast, a full load with no exclusion is 2,708,804 classes, 54,700,052
relations, and **4.34 GB** on disk, of which 2.79 GB is indexes.

**Peak memory is not explained.** Removing a call measured at 3.71 GB should have
lowered the peak and it rose. Peak is a high-water mark rather than a sum, and
nobody has attributed it to a phase, which means nobody can say how much headroom
the production worker's 12 GiB limit actually leaves. Tracked at
https://github.com/microbiomedata/ontology-loader/issues/87. **Do not quote a
memory figure from this table as if it were settled.**

Wall times are from a laptop and do not transfer to the production worker. Disk
figures should. Neither is an SLA.
