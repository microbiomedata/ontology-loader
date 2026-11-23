# Loading and updating ontologies

The loader reads an ontology from a SQLite database built by Semantic SQL (semsql, https://github.com/INCATools/semantic-sql) and writes NMDC schema (https://github.com/microbiomedata/nmdc-schema) `OntologyClass` and `OntologyRelation` documents to MongoDB's `ontology_class_set` and `ontology_relation_set`. It does not copy semsql tables into MongoDB. NMDC loads four ontologies this way: NCBITaxon (https://obofoundry.org/ontology/ncbitaxon.html), ENVO (https://obofoundry.org/ontology/envo.html), UBERON (https://obofoundry.org/ontology/uberon.html) and PO (https://obofoundry.org/ontology/po.html).

## Choose a mode and closure

| Operation | `--source-ontology` | `--mode` | `--closure` |
| --- | --- | --- | --- |
| First NCBITaxon load, or a retry of the same snapshot | `ncbitaxon` | `fast-initial` | `isa` |
| Replace NCBITaxon with a new snapshot | `ncbitaxon` | `fast-initial`, after removing the old contents | `isa` |
| First load or routine update of ENVO, UBERON or PO | `envo`, `uberon`, `po` | `meticulous` | `combined` |

The closure setting decides which ancestor relations are written for each class. `isa` writes `entailed_isa_closure`. NCBITaxon has no `part_of` edges in the snapshot tested. `combined` writes `entailed_isa_partof_closure`, which follows both `rdfs:subClassOf` (is a) and `BFO:0000050` (part of). Direct relations are written in every case. `all` writes several closure predicates and isn't needed for these loads.

`--exclude-descendants-of <CURIE>`, repeatable, leaves out the `rdfs:subClassOf` descendants of a term and keeps the term itself. Excluding `NCBITaxon:6656` (Arthropoda) cuts an NCBITaxon load from 4.34 GB to 2.12 GB on disk, because arthropods are 49.6 percent of the ancestry closure. The option filters only the current load. It doesn't delete documents that an earlier load stored, so getting the smaller collections needs an empty target database.

## Load into a local MongoDB

Set the connection explicitly. The loader's default port is 27022 and its default database is `nmdc`, and neither is necessarily right for a local trial. Set `MONGO_PASSWORD` in your environment without writing it into a committed file or onto the command line.

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
- Replace by removing the old contents and then running `fast-initial`. The loader doesn't clear collections itself. https://github.com/microbiomedata/nmdc-runtime/pull/1562 proposes a scoped delete followed by a fast-initial reload. The data is missing or partial until the reload finishes. Check each ontology before deleting its documents by ID prefix, because one ontology's load can include terms from another ontology.

For a local trial, load into a new database and check it before switching readers to it. That keeps the previous snapshot. The loader doesn't switch readers.

## Source freshness depends on the mode

Running in `meticulous` mode deletes the cached source and downloads it again. Running in `fast-initial` mode reuses the cached source and downloads it only when it's missing. There's no separate refresh option, so deleting the MongoDB documents and running `fast-initial` can reload the same old snapshot. To reload a new release with `fast-initial`, first delete the ontology's whole cache directory (by default `~/.pystow/<ontology>`, managed by pystow, https://github.com/cthoyt/pystow), not only the `.db.gz` file, because the loader reuses an already-extracted `.db` file. Then check the new file's `owl:versionIRI` before deleting any MongoDB documents.

## The same ontologies in the KBase lakehouse

The data lakehouse that KBase runs, K-BERDL (https://hub.berdl.kbase.us), already holds NCBITaxon, ENVO, UBERON and PO as semsql tables (`statements`, `entailed_edge` and `prefix`) in `refdata.ontology_source` and `kbase.ontology_source`. The BRIDGE lakehouse will likely get the same tables.

The only NMDC application that reads `ontology_class_set` and `ontology_relation_set` in production is the NMDC data portal (https://github.com/microbiomedata/nmdc-server). During its data load, the portal copies both collections into its own PostgreSQL database and builds the ENVO term tree behind its environment search filters from them (https://github.com/microbiomedata/nmdc-server/blob/835d61e7fc929212a4cd81836bafcd12aeaa1154/nmdc_server/ingest/all.py#L73-L77). If the portal built that tree from the lakehouse tables instead, NMDC would no longer need this loader or the MongoDB copies it writes.

Not yet known: which code loads the K-BERDL tables, and which ontology releases they hold. The tables don't record a release for any of these four ontologies.

## Known gaps

- Nothing records which ontology release a load came from, or whether it was subsetted. A subsetted load and one that failed partway look the same. https://github.com/microbiomedata/ontology-loader/issues/81
- Refreshing the source is tied to the mode. https://github.com/microbiomedata/ontology-loader/issues/13
- Class documents differ by mode: meticulous embeds relations and fast-initial doesn't. https://github.com/microbiomedata/ontology-loader/issues/78
- The deprecated `generate_reports=False` argument now selects fast-initial, which skips updates to existing documents. https://github.com/microbiomedata/ontology-loader/issues/96
- Peak memory isn't explained, so it's unknown how close an NCBITaxon load comes to the 12 GiB memory limit of the nmdc-runtime job that runs the loader. https://github.com/microbiomedata/ontology-loader/issues/87
- How nmdc-runtime (https://github.com/microbiomedata/nmdc-runtime) should order and retry fast-initial reloads isn't settled. https://github.com/microbiomedata/nmdc-runtime/issues/1565

## NCBITaxon size

A full NCBITaxon load writes 2,708,804 class documents and 54,700,052 relation documents, 4.34 GB in MongoDB including indexes. Excluding the descendants of `NCBITaxon:6656` (Arthropoda) brings that to 1,742,956 classes, 27,958,594 relations and 2.12 GB. Both measured September 2026 from NCBITaxon semsql release 2025-12-03.
