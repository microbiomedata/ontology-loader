"""Test OntologyProcessor class and its methods."""

import itertools
import sqlite3
import tracemalloc
import weakref
from collections.abc import Iterator
from contextlib import closing
from dataclasses import asdict

import pytest
from linkml_runtime.dumpers import json_dumper

from src.ontology_loader.ontology_processor import OntologyProcessor


@pytest.mark.parametrize(
    "ontology_name, entity_id, expected",
    [
        # Same-case prefixes (the historical path)
        ("envo", "ENVO:00002005", True),
        ("envo", "envo:00002005", True),
        ("uberon", "UBERON:0000001", True),
        ("po", "PO:0000001", True),
        # Mixed-case prefixes that the prior `.upper()`-based filter dropped silently
        ("ncbitaxon", "NCBITaxon:9606", True),
        ("ncbitaxon", "NCBITAXON:9606", True),
        ("ncbitaxon", "ncbitaxon:9606", True),
        ("chebi", "CHEBI:12345", True),
        # Wrong ontology — must reject
        ("envo", "UBERON:0000001", False),
        ("ncbitaxon", "PR:Q9606", False),
        # Missing colon — must reject
        ("ncbitaxon", "NCBITaxon", False),
        ("envo", "ENVO", False),
        ("envo", "", False),
    ],
)
def test_matches_ontology(ontology_name, entity_id, expected):
    """`_matches_ontology` compares the CURIE head case-insensitively to the configured ontology."""

    # Avoid the heavy `OntologyProcessor.__init__` (which downloads + opens sqlite); the method
    # only depends on `self._ontology_lc`, so a minimal stand-in object is sufficient.
    class _Fake:
        pass

    fake = _Fake()
    fake._ontology_lc = ontology_name.lower()
    assert OntologyProcessor._matches_ontology(fake, entity_id) is expected


def test_ontology_processor():
    """Test OntologyProcessor initialization and ontology retrieval."""
    ontology_name = "envo"
    processor = OntologyProcessor(ontology_name, force_refresh=False)

    assert processor.ontology == ontology_name
    assert processor.ontology_db_path.exists()


def test_get_terms_and_metadata():
    """Test retrieval of ontology terms and metadata."""
    processor = OntologyProcessor("envo", force_refresh=False)
    ontology_classes = processor.get_terms_and_metadata()

    assert isinstance(ontology_classes, list)
    for ontology_class in ontology_classes:
        assert "id" in ontology_class and "type" in ontology_class
        assert ontology_class["type"] == "nmdc:OntologyClass"


def test_get_relations_closure():
    """Test retrieval of ontology relations closure."""
    processor = OntologyProcessor("envo", force_refresh=False)
    ontology_relations, _ = processor.get_relations_closure()

    assert isinstance(ontology_relations, list)
    assert all(isinstance(rel, dict) for rel in ontology_relations)
    for rel in ontology_relations:
        assert "subject" in rel
        assert "predicate" in rel
        assert "object" in rel


@pytest.mark.parametrize(
    "predicates",
    [
        ["rdfs:subClassOf"],
        # part_of is almost never reflexive in entailed_edge itself, unlike subClassOf; catches a
        # method that relies on the table's own reflexivity instead of unioning self-pairs in.
        ["BFO:0000050"],
        # The production `combined` closure passes both predicates in one call, and DISTINCT is
        # unconditional so a pair reachable via more than one predicate is only emitted once; a
        # single-predicate case can't exercise that cross-predicate dedup.
        ["rdfs:subClassOf", "BFO:0000050"],
    ],
)
def test_ancestry_pairs_from_entailed_edge_matches_adapter_ancestors(predicates):
    """
    The bulk entailed_edge query must match the old per-entity adapter.ancestors() loop exactly.

    See https://github.com/microbiomedata/ontology-loader/issues/18: same result, one bulk query
    against `entailed_edge` instead of one per-entity query against the same table via
    `adapter.ancestors()`. Checked against real envo entities, not mocks: the risk here is a
    semantic mismatch between the two query shapes (e.g. reflexivity, or a predicate not actually
    being entailed), which a mock can't catch because it can't be wrong about what oaklib itself
    does.
    """
    processor = OntologyProcessor("envo", force_refresh=False)

    sample_entities = list(
        itertools.islice(
            (entity for entity in processor.adapter.entities(filter_obsoletes=True) if entity.startswith("ENVO:")),
            20,
        )
    )
    assert len(sample_entities) == 20, "sanity check: envo should have at least 20 non-obsolete classes"
    relevant_entities = set(entity for entity in processor.adapter.entities() if processor._matches_ontology(entity))
    sample_entities_set = set(sample_entities)

    # One bulk query for all sample entities at once -- this is the whole point of the change.
    # Only retain pairs for the sampled subjects: the full closure can be tens of millions of
    # rows at NCBITaxon scale, and this test only needs 20 of them.
    #
    # Kept as a list, not deduplicated into the per-subject sets until after the uniqueness check
    # below: a set would silently swallow a duplicate (subject, object) emission, which is exactly
    # the shape of bug this test needs to catch (an entity whose self-pair entailed_edge already
    # had natively could otherwise be yielded twice, or the same pair reached via two different
    # predicates in the combined case).
    pairs = list(processor._ancestry_pairs_from_entailed_edge(predicates, relevant_entities))
    sample_pairs = [(s, o) for s, o in pairs if s in sample_entities_set]
    assert len(sample_pairs) == len(set(sample_pairs)), "duplicate (subject, object) pair emitted"

    new_ancestors_by_subject = {}
    for subject, obj in sample_pairs:
        new_ancestors_by_subject.setdefault(subject, set()).add(obj)

    for entity in sample_entities:
        old_ancestors = {
            a
            for a in processor.adapter.ancestors(entity, reflexive=True, predicates=predicates)
            if processor._matches_ontology(a)
        }
        new_ancestors = new_ancestors_by_subject.get(entity, set())
        assert new_ancestors == old_ancestors, f"mismatch for {entity} on predicates {predicates}"


def test_ancestry_pairs_from_entailed_edge_excludes_deprecated_subjects():
    """
    A deprecated/obsolete entity must not appear as a *subject* in the results.

    The old per-entity loop's start set came from ``self.adapter.entities()``, which defaults to
    ``filter_obsoletes=True`` and so never iterated a deprecated entity in the first place. A bare
    id-prefix match on ``entailed_edge`` does not know about deprecation, and envo's own
    ``entailed_edge`` does contain rows for deprecated subjects -- so without filtering against the
    real entity set, an obsolete class could leak into the closure as a subject.

    Deliberately does not assert the same for objects: the production method preserves the old
    per-entity loop's asymmetry on purpose (subjects checked against ``relevant_entities``, objects
    only prefix-checked), since the old code only ever ontology-prefix-filtered returned ancestors,
    never deprecation-filtered them. Asserting on both would test stricter behavior than what the
    method actually guarantees.

    Checks *all* obsolete entities, not one arbitrary pick: the first obsolete entity oaklib
    returns for envo has zero rows as a subject in ``entailed_edge`` at all, so a test asserting
    only on that one would pass whether or not the deprecated-subject filter existed; checking all
    of them is what actually exercises the filter.
    """
    processor = OntologyProcessor("envo", force_refresh=False)

    obsolete_entities = [e for e in processor.adapter.obsoletes() if processor._matches_ontology(e)]
    assert obsolete_entities, "sanity check: envo should have at least one obsolete class"

    relevant_entities = set(entity for entity in processor.adapter.entities() if processor._matches_ontology(entity))
    assert not (set(obsolete_entities) & relevant_entities), (
        "sanity check: obsolete entities excluded from relevant_entities"
    )

    pairs = list(processor._ancestry_pairs_from_entailed_edge(["rdfs:subClassOf"], relevant_entities))

    subjects = {s for s, _ in pairs}
    leaked = subjects & set(obsolete_entities)
    assert not leaked, f"obsolete entities leaked in as subjects: {leaked}"


@pytest.mark.parametrize("closure", ["combined", "isa", "partof", "all", "none", ["isa", "partof"]])
def test_streaming_matches_list_api(closure: str | list[str]) -> None:
    """
    Real ENVO streams preserve metadata, relation contents, and meticulous embedding.

    The comparison is deliberately exact rather than order-normalized. `alternative_names` is
    built from a set and its order varies with PYTHONHASHSEED across processes, tracked in
    https://github.com/microbiomedata/ontology-loader/issues/76, but the seed is fixed for the
    life of a process, so both extractions here observe the same order. Verified against seeds
    0, 1, 42, 12345 and 99999: zero differing alias lists in every case. Sorting before comparing
    would hide a real regression in which the streaming API emitted a different order from the
    list API, which is precisely what this test exists to catch.
    """
    processor = OntologyProcessor("envo", force_refresh=False)
    classes = processor.get_terms_and_metadata()
    stream = processor.iter_terms_and_metadata()
    assert isinstance(stream, Iterator)
    assert [asdict(term) for term in stream] == [asdict(term) for term in classes]
    relation_stream = processor.iter_relations_closure(closure)
    assert isinstance(relation_stream, Iterator)
    streamed_relations = list(relation_stream)
    assert streamed_relations
    assert all(term.relations == [] for term in classes)
    relations, updated_classes = processor.get_relations_closure(closure, classes)
    assert isinstance(relations, list)
    assert updated_classes == classes
    assert streamed_relations == relations
    by_subject: dict[str, list[dict]] = {}
    for relation in relations:
        by_subject.setdefault(relation["subject"], []).append(relation)
    for term in classes:
        assert [json_dumper.to_dict(relation) for relation in term.relations] == by_subject.get(term.id, [])


def test_class_stream_releases_consumed_classes() -> None:
    """Consumed ENVO classes are not kept alive by the class generator."""
    processor = OntologyProcessor("envo", force_refresh=False)
    stream = processor.iter_terms_and_metadata()
    try:
        assert isinstance(stream, Iterator)
        references = [weakref.ref(next(stream)) for _ in range(100)]
        assert sum(reference() is not None for reference in references) <= 1
    finally:
        stream.close()
    assert all(reference() is None for reference in references)


@pytest.mark.parametrize("closure", ["isa", "partof"])
def test_relation_stream_does_not_accumulate(closure: str) -> None:
    """
    Ten times as many single-predicate relations must not cause proportional memory growth.

    The relevant_entities CURIE set requires O(number of entities) memory and is legitimate:
    ancestry filtering and reflexive self-pairs need it. Keep the ontology fixed and compare
    Python allocations after N and 10N emitted relations, allowing a factor of two for allocator
    and adapter variation rather than the tenfold growth of relation accumulation. Reset the
    peak at N so startup allocations cannot hide a later peak. tracemalloc measures Python
    allocations, not SQLite's native memory. Multi-predicate closures deliberately retain
    DISTINCT and its temporary B-tree; this test makes no memory claim for those closures.
    """
    processor = OntologyProcessor("envo", force_refresh=False)
    stream = processor.iter_relations_closure(closure)
    n = 1000
    tracemalloc.start()
    try:
        assert isinstance(stream, Iterator)
        assert sum(1 for _ in itertools.islice(stream, n)) == n
        retained_n, _ = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()
        assert sum(1 for _ in itertools.islice(stream, 9 * n)) == 9 * n
        retained_10n, peak_10n = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        stream.close()
    assert retained_10n < 2 * retained_n
    assert peak_10n < 2 * retained_n


@pytest.mark.parametrize(
    "predicates, expects_temp_btree",
    [
        (["rdfs:subClassOf"], False),
        (["BFO:0000050"], False),
        (["rdfs:subClassOf", "BFO:0000050"], True),
    ],
)
def test_ancestry_query_plan_uses_temp_btree_only_for_multiple_predicates(
    predicates: list[str], expects_temp_btree: bool
) -> None:
    """The real ancestry query avoids SQLite deduplication storage for a single predicate."""
    processor = OntologyProcessor("envo", force_refresh=False)
    query, params = processor._ancestry_query(predicates)
    with closing(sqlite3.connect(processor.ontology_db_path)) as connection:
        plan = [row[3] for row in connection.execute("EXPLAIN QUERY PLAN " + query, params)]
    assert any("TEMP B-TREE" in detail for detail in plan) is expects_temp_btree, plan
