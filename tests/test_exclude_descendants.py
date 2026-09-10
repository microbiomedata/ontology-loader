"""Exercise subtree exclusion against a real, local semsql fixture."""

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path

import pytest
from click import Context
from semsql.sqla.semsql import Base
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from ontology_loader.cli import cli
from ontology_loader.ontology_processor import OntologyProcessor


@pytest.fixture
def ontology_path(tmp_path: Path) -> Path:
    """Build two branches, an obsolete descendant, and a part-of boundary crossing."""
    path = tmp_path / "fixture.db"
    engine = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(engine)
    engine.dispose()
    nodes = ["T:top", "T:root", "T:child", "T:leaf", "T:kept", "T:other", "T:old"]
    isa = "rdfs:subClassOf"
    direct = [
        ("T:root", isa, "T:top"),
        ("T:child", isa, "T:root"),
        ("T:leaf", isa, "T:child"),
        ("T:kept", isa, "T:top"),
        ("T:other", isa, "T:kept"),
        ("T:old", isa, "T:root"),
        ("T:kept", "BFO:0000050", "T:child"),
    ]
    closure = (
        set(direct)
        | {(n, isa, n) for n in nodes}
        | {
            ("T:child", isa, "T:top"),
            ("T:leaf", isa, "T:root"),
            ("T:leaf", isa, "T:top"),
            ("T:other", isa, "T:top"),
            ("T:old", isa, "T:top"),
        }
    )
    with closing(sqlite3.connect(path)) as con:
        con.executemany("INSERT INTO node (id) VALUES (?)", [(n,) for n in nodes])
        con.execute("INSERT INTO deprecated_node (id) VALUES ('T:old')")
        con.executemany("INSERT INTO edge (subject,predicate,object) VALUES (?,?,?)", direct)
        con.executemany("INSERT INTO entailed_edge (subject,predicate,object) VALUES (?,?,?)", sorted(closure))
        con.commit()
    return path


@pytest.fixture
def processor_type(ontology_path: Path) -> Iterator[type[OntologyProcessor]]:
    """Use normal processor initialization with a local fixture source and real oaklib."""
    instances = []

    class LocalProcessor(OntologyProcessor):
        """Read an already prepared fixture instead of downloading an ontology."""

        def download_and_prepare_ontology(self) -> Path:
            """Return the fixture source."""
            instances.append(self)
            return ontology_path

    yield LocalProcessor
    for processor in instances:
        processor.adapter.session.close()
        processor.adapter.engine.dispose()


@pytest.mark.parametrize("closure", ["combined", "isa", "partof", "all", "none"])
def test_exclusion(processor_type: type[OntologyProcessor], closure: str) -> None:
    """Both streams exclude descendants and retain the root and its ancestry."""
    full = processor_type("t")
    filtered = processor_type("t", exclude_descendants_of=["T:root"])
    all_classes = {c.id for c in full.iter_terms_and_metadata()}
    kept = {c.id for c in filtered.iter_terms_and_metadata()}
    excluded = filtered.excluded_entities
    assert excluded == {"T:child", "T:leaf", "T:old"}
    assert excluded < all_classes
    assert kept == all_classes - excluded
    assert len(kept) + len(excluded) == len(all_classes)
    assert "T:root" in kept
    before = list(full.iter_relations_closure(closure))
    after = list(filtered.iter_relations_closure(closure))
    assert any(r["subject"] in excluded for r in before)
    if closure != "isa":
        assert any(r["subject"] == "T:kept" and r["object"] == "T:child" for r in before)
    assert after
    assert all(r["subject"] not in excluded and r["object"] not in excluded for r in after)
    expected = [r for r in before if r["subject"] not in excluded and r["object"] not in excluded]

    def key(r: dict) -> tuple[str, str, str]:
        """Order relation dictionaries for comparing different entity sets."""
        return r["subject"], r["predicate"], r["object"]

    assert sorted(after, key=key) == sorted(expected, key=key)
    if closure in {"combined", "isa", "all"}:
        assert any(
            r["subject"] == "T:root" and r["object"] == "T:top" and r["predicate"].startswith("entailed_")
            for r in after
        )
    relations, classes = filtered.get_relations_closure(closure, filtered.get_terms_and_metadata())
    assert relations == after
    assert {c.id for c in classes} == kept
    assert all(r.object not in excluded for c in classes for r in c.relations)
    query, params = filtered._ancestry_query(["rdfs:subClassOf", "BFO:0000050"])
    with closing(filtered._connect_readonly()) as con:
        pairs = list(con.execute(query, params))
    assert pairs
    assert all(s not in excluded and o not in excluded for s, o in pairs)


def test_repeated_roots(processor_type: type[OntologyProcessor]) -> None:
    """Repeated and nested named roots survive; both branches lose descendants."""
    processor = processor_type("t", exclude_descendants_of=["T:root", "T:child", "T:kept", "T:root"])
    assert processor.excluded_entities == {"T:leaf", "T:old", "T:other"}
    assert {c.id for c in processor.iter_terms_and_metadata()} == {"T:top", "T:root", "T:child", "T:kept"}


def test_no_exclusion(processor_type: type[OntologyProcessor]) -> None:
    """Omitted and empty options preserve serialized output and the original SQL."""
    from linkml_runtime.dumpers import json_dumper

    default = processor_type("t")
    empty = processor_type("t", exclude_descendants_of=[])
    assert list(default.iter_terms_and_metadata())
    assert [json_dumper.dumps(c) for c in default.get_terms_and_metadata()] == [
        json_dumper.dumps(c) for c in empty.get_terms_and_metadata()
    ]
    assert list(default.iter_relations_closure()) == list(empty.iter_relations_closure())
    assert default._ancestry_query(["rdfs:subClassOf"]) == (
        "SELECT subject, object FROM entailed_edge WHERE predicate IN (?) AND subject LIKE ? AND object LIKE ?",
        ["rdfs:subClassOf", "t:%", "t:%"],
    )


def test_readonly(processor_type: type[OntologyProcessor], ontology_path: Path) -> None:
    """Both SQLite access paths reject writes and leave the source bytes unchanged."""
    before = hashlib.sha256(ontology_path.read_bytes()).digest()
    processor = processor_type("t", exclude_descendants_of=["T:root"])
    assert processor.excluded_entities
    with closing(processor._connect_readonly()) as con:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            con.execute("CREATE TABLE forbidden (id TEXT)")
    with processor.adapter.engine.connect() as con:
        with pytest.raises(OperationalError, match="readonly"):
            con.exec_driver_sql("CREATE TABLE forbidden (id TEXT)")
    assert list(processor.iter_terms_and_metadata())
    assert list(processor.iter_relations_closure())
    assert hashlib.sha256(ontology_path.read_bytes()).digest() == before


def test_cli_repeatable() -> None:
    """Click parses each exclusion root without connecting to MongoDB."""
    with Context(cli) as context:
        cli.parse_args(
            context,
            ["--source-ontology", "t", "--exclude-descendants-of", "T:root", "--exclude-descendants-of", "T:kept"],
        )
        assert context.params["exclude_descendants_of"] == ("T:root", "T:kept")


@pytest.mark.parametrize("closure", ["combined", "isa", "partof", "all", "none"])
def test_unfiltered_baseline(processor_type: type[OntologyProcessor], closure: str) -> None:
    """Match output captured from b092788 on this fixture, with stable relation ordering."""
    from linkml_runtime.dumpers import json_dumper

    baseline = json.loads((Path(__file__).parent / "fixtures" / "exclusion_unfiltered.json").read_text())
    processor = processor_type("t")
    classes = [json_dumper.dumps(c) for c in processor.iter_terms_and_metadata()]
    relations = sorted(
        processor.iter_relations_closure(closure), key=lambda r: (r["subject"], r["predicate"], r["object"])
    )
    assert baseline["classes"]
    assert baseline["relations"][closure]
    assert classes == baseline["classes"]
    assert relations == baseline["relations"][closure]


def test_scalar_curie_is_one_curie_not_characters() -> None:
    """
    A bare string must mean one CURIE, not one CURIE per character.

    `str` satisfies `Iterable[str]`, so without normalisation a caller passing a
    single CURIE gets it iterated character by character. That fails silently:
    nothing matches, the exclusion set is empty, and the load runs unfiltered.
    """
    from ontology_loader.ontology_processor import normalize_curie_list

    # The failure this guards against is real, so assert the naive form differs.
    assert len(tuple("T:root")) > 1
    assert normalize_curie_list("T:root") == ("T:root",)
    assert normalize_curie_list(["T:root", "T:kept"]) == ("T:root", "T:kept")
    assert normalize_curie_list(["T:root", "T:root"]) == ("T:root",)
    assert normalize_curie_list(()) == ()


def test_cli_forwards_exclusions_all_the_way_to_the_processor(monkeypatch) -> None:
    """
    The values must reach the processor, not merely be parsed by the CLI.

    A test that stops at argument parsing cannot catch a dropped hand-off, because
    the controller fake accepts and ignores keyword arguments, so removing the
    forwarding would leave the suite green and the CLI would silently do a full load.
    """
    from click.testing import CliRunner

    from ontology_loader import cli as cli_module
    from ontology_loader import ontology_load_controller as controller_module

    seen: dict = {}

    class _Recorder:
        def __init__(self, *args, **kwargs):
            seen.update(kwargs)

        def run_ontology_loader(self, *args, **kwargs):
            return None

    monkeypatch.setattr(controller_module, "OntologyProcessor", _Recorder, raising=False)
    monkeypatch.setattr(cli_module, "OntologyLoaderController", _Recorder)

    result = CliRunner().invoke(
        cli_module.cli,
        ["--source-ontology", "t", "--exclude-descendants-of", "T:root", "--exclude-descendants-of", "T:kept"],
    )
    assert result.exit_code == 0, result.output
    assert seen.get("exclude_descendants_of") == ("T:root", "T:kept"), seen


def test_controller_normalizes_a_scalar_curie() -> None:
    """
    Constructing the controller must work and must not split a CURIE.

    Constructed for real rather than mocked: an earlier version of this change
    referenced the normaliser in the controller without importing it, which every
    mock-based test happily passed while the real call site raised NameError.
    """
    from ontology_loader.ontology_load_controller import OntologyLoaderController

    controller = OntologyLoaderController(source_ontology=["envo"], exclude_descendants_of="T:root")
    assert controller.exclude_descendants_of == ("T:root",)

    controller = OntologyLoaderController(source_ontology=["envo"], exclude_descendants_of=["T:a", "T:b", "T:a"])
    assert controller.exclude_descendants_of == ("T:a", "T:b")
