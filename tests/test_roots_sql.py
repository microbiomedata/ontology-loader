"""Exercise SQL roots against oaklib using small, local semsql databases."""

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from oaklib import get_adapter

from ontology_loader.ontology_processor import OntologyProcessor


@pytest.fixture
def roots_database(tmp_path: Path) -> Path:
    """Create an empty semsql database without downloading an ontology."""
    path = tmp_path / "roots.db"
    schema = Path(__file__).with_name("input") / "roots_schema.sql"
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(schema.read_text())
    return path


def add_statements(path: Path, triples: list[tuple[str, str, str]]) -> None:
    """Insert triples into the fixture's statements table."""
    with closing(sqlite3.connect(path)) as connection:
        connection.executemany(
            "INSERT INTO statements (stanza, subject, predicate, object) VALUES (?, ?, ?, ?)",
            [(subject, subject, predicate, obj) for subject, predicate, obj in triples],
        )
        connection.commit()


def sql_roots(path: Path) -> set[str]:
    """Run the production query without the constructor's download step."""
    processor = OntologyProcessor.__new__(OntologyProcessor)
    processor.ontology_db_path = path
    return processor._roots_from_statements()


@pytest.mark.parametrize(
    "predicate",
    ["owl:equivalentClass", "rdf:type", "TEST:property", "rdfs:domain", "rdfs:range", "owl:inverseOf", "owl:hasValue"],
)
@pytest.mark.parametrize("target", ["TEST:parent", "TEST:child", "owl:Thing", "_:blank"])
def test_relationship_sources(roots_database: Path, predicate: str, target: str) -> None:
    """All relationship sources apply the same self, Thing and blank filters."""
    triples = [(term, "rdf:type", "owl:Class") for term in ("TEST:child", "TEST:parent", "_:blank")]
    triples.append(("TEST:property", "rdf:type", "owl:ObjectProperty"))
    if predicate == "owl:hasValue":
        triples.extend(
            [
                ("TEST:child", "rdfs:subClassOf", "_:restriction"),
                ("_:restriction", "owl:onProperty", "TEST:property"),
                ("_:restriction", "owl:hasValue", target),
            ]
        )
    else:
        triples.append(("TEST:child", predicate, target))
    if predicate in {"rdfs:domain", "rdfs:range", "owl:inverseOf"}:
        triples.append(("TEST:child", "rdf:type", "owl:ObjectProperty"))
    add_statements(roots_database, triples)
    expected = {"TEST:parent"} if target == "TEST:parent" else {"TEST:parent", "TEST:child"}
    adapter = get_adapter(f"sqlite:{roots_database}")
    assert set(adapter.roots()) == expected
    assert sql_roots(roots_database) == expected
    with closing(sqlite3.connect(roots_database)) as connection:
        # rdf:type is already in the semsql edge view; the other cases are not.
        if predicate != "rdf:type":
            assert connection.execute("SELECT COUNT(*) FROM edge WHERE subject = 'TEST:child'").fetchone()[0] == 0
    adapter.session.close()


@pytest.mark.parametrize("declare_meta_class", [False, True])
def test_rdf_type_meta_class(roots_database: Path, declare_meta_class: bool) -> None:
    """The non-index roots path does not suppress OWL meta-class objects."""
    triples = [("TEST:root", "rdf:type", "owl:Class")]
    if declare_meta_class:
        triples.extend([("owl:Class", "rdf:type", "owl:Class"), ("owl:Class", "rdfs:subClassOf", "TEST:root")])
    add_statements(roots_database, triples)
    adapter = get_adapter(f"sqlite:{roots_database}")
    adapter.precompute_lookups()
    expected = set() if declare_meta_class else {"TEST:root"}
    assert set(adapter.roots()) == expected
    assert sql_roots(roots_database) == expected
    adapter.session.close()
