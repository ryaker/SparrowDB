"""
SPA-104: Python binding end-to-end acceptance test (acceptance check #14).

Exercises full Cypher round-trip via the sparrowdb Python API against a real
on-disk database.  All tests use ``tempfile.TemporaryDirectory`` so no
persistent state leaks between runs.

The ``execute()`` method returns a ``list[dict]`` where each dict maps
column-name -> value.
"""

import os
import tempfile

import pytest


def _db():
    """Import sparrowdb.GraphDb, skip if not built."""
    try:
        import sparrowdb  # noqa: F401

        return sparrowdb.GraphDb
    except ImportError:
        pytest.skip(
            "sparrowdb Python binding not built — run `maturin develop` first"
        )


# ── CREATE + MATCH round-trips ────────────────────────────────────────────────


def test_create_and_query_nodes():
    """Basic CREATE + MATCH round-trip returns correct node properties."""
    GraphDb = _db()
    with tempfile.TemporaryDirectory() as tmpdir:
        db = GraphDb(os.path.join(tmpdir, "test.db"))
        db.execute("CREATE (n:Person {name: 'Alice', age: 30})")
        db.execute("CREATE (n:Person {name: 'Bob', age: 25})")
        rows = db.execute("MATCH (n:Person) RETURN n.name ORDER BY n.name")
        names = [r["n.name"] for r in rows]
        assert names == ["Alice", "Bob"], f"Expected ['Alice', 'Bob'], got {names}"


def test_create_and_query_relationships():
    """CREATE with relationship + single-hop traversal returns correct target."""
    GraphDb = _db()
    with tempfile.TemporaryDirectory() as tmpdir:
        db = GraphDb(os.path.join(tmpdir, "test.db"))
        db.execute("CREATE (a:Person {name: 'Alice'})")
        db.execute("CREATE (b:Person {name: 'Bob'})")
        db.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        rows = db.execute(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name"
        )
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        assert rows[0]["b.name"] == "Bob"


# ── Aggregation ───────────────────────────────────────────────────────────────


def test_count_star():
    """COUNT(*) returns the correct total node count."""
    GraphDb = _db()
    with tempfile.TemporaryDirectory() as tmpdir:
        db = GraphDb(os.path.join(tmpdir, "test.db"))
        for name, age in [("Alice", 30), ("Bob", 25), ("Carol", 35)]:
            db.execute(f"CREATE (n:Person {{name: '{name}', age: {age}}})")
        rows = db.execute("MATCH (n:Person) RETURN COUNT(*)")
        assert len(rows) == 1
        # Column name is lowercased by the engine: 'count(*)'
        count = rows[0]["count(*)"]
        assert count == 3, f"Expected count(*)=3, got {count}"


def test_avg_aggregation():
    """AVG(n.age) returns the correct average."""
    GraphDb = _db()
    with tempfile.TemporaryDirectory() as tmpdir:
        db = GraphDb(os.path.join(tmpdir, "test.db"))
        for name, age in [("Alice", 30), ("Bob", 25), ("Carol", 35)]:
            db.execute(f"CREATE (n:Person {{name: '{name}', age: {age}}})")
        rows = db.execute("MATCH (n:Person) RETURN AVG(n.age)")
        assert len(rows) == 1
        avg = list(rows[0].values())[0]
        assert abs(avg - 30.0) < 0.01, f"Expected AVG≈30.0, got {avg}"


# ── WHERE predicate ───────────────────────────────────────────────────────────


def test_where_filter():
    """WHERE n.value > 2 filters to the correct subset."""
    GraphDb = _db()
    with tempfile.TemporaryDirectory() as tmpdir:
        db = GraphDb(os.path.join(tmpdir, "test.db"))
        for i in range(5):
            db.execute(f"CREATE (n:Item {{value: {i}}})")
        rows = db.execute("MATCH (n:Item) WHERE n.value > 2 RETURN n.value")
        values = sorted(r["n.value"] for r in rows)
        assert values == [3, 4], f"Expected [3, 4], got {values}"


# ── Persistence (close and reopen) ────────────────────────────────────────────


def test_persistence_across_reopen():
    """Data written to disk survives db close and reopen."""
    GraphDb = _db()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "persist.db")
        db = GraphDb(db_path)
        db.execute("CREATE (n:Marker {key: 'hello'})")
        del db  # close / flush

        db2 = GraphDb(db_path)
        rows = db2.execute("MATCH (n:Marker) RETURN n.key")
        assert len(rows) == 1, f"Expected 1 row after reopen, got {len(rows)}"
        assert rows[0]["n.key"] == "hello"


# ── ORDER BY / LIMIT / SKIP ───────────────────────────────────────────────────


def test_order_by_limit_skip():
    """ORDER BY + SKIP + LIMIT returns the correct slice."""
    GraphDb = _db()
    with tempfile.TemporaryDirectory() as tmpdir:
        db = GraphDb(os.path.join(tmpdir, "test.db"))
        for i in range(10):
            db.execute(f"CREATE (n:Num {{v: {i}}})")
        rows = db.execute(
            "MATCH (n:Num) RETURN n.v ORDER BY n.v SKIP 3 LIMIT 3"
        )
        values = [r["n.v"] for r in rows]
        assert values == [3, 4, 5], f"Expected [3, 4, 5], got {values}"


# ── Column names ──────────────────────────────────────────────────────────────


def test_column_names_present_in_result():
    """Each row dict contains expected column keys."""
    GraphDb = _db()
    with tempfile.TemporaryDirectory() as tmpdir:
        db = GraphDb(os.path.join(tmpdir, "test.db"))
        db.execute("CREATE (n:Thing {x: 42})")
        rows = db.execute("MATCH (n:Thing) RETURN n.x")
        assert len(rows) == 1
        row = rows[0]
        assert "n.x" in row, f"Expected key 'n.x' in row dict, got keys: {list(row.keys())}"
        assert row["n.x"] == 42
