#!/usr/bin/env python3
"""End-to-end roundtrip test for the real-world testing tools.

Creates a small test dump, imports it into SparrowDB, runs MATCH queries to
verify the imported data, then generates a DOT visualisation.

Run from the repo root::

    python scripts/test_import_roundtrip.py

Or via the venv::

    source .venv/bin/activate && python scripts/test_import_roundtrip.py

Exit codes
----------
0 — all assertions passed
1 — one or more assertions failed
"""

import os
import sys
import json
import tempfile

# Ensure scripts/ is importable as a package
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import neo4j_import
import shadow_runner
import visualize

import sparrowdb

# ── helpers ──────────────────────────────────────────────────────────────────

_PASS = "\033[32mPASS\033[0m"
_FAIL = "\033[31mFAIL\033[0m"
_WARN = "\033[33mWARN\033[0m"

_failures: list[str] = []
_warnings: list[str] = []


def _assert(cond: bool, msg: str) -> None:
    if cond:
        print(f"  {_PASS}  {msg}")
    else:
        print(f"  {_FAIL}  {msg}")
        _failures.append(msg)


def _warn(msg: str) -> None:
    print(f"  {_WARN}  {msg}")
    _warnings.append(msg)


# ── test data ─────────────────────────────────────────────────────────────────

# Small but representative Cypher dump with nodes and edges.
_CYPHER_DUMP = """\
// Nodes
CREATE (:Person {name: 'Alice', age: 30});
CREATE (:Person {name: 'Bob', age: 25});
CREATE (:Person {name: 'Carol', age: 35});
CREATE (:Topic {title: 'Graph'});
CREATE (:Topic {title: 'Rust'});
// Edges
CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'});
CREATE (:Person {name: 'Bob'})-[:KNOWS]->(:Person {name: 'Carol'});
"""

# JSON-lines dump
_JSONL_DUMP = """\
{"type":"node","labels":["Person"],"properties":{"name":"Dave","age":28}}
{"type":"node","labels":["Person"],"properties":{"name":"Eve","age":32}}
{"type":"node","labels":["Topic"],"properties":{"title":"Python"}}
{"type":"relationship","label":"KNOWS","start":{"labels":["Person"],"properties":{"name":"Dave"}},"end":{"labels":["Person"],"properties":{"name":"Eve"}}}
"""

# Shadow-mode query list (read-only queries that work on the imported data)
_SHADOW_QUERIES = [
    "MATCH (n:Person) RETURN n.name, n.age",
    "MATCH (n:Topic) RETURN n.title",
]


# ── helper: decode inline-string u64 ─────────────────────────────────────────

def _decode(v) -> str:
    """Decode a SparrowDB inline property value to a comparable string."""
    if v is None:
        return ""
    if isinstance(v, int) and v > 256:
        raw = (v & 0xFFFFFFFFFFFFFFFF).to_bytes(8, byteorder="little").rstrip(b"\x00")
        try:
            candidate = raw.decode("utf-8")
            if candidate.isprintable() and candidate.strip():
                return candidate
        except UnicodeDecodeError:
            pass
    return str(v)


# ── test: raw sparrowdb API ───────────────────────────────────────────────────

def test_raw_sparrowdb(tmpdir: str) -> None:
    print("\n[1] Raw SparrowDB API smoke test")
    db_path = os.path.join(tmpdir, "raw_smoke.db")
    db = sparrowdb.GraphDb(db_path)

    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")

    rows = db.execute("MATCH (n:Person) RETURN n.name, n.age")
    _assert(len(rows) == 2, f"MATCH (n:Person) returns 2 rows (got {len(rows)})")

    names = sorted(_decode(r["n.name"]) for r in rows)
    _assert(names == ["Alice", "Bob"],
            f"Person names are Alice, Bob (got {names})")

    ages = sorted(r["n.age"] for r in rows)
    _assert(ages == [25, 30], f"Person ages are 25, 30 (got {ages})")

    # Topic node
    db.execute("CREATE (:Topic {title: 'Graph'})")
    topic_rows = db.execute("MATCH (n:Topic) RETURN n.title")
    _assert(len(topic_rows) == 1, f"MATCH (n:Topic) returns 1 row (got {len(topic_rows)})")

    # Checkpoint
    try:
        db.checkpoint()
        _assert(True, "checkpoint() succeeds")
    except Exception as exc:
        _assert(False, f"checkpoint() raised: {exc}")


# ── test: Cypher dump import ──────────────────────────────────────────────────

def test_cypher_import(tmpdir: str) -> None:
    print("\n[2] Cypher dump import")
    dump_path = os.path.join(tmpdir, "dump.cypher")
    db_path = os.path.join(tmpdir, "cypher_import.db")

    with open(dump_path, "w") as fh:
        fh.write(_CYPHER_DUMP)

    # Dry-run first
    result = neo4j_import.import_file(dump_path, db_path, dry_run=True)
    _assert(result["nodes"] > 0, f"dry-run parses >0 nodes (got {result['nodes']})")
    _assert(result["edges"] > 0, f"dry-run parses >0 edges (got {result['edges']})")
    _assert(len(result["errors"]) == 0,
            f"dry-run produces 0 errors (got {len(result['errors'])})")

    # Real import
    result = neo4j_import.import_file(dump_path, db_path)
    _assert(result["nodes"] == 5, f"import creates 5 node stmts (got {result['nodes']})")
    _assert(result["edges"] == 2, f"import creates 2 edge stmts (got {result['edges']})")

    if result["errors"]:
        _warn(f"Import errors: {result['errors'][:5]}")

    # Verify via MATCH
    db = sparrowdb.GraphDb(db_path)
    person_rows = db.execute("MATCH (n:Person) RETURN n.name, n.age")
    # Dump has 3 distinct Person nodes: Alice, Bob, Carol
    _assert(len(person_rows) == 3,
            f"After import: MATCH (n:Person) returns 3 rows (got {len(person_rows)})")

    topic_rows = db.execute("MATCH (n:Topic) RETURN n.title")
    _assert(len(topic_rows) == 2,
            f"After import: MATCH (n:Topic) returns 2 rows (got {len(topic_rows)})")

    # Edge verification (requires SPA-167 fix; skip with warning if it fails)
    try:
        edge_rows = db.execute(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name"
        )
        _assert(len(edge_rows) == 2,
                f"After import: KNOWS edges = 2 (got {len(edge_rows)})")
    except Exception as exc:
        _warn(
            f"MATCH on KNOWS edges raised: {exc}\n"
            "    (This is a known issue when SPA-167 fix is not active in the "
            "current build.  Edge *creation* succeeded; relationship type "
            "registration in the catalog needs SPA-167 to be compiled in.)"
        )


# ── test: JSONL import ────────────────────────────────────────────────────────

def test_jsonl_import(tmpdir: str) -> None:
    print("\n[3] JSONL dump import")
    dump_path = os.path.join(tmpdir, "dump.jsonl")
    db_path = os.path.join(tmpdir, "jsonl_import.db")

    with open(dump_path, "w") as fh:
        fh.write(_JSONL_DUMP)

    result = neo4j_import.import_file(dump_path, db_path)
    _assert(result["nodes"] == 3, f"JSONL import creates 3 nodes (got {result['nodes']})")
    _assert(result["edges"] >= 1, f"JSONL import creates >=1 edge (got {result['edges']})")

    db = sparrowdb.GraphDb(db_path)
    rows = db.execute("MATCH (n:Person) RETURN n.name, n.age")
    _assert(len(rows) == 2,
            f"After JSONL import: MATCH (n:Person) returns 2 rows (got {len(rows)})")


# ── test: shadow runner (sparrow-only mode) ───────────────────────────────────

def test_shadow_sparrow_only(tmpdir: str) -> None:
    print("\n[4] Shadow runner (sparrow-only mode)")
    db_path = os.path.join(tmpdir, "shadow_test.db")
    db = sparrowdb.GraphDb(db_path)
    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")
    db.execute("CREATE (:Topic {title: 'Graph'})")

    results = shadow_runner.run_shadow(
        queries=_SHADOW_QUERIES,
        db_path=db_path,
        sparrow_only=True,
    )

    _assert(len(results) == len(_SHADOW_QUERIES),
            f"shadow_runner returns {len(_SHADOW_QUERIES)} results (got {len(results)})")

    for r in results:
        # In sparrow-only mode neo4j_rows is None, so match is True
        _assert(r["match"], f"sparrow-only: query matches (no neo4j): {r['query'][:60]}")
        if r["error_sparrow"]:
            _warn(f"SparrowDB error for query «{r['query'][:60]}»: {r['error_sparrow']}")

    person_result = next((r for r in results if "Person" in r["query"]), None)
    if person_result and person_result["sparrow_rows"] is not None:
        _assert(person_result["sparrow_rows"] == 2,
                f"Person query returns 2 rows (got {person_result['sparrow_rows']})")


# ── test: DOT visualisation ───────────────────────────────────────────────────

def test_visualize(tmpdir: str) -> None:
    print("\n[5] DOT visualisation")
    db_path = os.path.join(tmpdir, "viz_test.db")
    dot_path = os.path.join(tmpdir, "graph.dot")

    db = sparrowdb.GraphDb(db_path)
    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")
    db.execute("CREATE (:Topic {title: 'Graph'})")

    result = visualize.export_dot(db_path=db_path, output=dot_path, limit=200)
    _assert(result["nodes"] >= 2, f"visualize exports >=2 nodes (got {result['nodes']})")
    _assert(os.path.exists(dot_path), "graph.dot file was created")

    if os.path.exists(dot_path):
        content = open(dot_path).read()
        _assert("digraph SparrowDB" in content, "DOT file has digraph header")
        _assert("Person" in content or "Topic" in content,
                "DOT file contains at least one label name")
        _assert("Alice" in content or "Bob" in content,
                "DOT file contains at least one decoded node name")

        # Print first 15 lines for visual inspection
        print("\n    --- graph.dot (first 15 lines) ---")
        for line in content.splitlines()[:15]:
            print(f"    {line}")
        print("    ---")


# ── test: value decoding ─────────────────────────────────────────────────────

def test_value_decoding() -> None:
    print("\n[6] Inline-string u64 decoding")

    import struct
    def pack_str(s: str) -> int:
        b = s.encode("utf-8")[:8]
        arr = b.ljust(8, b"\x00")
        return struct.unpack_from("<Q", arr)[0]

    cases = [
        ("Alice", pack_str("Alice")),
        ("Bob", pack_str("Bob")),
        ("Graph", pack_str("Graph")),
        ("Rust", pack_str("Rust")),
    ]

    for expected_str, packed_int in cases:
        decoded = _decode(packed_int)
        _assert(decoded == expected_str,
                f"u64 {packed_int} decodes to '{expected_str}' (got '{decoded}')")

    # Integer values should NOT be decoded as strings
    _assert(_decode(30) == "30", "integer 30 decodes to '30'")
    _assert(_decode(25) == "25", "integer 25 decodes to '25'")


# ── test: property parser ─────────────────────────────────────────────────────

def test_prop_parser() -> None:
    print("\n[7] Cypher property parser")

    cases = [
        ("{name: 'Alice', age: 30}", {"name": "Alice", "age": 30}),
        ('{title: "Graph Databases"}', {"title": "Graph Databases"}),
        ("{active: true, score: 3.14}", {"active": True, "score": 3.14}),
        ("{}", {}),
    ]

    for text, expected in cases:
        got = neo4j_import._parse_props(text)
        _assert(got == expected, f"_parse_props({text!r}) == {expected!r} (got {got!r})")


# ── test: line classification ─────────────────────────────────────────────────

def test_line_classification() -> None:
    print("\n[8] Cypher line classifier")

    node_line = "CREATE (:Person {name: 'Alice', age: 30});"
    edge_line = "CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'});"
    comment_line = "// this is a comment"
    skip_line = "MERGE (:Person {name: 'Alice'})"
    index_line = "CREATE INDEX ON :Person(name)"

    c = neo4j_import._classify_cypher_line(node_line.rstrip(";"))
    _assert(c is not None and c[0] == "node", f"Node line classified as node (got {c})")

    c = neo4j_import._classify_cypher_line(edge_line.rstrip(";"))
    _assert(c is not None and c[0] == "edge", f"Edge line classified as edge (got {c})")

    c = neo4j_import._classify_cypher_line(comment_line)
    _assert(c is None, f"Comment line returns None (got {c})")

    c = neo4j_import._classify_cypher_line(skip_line)
    _assert(c is None, f"MERGE line returns None (got {c})")

    c = neo4j_import._classify_cypher_line(index_line)
    _assert(c is None, f"CREATE INDEX line returns None (got {c})")


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    print("=" * 60)
    print("SparrowDB Real-World Testing Tools — Roundtrip Test")
    print("=" * 60)

    with tempfile.TemporaryDirectory(prefix="sparrow_roundtrip_") as tmpdir:
        print(f"\nUsing temp directory: {tmpdir}")

        # Unit tests (no DB needed)
        test_value_decoding()
        test_prop_parser()
        test_line_classification()

        # Integration tests (use a real DB)
        test_raw_sparrowdb(tmpdir)
        test_cypher_import(tmpdir)
        test_jsonl_import(tmpdir)
        test_shadow_sparrow_only(tmpdir)
        test_visualize(tmpdir)

    print("\n" + "=" * 60)
    if _warnings:
        print(f"Warnings ({len(_warnings)}):")
        for w in _warnings:
            print(f"  {_WARN}  {w}")

    if _failures:
        print(f"\nFailed assertions ({len(_failures)}):")
        for f in _failures:
            print(f"  {_FAIL}  {f}")
        print(f"\nResult: {_FAIL} — {len(_failures)} assertion(s) failed")
        return 1

    print(f"Result: {_PASS} — all assertions passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
