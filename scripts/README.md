# SparrowDB Real-World Testing Tools

Three scripts for importing real graph data into SparrowDB, comparing query
results with Neo4j, and visualising the stored graph.

---

## Prerequisites

Build the Python bindings and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
maturin develop -m crates/sparrowdb-python/Cargo.toml
pip install tqdm neo4j            # both optional — see notes below
```

`tqdm` is optional: without it the scripts print progress every 1 000 records.
`neo4j` is optional: without it `shadow_runner.py` falls back to sparrow-only
mode automatically.

---

## Tool 1 — `neo4j_import.py`

Import a Neo4j dump into SparrowDB.

### Supported formats

| Format | Extension | Source |
|--------|-----------|--------|
| Cypher statements | `.cypher` / `.cql` | `neo4j-admin dump`, APOC export, hand-written |
| JSON-lines | `.jsonl` / `.ndjson` | APOC `apoc.export.json.all` |
| CSV | `.csv` | `neo4j-admin import` headers |

### Usage

```bash
# Basic import
python scripts/neo4j_import.py --input dump.cypher --db /tmp/mydb

# Dry-run: parse and count without writing
python scripts/neo4j_import.py --input dump.cypher --db /tmp/mydb --dry-run

# Import and validate counts via MATCH queries
python scripts/neo4j_import.py --input dump.cypher --db /tmp/mydb --validate

# JSONL format (auto-detected)
python scripts/neo4j_import.py --input export.jsonl --db /tmp/mydb
```

### Cypher dump format expected

```cypher
// Comments are skipped
CREATE (:Person {name: 'Alice', age: 30});
CREATE (:Topic {title: 'Graph Databases'});
CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'});
```

### JSONL format expected

```jsonl
{"type":"node","labels":["Person"],"properties":{"name":"Alice","age":30}}
{"type":"relationship","label":"KNOWS","start":{"labels":["Person"],"properties":{"name":"Alice"}},"end":{"labels":["Person"],"properties":{"name":"Bob"}}}
```

### Known limitations

- **Two-pass import**: nodes are written first, edges second.  Edge statements
  use `MATCH … CREATE (a)-[:REL]->(b)` so nodes must already exist.
- **No upsert / deduplication**: running the importer twice on the same database
  will create duplicate nodes.  Use `--dry-run` to check before importing.
- **Inline string encoding**: SparrowDB stores strings ≤ 8 bytes as packed u64
  values.  The import script handles this transparently.
- **Relationship type catalog** (SPA-167): `MATCH (a)-[:KNOWS]->(b)` queries
  require the relationship type to be registered in the catalog.  This happens
  automatically on `CREATE … -[:KNOWS]->` statements when the SPA-167 fix is
  compiled in.  If you see `unknown relationship type` errors after import,
  ensure your build includes commit `d9d2a63`.

---

## Tool 2 — `shadow_runner.py`

Run the same Cypher queries against both Neo4j and SparrowDB, compare results
row-by-row.

### Usage

```bash
# Single query (requires a running Neo4j instance)
python scripts/shadow_runner.py \
    --neo4j bolt://localhost:7687 \
    --neo4j-user neo4j --neo4j-pass secret \
    --db /tmp/mydb \
    --query "MATCH (n:Person) RETURN n.name, n.age"

# File of queries (one per line, # comments supported)
python scripts/shadow_runner.py \
    --neo4j bolt://localhost:7687 \
    --db /tmp/mydb \
    --queries queries.txt \
    --report shadow_report.json

# SparrowDB-only (no Neo4j required)
python scripts/shadow_runner.py \
    --db /tmp/mydb \
    --query "MATCH (n:Person) RETURN n.name" \
    --sparrow-only
```

### Result comparison

Results are compared as **bags** (multisets) of row dicts.  Column name
prefixes are stripped (`n.name` → `name`), and integer values that look like
packed inline strings are decoded before comparison.

### Query file format

```text
# This is a comment — skipped
MATCH (n:Person) RETURN n.name, n.age
MATCH (n:Topic) RETURN n.title
```

### JSON report format

```json
[
  {
    "query": "MATCH (n:Person) RETURN n.name, n.age",
    "match": true,
    "sparrow_rows": 3,
    "neo4j_rows": 3,
    "sparrow_time_ms": 1.2,
    "neo4j_time_ms": 4.7,
    "comparison": {"match": true, "rows": 3}
  }
]
```

### Exit code

`0` if all queries match (or if Neo4j is skipped).  `1` if any queries produce
different results.

---

## Tool 3 — `visualize.py`

Export a SparrowDB graph to DOT format for Graphviz.

### Usage

```bash
# Export to graph.dot
python scripts/visualize.py --db /tmp/mydb

# Limit node count and filter by label
python scripts/visualize.py --db /tmp/mydb --label Person --limit 100

# Specify output file and layout engine
python scripts/visualize.py --db /tmp/mydb --output out.dot --format neato

# Render to SVG automatically (requires graphviz in PATH)
python scripts/visualize.py --db /tmp/mydb --render

# Then render manually
dot -Tsvg graph.dot > graph.svg
dot -Tpng graph.dot > graph.png
open graph.svg
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--db` | required | SparrowDB database path |
| `--output` | `graph.dot` | Output file |
| `--limit` | 500 | Max nodes to export |
| `--label` | all | Only export this label |
| `--labels` | auto-detect | Comma-separated labels to probe |
| `--rel-types` | built-in list | Comma-separated relationship types to probe |
| `--format` | `dot` | Graphviz layout engine |
| `--render` | false | Auto-render to SVG |

### DOT output example

```dot
digraph SparrowDB {
  graph [layout=dot overlap=false fontname="Helvetica"];
  n0 [label=<<TABLE ...><TR><TD>Person</TD></TR><TR><TD>Alice</TD></TR></TABLE>>
       style=filled fillcolor="#4e79a7" fontcolor="white"];
  n0 -> n1 [label="KNOWS"];
}
```

Nodes use HTML-like table labels showing the label name and up to 8 properties.
Each label gets a distinct fill colour.  A legend subgraph is included.

---

## Test — `test_import_roundtrip.py`

End-to-end test covering all three tools.

```bash
python scripts/test_import_roundtrip.py
```

Runs 28 assertions across 8 test sections:

1. Raw SparrowDB API smoke test
2. Cypher dump import (dry-run + real)
3. JSONL dump import
4. Shadow runner in sparrow-only mode
5. DOT visualisation
6. Inline-string u64 decoding
7. Cypher property parser
8. Cypher line classifier

Exit code `0` on full pass, `1` on any failure.

### Known warnings

The test warns (but does not fail) if `MATCH (a)-[:KNOWS]->(b)` raises
`unknown relationship type`.  This is a merge regression: the SPA-167 fix
(`d9d2a63`) landed on `main` before the `feature/spa-130-with-clause` branch
was merged, and the `MatchCreate` arm in `engine.rs` was overwritten with a
stub.  Edge *creation* works; only the catalog registration step is missing.
The fix is tracked in SPA-167.

---

## Property encoding note

SparrowDB stores string properties as raw u64 inline values (up to 8 bytes,
little-endian packed).  When queried via the Python bindings, these come back
as Python `int`.  All three tools include a `_decode_value` helper that
transparently converts them back to strings for display and comparison
purposes.

Integer properties (e.g. `age: 30`) are stored as `int64` and return as
Python `int` unchanged.
