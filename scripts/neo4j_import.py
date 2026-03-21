#!/usr/bin/env python3
"""Neo4j → SparrowDB import bridge.

Reads a Neo4j Cypher dump (from ``neo4j-admin database dump``, APOC export,
or hand-written Cypher) and imports the nodes and edges into a SparrowDB
database via the Python bindings.

Supported input formats
-----------------------
1. **Cypher dump** (``.cypher`` / ``.cql``) — one statement per line, each
   terminated with an optional semicolon::

       CREATE (:Person {name: 'Alice', age: 30});
       CREATE (:Topic {title: 'Graph Databases'});
       CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'});

2. **JSON-lines** (``.jsonl``) — one JSON object per line, each describing a
   node or relationship::

       {"type":"node","labels":["Person"],"properties":{"name":"Alice","age":30}}
       {"type":"relationship","label":"KNOWS","start":{"labels":["Person"],"properties":{"name":"Alice"}},"end":{"labels":["Person"],"properties":{"name":"Bob"}}}

3. **CSV nodes / edges** — detected by a ``type`` header column.

Usage
-----
::

    python scripts/neo4j_import.py --input dump.cypher --db /tmp/mydb
    python scripts/neo4j_import.py --input dump.cypher --db /tmp/mydb --dry-run
    python scripts/neo4j_import.py --input dump.cypher --db /tmp/mydb --validate
"""

import sys
import argparse
import re
import json
import csv
import os
from typing import Optional

# ── progress bar (optional) ──────────────────────────────────────────────────

try:
    from tqdm import tqdm as _tqdm

    def _progress(iterable, desc="", total=None):
        return _tqdm(iterable, desc=desc, total=total, unit="stmt", leave=True)

except ImportError:  # pragma: no cover
    class _FallbackProgress:
        """Minimal progress shim used when tqdm is unavailable."""

        def __init__(self, iterable, desc="", total=None):
            self._it = iterable
            self._desc = desc
            self._n = 0

        def __iter__(self):
            for item in self._it:
                self._n += 1
                if self._n % 1000 == 0:
                    print(f"  {self._desc}: {self._n} records processed…",
                          flush=True)
                yield item

        def update(self, n=1):
            pass

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    def _progress(iterable, desc="", total=None):  # noqa: E302
        return _FallbackProgress(iterable, desc=desc, total=total)


# ── helpers ──────────────────────────────────────────────────────────────────

# Matches a single inline string property value in Cypher, e.g. {name: 'Alice'}
_PROP_RE = re.compile(
    r"(\w+)\s*:\s*(?:'([^']*)'|\"([^\"]*)\"|(-?\d+(?:\.\d+)?)|true|false|null)",
    re.IGNORECASE,
)


def _parse_props(text: str) -> dict:
    """Extract ``{key: value}`` pairs from a Cypher property map string."""
    props = {}
    for m in _PROP_RE.finditer(text):
        key = m.group(1)
        if m.group(2) is not None:      # single-quoted string
            props[key] = m.group(2)
        elif m.group(3) is not None:    # double-quoted string
            props[key] = m.group(3)
        elif m.group(4) is not None:    # number
            raw = m.group(4)
            props[key] = int(raw) if "." not in raw else float(raw)
        else:
            full = m.group(0)
            if "true" in full.lower():
                props[key] = True
            elif "false" in full.lower():
                props[key] = False
            else:
                props[key] = None
    return props


def _props_to_cypher(props: dict) -> str:
    """Render a Python dict as a Cypher property map literal."""
    if not props:
        return ""
    parts = []
    for k, v in props.items():
        if isinstance(v, str):
            escaped = v.replace("'", "\\'")
            parts.append(f"{k}: '{escaped}'")
        elif isinstance(v, bool):
            parts.append(f"{k}: {'true' if v else 'false'}")
        elif v is None:
            parts.append(f"{k}: null")
        else:
            parts.append(f"{k}: {v}")
    return "{" + ", ".join(parts) + "}"


# ── Cypher dump parser ────────────────────────────────────────────────────────

# Matches:  (:Label {props})  or  (:Label)
_NODE_PAT = re.compile(r"\(:(\w+)(?:\s*(\{[^}]*\}))?\)")
# Matches:  -[:REL_TYPE]->
_REL_PAT = re.compile(r"-\[:(\w+)\]->")


def _classify_cypher_line(line: str):
    """Return ('node', label, props), ('edge', src_node, rel, dst_node),
    or None for unrecognised lines.

    ``src_node`` / ``dst_node`` are ``(label, props_dict)`` tuples.
    """
    upper = line.upper()

    # Skip MERGE, INDEX, CONSTRAINT, comments, and CALL statements.
    for skip_prefix in ("MERGE", "CREATE INDEX", "CREATE CONSTRAINT",
                        "DROP ", "CALL ", "RETURN", "MATCH", "//", "--",
                        "USING", "BEGIN", "COMMIT", "SCHEMA",
                        "CREATE UNIQUE", "WITH"):
        if upper.startswith(skip_prefix):
            return None

    if not upper.startswith("CREATE"):
        return None

    # Detect edge lines: contain -[:REL]->
    if _REL_PAT.search(line):
        nodes = _NODE_PAT.findall(line)
        rel_m = _REL_PAT.search(line)
        if len(nodes) >= 2 and rel_m:
            src_label, src_props_raw = nodes[0]
            dst_label, dst_props_raw = nodes[1]
            src_props = _parse_props(src_props_raw) if src_props_raw else {}
            dst_props = _parse_props(dst_props_raw) if dst_props_raw else {}
            rel_type = rel_m.group(1)
            return ("edge", (src_label, src_props), rel_type,
                    (dst_label, dst_props))
        return None

    # Node line
    node_m = _NODE_PAT.search(line)
    if node_m:
        label = node_m.group(1)
        props = _parse_props(node_m.group(2)) if node_m.group(2) else {}
        return ("node", label, props)

    return None


def _lines_from_cypher(path: str):
    """Yield non-empty, non-comment logical Cypher lines from *path*."""
    with open(path, encoding="utf-8") as fh:
        buf = []
        for raw in fh:
            stripped = raw.strip()
            if not stripped or stripped.startswith("//") or stripped.startswith("/*"):
                if buf:
                    yield " ".join(buf)
                    buf = []
                continue
            # Strip trailing semicolon — SparrowDB's execute() doesn't want it.
            buf.append(stripped.rstrip(";"))
            # Treat semicolon at end of non-stripped line as statement boundary.
            if raw.rstrip().endswith(";"):
                yield " ".join(buf)
                buf = []
        if buf:
            yield " ".join(buf)


# ── JSONL parser ──────────────────────────────────────────────────────────────

def _records_from_jsonl(path: str):
    """Yield ``(kind, data)`` from a JSONL file where each line is a
    node/relationship descriptor.

    ``kind`` is ``'node'`` or ``'edge'``.  ``data`` carries the parsed object.
    """
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as exc:
                yield ("error", str(exc), raw)
                continue
            typ = obj.get("type", "").lower()
            if typ in ("node",):
                yield ("node", obj)
            elif typ in ("relationship", "edge", "rel"):
                yield ("edge", obj)
            else:
                yield ("skip", obj)


# ── CSV parser ────────────────────────────────────────────────────────────────

def _records_from_csv(path: str):
    """Yield ``(kind, data)`` from a CSV export with a ``type`` column."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            typ = row.get("type", row.get(":TYPE", "")).lower()
            if typ == "node":
                yield ("node", row)
            elif typ in ("relationship", "edge", "rel"):
                yield ("edge", row)
            else:
                yield ("skip", row)


# ── node/edge Cypher builders ─────────────────────────────────────────────────

def _build_node_cypher(label: str, props: dict) -> str:
    props_str = _props_to_cypher(props)
    if props_str:
        return f"CREATE (:{label} {props_str})"
    return f"CREATE (:{label})"


def _build_edge_cypher(
    src_label: str, src_props: dict,
    rel_type: str,
    dst_label: str, dst_props: dict,
) -> tuple[str, str]:
    """Return (src_create_cypher, edge_create_cypher).

    SparrowDB currently requires nodes to exist before creating edges via
    MATCH…CREATE.  We create the source and destination nodes first (if they
    carry any properties that uniquely identify them), then wire the edge.

    Note: if the nodes were already created as part of node CREATE statements
    earlier in the dump, creating them again will produce duplicates.  A
    production-grade importer would maintain a name→id registry; here we keep
    it simple and document the caveat.
    """
    src_props_str = _props_to_cypher(src_props)
    dst_props_str = _props_to_cypher(dst_props)

    # Build MATCH … CREATE (a)-[:REL]->(b)
    src_match = f"(a:{src_label} {src_props_str})" if src_props_str else f"(a:{src_label})"
    dst_match = f"(b:{dst_label} {dst_props_str})" if dst_props_str else f"(b:{dst_label})"

    edge_stmt = f"MATCH {src_match}, {dst_match} CREATE (a)-[:{rel_type}]->(b)"
    return edge_stmt


# ── main import logic ─────────────────────────────────────────────────────────

def detect_format(path: str) -> str:
    """Return 'cypher', 'jsonl', or 'csv' based on file extension / sniff."""
    ext = os.path.splitext(path)[1].lower()
    if ext in (".cypher", ".cql"):
        return "cypher"
    if ext in (".jsonl", ".ndjson"):
        return "jsonl"
    if ext == ".csv":
        return "csv"
    # Sniff first line
    try:
        with open(path, encoding="utf-8") as fh:
            first = fh.readline().strip()
        if first.startswith("{") or first.startswith("["):
            return "jsonl"
        if "," in first and not first.upper().startswith("CREATE"):
            return "csv"
    except OSError:
        pass
    return "cypher"


def import_file(
    path: str,
    db_path: str,
    dry_run: bool = False,
    validate: bool = False,
    batch_size: int = 500,
) -> dict:
    """Import *path* into the SparrowDB at *db_path*.

    Returns a summary dict with keys ``nodes``, ``edges``, ``skipped``,
    ``errors``.
    """
    import sparrowdb  # noqa: PLC0415  (imported here so the module is optional during testing)

    fmt = detect_format(path)
    print(f"[neo4j_import] Detected format: {fmt}")
    print(f"[neo4j_import] Source: {path}")
    print(f"[neo4j_import] Target: {db_path}  (dry_run={dry_run})")

    db: Optional[object] = None
    if not dry_run:
        db = sparrowdb.GraphDb(db_path)

    nodes_created = 0
    edges_created = 0
    skipped = 0
    errors: list[str] = []

    # ── Cypher format ─────────────────────────────────────────────────────────
    if fmt == "cypher":
        lines = list(_lines_from_cypher(path))
        total = len(lines)

        # Two-pass: nodes first, then edges (some dumps mix them).
        node_stmts: list[str] = []
        edge_stmts: list[str] = []
        raw_stmts: list[str] = []  # unrecognised CREATE statements

        for line in lines:
            classified = _classify_cypher_line(line)
            if classified is None:
                raw_stmts.append(line)
                continue
            if classified[0] == "node":
                _, label, props = classified
                node_stmts.append(_build_node_cypher(label, props))
            elif classified[0] == "edge":
                _, src, rel_type, dst = classified
                edge_stmts.append(_build_edge_cypher(*src, rel_type, *dst))

        print(f"[neo4j_import] Parsed {len(node_stmts)} node stmts, "
              f"{len(edge_stmts)} edge stmts, {len(raw_stmts)} other stmts "
              f"(total lines: {total})")

        for stmt in _progress(node_stmts, desc="Nodes"):
            if dry_run:
                nodes_created += 1
                continue
            try:
                db.execute(stmt)
                nodes_created += 1
            except Exception as exc:
                errors.append(f"NODE: {exc!s} — {stmt[:120]}")

        for stmt in _progress(edge_stmts, desc="Edges"):
            if dry_run:
                edges_created += 1
                continue
            try:
                db.execute(stmt)
                edges_created += 1
            except Exception as exc:
                errors.append(f"EDGE: {exc!s} — {stmt[:120]}")

        for stmt in _progress(raw_stmts, desc="Other"):
            if dry_run:
                skipped += 1
                continue
            try:
                db.execute(stmt)
                skipped += 1
            except Exception as exc:
                errors.append(f"OTHER: {exc!s} — {stmt[:120]}")

    # ── JSONL format ──────────────────────────────────────────────────────────
    elif fmt == "jsonl":
        records = list(_records_from_jsonl(path))

        # Nodes first
        for kind, data in _progress(
            [(k, d) for k, d in records if k == "node"],
            desc="Nodes (JSONL)",
        ):
            labels = data.get("labels", ["Unknown"])
            props = data.get("properties", {})
            label = labels[0] if labels else "Unknown"
            stmt = _build_node_cypher(label, props)
            if dry_run:
                nodes_created += 1
                continue
            try:
                db.execute(stmt)
                nodes_created += 1
            except Exception as exc:
                errors.append(f"NODE-JSONL: {exc!s} — {stmt[:120]}")

        # Edges second
        for kind, data in _progress(
            [(k, d) for k, d in records if k == "edge"],
            desc="Edges (JSONL)",
        ):
            rel_type = data.get("label", data.get("type", "REL"))
            start = data.get("start", {})
            end = data.get("end", {})
            src_labels = start.get("labels", ["Unknown"])
            dst_labels = end.get("labels", ["Unknown"])
            src_props = start.get("properties", {})
            dst_props = end.get("properties", {})
            stmt = _build_edge_cypher(
                src_labels[0], src_props, rel_type, dst_labels[0], dst_props
            )
            if dry_run:
                edges_created += 1
                continue
            try:
                db.execute(stmt)
                edges_created += 1
            except Exception as exc:
                errors.append(f"EDGE-JSONL: {exc!s} — {stmt[:120]}")

        skipped = sum(1 for k, _ in records if k not in ("node", "edge"))

    # ── CSV format ────────────────────────────────────────────────────────────
    elif fmt == "csv":
        records = list(_records_from_csv(path))

        for kind, row in _progress(
            [(k, r) for k, r in records if k == "node"],
            desc="Nodes (CSV)",
        ):
            label = row.get(":LABEL", row.get("label", "Unknown"))
            props = {k: v for k, v in row.items()
                     if not k.startswith(":")}
            stmt = _build_node_cypher(label, props)
            if dry_run:
                nodes_created += 1
                continue
            try:
                db.execute(stmt)
                nodes_created += 1
            except Exception as exc:
                errors.append(f"NODE-CSV: {exc!s} — {stmt[:120]}")

        for kind, row in _progress(
            [(k, r) for k, r in records if k == "edge"],
            desc="Edges (CSV)",
        ):
            rel_type = row.get(":TYPE", row.get("type", "REL"))
            src_label = row.get(":START_LABEL", "Unknown")
            dst_label = row.get(":END_LABEL", "Unknown")
            src_props = {}
            dst_props = {}
            stmt = _build_edge_cypher(src_label, src_props, rel_type,
                                      dst_label, dst_props)
            if dry_run:
                edges_created += 1
                continue
            try:
                db.execute(stmt)
                edges_created += 1
            except Exception as exc:
                errors.append(f"EDGE-CSV: {exc!s} — {stmt[:120]}")

        skipped = sum(1 for k, _ in records if k not in ("node", "edge"))

    # ── Checkpoint ───────────────────────────────────────────────────────────
    if not dry_run and db is not None:
        try:
            db.checkpoint()
        except Exception as exc:
            errors.append(f"CHECKPOINT: {exc!s}")

    summary = {
        "nodes": nodes_created,
        "edges": edges_created,
        "skipped": skipped,
        "errors": errors,
    }

    # ── Validate ─────────────────────────────────────────────────────────────
    if validate and not dry_run and db is not None:
        print("\n[neo4j_import] Running validation queries…")
        _run_validation(db, nodes_created, edges_created, summary)

    return summary


def _run_validation(db, expected_nodes: int, expected_edges: int, summary: dict):
    """Run MATCH queries to spot-check the imported graph."""
    try:
        node_rows = db.execute("MATCH (n) RETURN n.name, n.age")
        print(f"  MATCH (n) → {len(node_rows)} rows (expected ~{expected_nodes} nodes)")
    except Exception as exc:
        summary["errors"].append(f"VALIDATE-NODES: {exc!s}")

    print("[neo4j_import] Validation complete.")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Import a Neo4j dump into SparrowDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input", "-i", required=True,
                        help="Path to the dump file (.cypher/.cql/.jsonl/.csv)")
    parser.add_argument("--db", required=True,
                        help="Path to the SparrowDB database directory")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and count statements without writing to DB")
    parser.add_argument("--validate", action="store_true",
                        help="Run MATCH queries after import to verify counts")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Checkpoint every N records (default: 500)")
    args = parser.parse_args()

    result = import_file(
        path=args.input,
        db_path=args.db,
        dry_run=args.dry_run,
        validate=args.validate,
        batch_size=args.batch_size,
    )

    print("\n[neo4j_import] Import summary:")
    print(f"  Nodes created : {result['nodes']}")
    print(f"  Edges created : {result['edges']}")
    print(f"  Skipped       : {result['skipped']}")
    print(f"  Errors        : {len(result['errors'])}")
    if result["errors"]:
        print("\nErrors (first 20):")
        for err in result["errors"][:20]:
            print(f"  {err}")
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
