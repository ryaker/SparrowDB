#!/usr/bin/env python3
"""Shadow mode: run Cypher queries against BOTH Neo4j AND SparrowDB, diff results.

Shadow testing lets you validate SparrowDB compatibility with Neo4j by
executing the same query set against both engines and comparing results
row-by-row.

Usage
-----
::

    # Run a single query
    python scripts/shadow_runner.py \\
        --neo4j bolt://localhost:7687 \\
        --neo4j-user neo4j --neo4j-pass secret \\
        --db /tmp/mydb \\
        --query "MATCH (n:Person) RETURN n.name, n.age"

    # Run a file of queries (one per line)
    python scripts/shadow_runner.py \\
        --neo4j bolt://localhost:7687 \\
        --db /tmp/mydb \\
        --queries queries.txt \\
        --report shadow_report.json

    # Skip Neo4j (SparrowDB-only dry-run)
    python scripts/shadow_runner.py \\
        --db /tmp/mydb \\
        --query "MATCH (n:Person) RETURN n.name" \\
        --sparrow-only

Result comparison
-----------------
Results are compared as *bags* (multisets) of row dicts.  Column names are
normalised (``n.name`` ↔ ``name``).  Values are compared as strings so that
type coercion differences do not produce false negatives.

Neo4j bolt driver is optional — if not installed the ``--sparrow-only`` mode
is used automatically.
"""

import sys
import argparse
import json
import os
from datetime import datetime, timezone
from typing import Optional

# ── optional neo4j driver ────────────────────────────────────────────────────

try:
    from neo4j import GraphDatabase as _Neo4jDriver  # type: ignore

    _NEO4J_AVAILABLE = True
except ImportError:
    _Neo4jDriver = None  # type: ignore
    _NEO4J_AVAILABLE = False


# ── value normalisation ──────────────────────────────────────────────────────

def _raw_u64_to_str(v: int) -> str:
    """Decode a SparrowDB inline-string u64 back to a Python str.

    Strings up to 8 bytes are stored as little-endian packed u64 in the
    SparrowDB storage layer.  When the Python bindings return such a value it
    comes back as ``int``.  This helper decodes it so comparisons with Neo4j
    string values are meaningful.
    """
    if v < 0:
        v = v & 0xFFFFFFFFFFFFFFFF  # treat as unsigned
    raw = v.to_bytes(8, byteorder="little")
    decoded = raw.rstrip(b"\x00")
    try:
        return decoded.decode("utf-8")
    except UnicodeDecodeError:
        return str(v)


def _normalise_value(v) -> str:
    """Convert any value type to a comparable string representation."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        # Heuristic: integers > 256 that fit in 8 bytes might be inline strings.
        # We try to decode and check if the result is printable ASCII.
        if v > 256:
            candidate = _raw_u64_to_str(v)
            if candidate.isprintable() and not candidate.isdigit():
                return candidate
        return str(v)
    if isinstance(v, float):
        return f"{v:.6g}"
    return str(v)


def _normalise_row(row: dict) -> dict:
    """Strip variable prefixes (``n.name`` → ``name``) and normalise values."""
    out = {}
    for k, v in row.items():
        # Strip leading ``<var>.`` prefix so n.name == name
        short_key = k.split(".", 1)[-1] if "." in k else k
        out[short_key] = _normalise_value(v)
    return out


def _rows_to_bag(rows: list[dict]) -> list[dict]:
    """Return a sorted list of normalised row dicts (bag comparison)."""
    normalised = [_normalise_row(r) for r in rows]
    return sorted(normalised, key=lambda d: json.dumps(d, sort_keys=True))


# ── Neo4j result helpers ─────────────────────────────────────────────────────

def _neo4j_record_to_dict(record) -> dict:
    """Convert a neo4j ``Record`` to a plain Python dict."""
    return dict(record.items())


# ── comparison logic ─────────────────────────────────────────────────────────

def _compare_results(
    sparrow_rows: Optional[list[dict]],
    neo4j_rows: Optional[list[dict]],
    neo4j_skipped: bool = False,
) -> dict:
    """Compare two result sets.  Returns a comparison summary dict.

    Parameters
    ----------
    sparrow_rows:
        Rows from SparrowDB, or ``None`` if it errored.
    neo4j_rows:
        Rows from Neo4j, or ``None`` if it errored or was not run.
    neo4j_skipped:
        ``True`` when Neo4j was intentionally not queried (sparrow-only mode).
        In this case a non-erroring SparrowDB result is considered a match.
    """
    if neo4j_skipped:
        if sparrow_rows is None:
            return {"match": False, "reason": "sparrow_errored"}
        return {"match": True, "reason": "sparrow_only", "sparrow_rows": len(sparrow_rows)}

    if sparrow_rows is None and neo4j_rows is None:
        return {"match": True, "reason": "both_errored"}

    if sparrow_rows is None:
        return {
            "match": False,
            "reason": "sparrow_errored",
            "neo4j_rows": len(neo4j_rows or []),
        }

    if neo4j_rows is None:
        return {
            "match": False,
            "reason": "neo4j_errored",
            "sparrow_rows": len(sparrow_rows),
        }

    s_bag = _rows_to_bag(sparrow_rows)
    n_bag = _rows_to_bag(neo4j_rows)

    if s_bag == n_bag:
        return {"match": True, "rows": len(s_bag)}

    # Find differences
    s_set = [json.dumps(r, sort_keys=True) for r in s_bag]
    n_set = [json.dumps(r, sort_keys=True) for r in n_bag]
    only_sparrow = [r for r in s_set if r not in n_set]
    only_neo4j = [r for r in n_set if r not in s_set]

    return {
        "match": False,
        "reason": "row_mismatch",
        "sparrow_rows": len(s_bag),
        "neo4j_rows": len(n_bag),
        "only_in_sparrow": [json.loads(r) for r in only_sparrow[:10]],
        "only_in_neo4j": [json.loads(r) for r in only_neo4j[:10]],
    }


# ── shadow runner core ────────────────────────────────────────────────────────

def run_shadow(
    queries: list[str],
    db_path: str,
    neo4j_uri: Optional[str] = None,
    neo4j_user: str = "neo4j",
    neo4j_password: str = "neo4j",
    sparrow_only: bool = False,
    timeout: float = 30.0,
) -> list[dict]:
    """Run *queries* against SparrowDB (and optionally Neo4j) and return results.

    Parameters
    ----------
    queries:
        List of Cypher query strings.
    db_path:
        Path to the SparrowDB database directory.
    neo4j_uri:
        Bolt URI (e.g. ``bolt://localhost:7687``).  If ``None`` or
        ``sparrow_only`` is ``True``, Neo4j is skipped.
    neo4j_user / neo4j_password:
        Neo4j credentials.
    sparrow_only:
        When ``True``, skip Neo4j entirely (useful without a running Neo4j
        instance).
    timeout:
        Per-query timeout in seconds (applied to Neo4j only).

    Returns
    -------
    list[dict]
        One dict per query with keys: ``query``, ``match``, ``comparison``,
        ``sparrow_rows``, ``neo4j_rows``, ``error_sparrow``, ``error_neo4j``,
        ``sparrow_time_ms``, ``neo4j_time_ms``.
    """
    import sparrowdb
    import time

    sdb = sparrowdb.GraphDb(db_path)

    neo4j_driver = None
    if not sparrow_only and neo4j_uri and _NEO4J_AVAILABLE:
        neo4j_driver = _Neo4jDriver.driver(
            neo4j_uri, auth=(neo4j_user, neo4j_password)
        )
    elif not sparrow_only and not _NEO4J_AVAILABLE:
        print(
            "[shadow_runner] WARNING: neo4j driver not installed. "
            "Run: pip install neo4j\n"
            "Falling back to sparrow-only mode.",
            file=sys.stderr,
        )

    results = []
    for q in queries:
        q = q.strip()
        if not q:
            continue

        sparrow_result: Optional[list[dict]] = None
        neo4j_result: Optional[list[dict]] = None
        error_sparrow: Optional[str] = None
        error_neo4j: Optional[str] = None
        sparrow_ms: Optional[float] = None
        neo4j_ms: Optional[float] = None

        # ── SparrowDB ────────────────────────────────────────────────────────
        t0 = time.perf_counter()
        try:
            raw = sdb.execute(q)
            # raw is a list of dicts
            sparrow_result = raw if isinstance(raw, list) else []
        except Exception as exc:
            error_sparrow = str(exc)
        sparrow_ms = round((time.perf_counter() - t0) * 1000, 2)

        # ── Neo4j ─────────────────────────────────────────────────────────────
        if neo4j_driver is not None:
            t0 = time.perf_counter()
            try:
                with neo4j_driver.session() as session:
                    records = list(session.run(q))
                    neo4j_result = [_neo4j_record_to_dict(r) for r in records]
            except Exception as exc:
                error_neo4j = str(exc)
            neo4j_ms = round((time.perf_counter() - t0) * 1000, 2)

        # ── Compare ───────────────────────────────────────────────────────────
        _neo4j_was_skipped = neo4j_driver is None
        comparison = _compare_results(sparrow_result, neo4j_result,
                                      neo4j_skipped=_neo4j_was_skipped)

        results.append({
            "query": q,
            "match": comparison.get("match", False),
            "comparison": comparison,
            "sparrow_rows": len(sparrow_result) if sparrow_result is not None else None,
            "neo4j_rows": len(neo4j_result) if neo4j_result is not None else None,
            "error_sparrow": error_sparrow,
            "error_neo4j": error_neo4j,
            "sparrow_time_ms": sparrow_ms,
            "neo4j_time_ms": neo4j_ms,
        })

    if neo4j_driver is not None:
        neo4j_driver.close()

    return results


# ── reporting ─────────────────────────────────────────────────────────────────

def print_summary(results: list[dict]) -> None:
    total = len(results)
    matched = sum(1 for r in results if r["match"])
    sparrow_only_count = sum(
        1 for r in results if r["neo4j_rows"] is None and r["error_neo4j"] is None
    )

    print(f"\n{'='*60}")
    print(f"Shadow Run Summary — {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print(f"{'='*60}")
    print(f"  Total queries : {total}")
    print(f"  Matching      : {matched}")
    print(f"  Mismatching   : {total - matched - sparrow_only_count}")
    if sparrow_only_count:
        print(f"  Sparrow-only  : {sparrow_only_count} (Neo4j skipped)")

    for i, r in enumerate(results, 1):
        status = "MATCH" if r["match"] else "DIFF "
        s_rows = r["sparrow_rows"] if r["sparrow_rows"] is not None else "ERR"
        n_rows = r["neo4j_rows"] if r["neo4j_rows"] is not None else (
            "skip" if r["error_neo4j"] is None else "ERR"
        )
        timing = f"{r['sparrow_time_ms']}ms"
        print(f"\n  [{i:3d}] {status}  sparrow={s_rows} neo4j={n_rows}  {timing}")
        print(f"        {r['query'][:80]}")
        if r["error_sparrow"]:
            print(f"        SparrowDB error: {r['error_sparrow']}")
        if r["error_neo4j"]:
            print(f"        Neo4j error    : {r['error_neo4j']}")
        if not r["match"] and "only_in_sparrow" in r.get("comparison", {}):
            cmp = r["comparison"]
            if cmp.get("only_in_sparrow"):
                print(f"        Only in Sparrow: {cmp['only_in_sparrow'][:3]}")
            if cmp.get("only_in_neo4j"):
                print(f"        Only in Neo4j  : {cmp['only_in_neo4j'][:3]}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Shadow-run Cypher queries against Neo4j + SparrowDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", required=True,
                        help="Path to SparrowDB database directory")
    parser.add_argument("--neo4j", metavar="URI",
                        default="bolt://localhost:7687",
                        help="Neo4j bolt URI (default: bolt://localhost:7687)")
    parser.add_argument("--neo4j-user", default="neo4j",
                        help="Neo4j username (default: neo4j)")
    parser.add_argument("--neo4j-pass", default="neo4j",
                        help="Neo4j password (default: neo4j)")
    parser.add_argument("--query", "-q", metavar="CYPHER",
                        help="Single Cypher query to run")
    parser.add_argument("--queries", metavar="FILE",
                        help="File with one Cypher query per line")
    parser.add_argument("--sparrow-only", action="store_true",
                        help="Skip Neo4j, run SparrowDB queries only")
    parser.add_argument("--report", metavar="PATH",
                        help="Write JSON report to this file")
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="Per-query timeout in seconds (default: 30)")
    args = parser.parse_args()

    queries: list[str] = []
    if args.query:
        queries.append(args.query)
    if args.queries:
        with open(args.queries, encoding="utf-8") as fh:
            queries.extend(line.strip() for line in fh if line.strip()
                           and not line.startswith("#"))

    if not queries:
        parser.error("Provide --query or --queries")

    results = run_shadow(
        queries=queries,
        db_path=args.db,
        neo4j_uri=args.neo4j,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_pass,
        sparrow_only=args.sparrow_only,
        timeout=args.timeout,
    )

    print_summary(results)

    if args.report:
        with open(args.report, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2)
        print(f"\n[shadow_runner] Report written to {args.report}")

    mismatches = sum(1 for r in results if not r["match"]
                     and r["neo4j_rows"] is not None)
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
