#!/usr/bin/env python3
"""Export a SparrowDB graph to DOT format for Graphviz visualization.

Queries all nodes (and, where possible, edges) from the database and writes
a ``graph.dot`` file that can be rendered with Graphviz::

    python scripts/visualize.py --db /tmp/mydb
    dot -Tsvg graph.dot > graph.svg
    dot -Tpng graph.dot > graph.png

The DOT output uses HTML-like labels so that multiple properties are displayed
on each node.  Edge labels show the relationship type.

Usage
-----
::

    python scripts/visualize.py --db /tmp/mydb [--output graph.dot] [--limit 500]
    python scripts/visualize.py --db /tmp/mydb --label Person --limit 100
    python scripts/visualize.py --db /tmp/mydb --format neato  # for spring layout

Limitations
-----------
- SparrowDB's Python bindings expose ``db.execute(cypher)`` for read
  queries.  Edges are retrieved via label-pair scans when the relationship
  type is registered in the catalog.  Unknown relationship types are skipped
  with a warning.
- Node property values come back as raw u64 integers when they are inline
  strings (≤ 8 bytes packed).  This module decodes them automatically.
- For very large graphs use ``--limit`` to cap node/edge counts.
"""

import sys
import argparse
import html
import os
from typing import Optional

# ── raw u64 → str decoder (mirrors shadow_runner) ───────────────────────────

def _decode_value(v) -> str:
    """Return a human-readable string for any SparrowDB property value."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        # Heuristic: try to decode as an inline string
        if v > 256:
            raw = (v & 0xFFFFFFFFFFFFFFFF).to_bytes(8, byteorder="little").rstrip(b"\x00")
            try:
                candidate = raw.decode("utf-8")
                if candidate.isprintable() and candidate.strip():
                    return candidate
            except UnicodeDecodeError:
                pass
        return str(v)
    if isinstance(v, float):
        return f"{v:.4g}"
    return str(v)


# ── known labels / rel-types heuristics ─────────────────────────────────────

# Commonly used Neo4j / SparrowDB label names for demo / test datasets.
_COMMON_LABELS = [
    "Person", "Movie", "Topic", "Company", "City", "Country",
    "Product", "User", "Tag", "Category", "Node",
]

_COMMON_REL_TYPES = [
    "KNOWS", "LIKES", "FOLLOWS", "ACTED_IN", "DIRECTED", "WORKS_AT",
    "LIVES_IN", "BELONGS_TO", "TAGGED_WITH", "CONNECTED_TO", "REL",
    "RELATES_TO", "DEPENDS_ON", "CONTAINS",
]

# Shape / colour palette per label (cycles if more labels than entries)
_COLORS = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f",
    "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac",
]


def _label_style(label: str, label_index: int) -> tuple[str, str]:
    """Return (fillcolor, fontcolor) for DOT node styling."""
    color = _COLORS[label_index % len(_COLORS)]
    return color, "white"


# ── DOT generation ────────────────────────────────────────────────────────────

def _make_node_label(label: str, props: dict) -> str:
    """Build an HTML-like DOT label table for a node."""
    rows = [f'<TR><TD COLSPAN="2"><B>{html.escape(label)}</B></TD></TR>']
    for k, v in list(props.items())[:8]:  # cap at 8 props for readability
        key_esc = html.escape(str(k))
        val_esc = html.escape(_decode_value(v))
        rows.append(f'<TR><TD ALIGN="LEFT"><I>{key_esc}</I></TD>'
                    f'<TD ALIGN="LEFT">{val_esc}</TD></TR>')
    return "<<TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0'>" + "".join(rows) + "</TABLE>>"


def export_dot(
    db_path: str,
    output: str = "graph.dot",
    limit: int = 500,
    label_filter: Optional[str] = None,
    labels_to_probe: Optional[list[str]] = None,
    rel_types_to_probe: Optional[list[str]] = None,
    layout_engine: str = "dot",
) -> dict:
    """Export the SparrowDB graph at *db_path* to a DOT file at *output*.

    Parameters
    ----------
    db_path:
        Path to the SparrowDB database directory.
    output:
        Output ``.dot`` file path.
    limit:
        Maximum number of nodes to export (applied per label scan).
    label_filter:
        If set, only nodes with this label are exported.
    labels_to_probe:
        List of label names to attempt querying.  Defaults to
        ``_COMMON_LABELS`` plus any label specified in ``label_filter``.
    rel_types_to_probe:
        List of relationship type names to attempt querying.  Defaults to
        ``_COMMON_REL_TYPES``.
    layout_engine:
        Graphviz layout hint written into the DOT file (``dot``, ``neato``,
        ``fdp``, ``sfdp``, ``circo``).

    Returns
    -------
    dict
        Summary with keys ``nodes``, ``edges``, ``output``.
    """
    import sparrowdb

    db = sparrowdb.GraphDb(db_path)

    if labels_to_probe is None:
        labels_to_probe = list(_COMMON_LABELS)
        if label_filter and label_filter not in labels_to_probe:
            labels_to_probe.insert(0, label_filter)
    if rel_types_to_probe is None:
        rel_types_to_probe = list(_COMMON_REL_TYPES)

    nodes: dict[str, dict] = {}       # node_key → {label, props, id_val}
    edges: list[tuple[str, str, str]] = []  # (src_key, dst_key, rel_type)

    label_index: dict[str, int] = {}

    # ── Probe labels ─────────────────────────────────────────────────────────
    for label in labels_to_probe:
        if label_filter and label != label_filter:
            continue
        if len(nodes) >= limit:
            break
        try:
            rows = db.execute(f"MATCH (n:{label}) RETURN n.name, n.age, n.title, n.id")
        except Exception:
            # Label doesn't exist or query not supported — skip silently
            continue

        if not rows:
            continue

        if label not in label_index:
            label_index[label] = len(label_index)

        for row in rows:
            if len(nodes) >= limit:
                break
            # Build a node key from all returned properties
            props = {k: v for k, v in row.items() if v is not None}
            # Use the most identifying property as the key
            for id_key in ("n.id", "n.name", "n.title"):
                if id_key in props:
                    raw_id = props[id_key]
                    node_key = f"{label}:{_decode_value(raw_id)}"
                    break
            else:
                node_key = f"{label}:{len(nodes)}"

            nodes[node_key] = {
                "label": label,
                "props": props,
                "label_index": label_index[label],
            }

    # ── Probe relationship types ──────────────────────────────────────────────
    # SparrowDB requires a MATCH pattern with node+edge+node.  We probe
    # every combination of (src_label, rel_type, dst_label) for the labels
    # we actually found nodes for.
    found_labels = list(label_index.keys())

    for rel_type in rel_types_to_probe:
        if len(edges) >= limit * 2:
            break
        for src_label in found_labels:
            if len(edges) >= limit * 2:
                break
            for dst_label in found_labels:
                if len(edges) >= limit * 2:
                    break
                try:
                    rows = db.execute(
                        f"MATCH (a:{src_label})-[r:{rel_type}]->(b:{dst_label}) "
                        f"RETURN a.name, b.name LIMIT {limit}"
                    )
                except Exception:
                    continue

                for row in rows:
                    # Determine src/dst node keys
                    src_name_raw = row.get("a.name")
                    dst_name_raw = row.get("b.name")
                    src_key = f"{src_label}:{_decode_value(src_name_raw)}" if src_name_raw else None
                    dst_key = f"{dst_label}:{_decode_value(dst_name_raw)}" if dst_name_raw else None

                    if src_key and dst_key:
                        # Ensure both endpoints are in the nodes dict
                        if src_key not in nodes:
                            nodes[src_key] = {
                                "label": src_label,
                                "props": {"name": src_name_raw},
                                "label_index": label_index.get(src_label, 0),
                            }
                        if dst_key not in nodes:
                            nodes[dst_key] = {
                                "label": dst_label,
                                "props": {"name": dst_name_raw},
                                "label_index": label_index.get(dst_label, 0),
                            }
                        edges.append((src_key, dst_key, rel_type))

    # ── Write DOT ─────────────────────────────────────────────────────────────
    # Assign integer IDs
    node_ids: dict[str, int] = {k: i for i, k in enumerate(nodes)}

    lines = [
        f"// Generated by SparrowDB visualize.py",
        f"// Nodes: {len(nodes)}  Edges: {len(edges)}",
        f"// Layout engine: {layout_engine}",
        f"digraph SparrowDB {{",
        f'  graph [layout={layout_engine} overlap=false fontname="Helvetica"];',
        f'  node  [shape=plaintext fontname="Helvetica" fontsize=10];',
        f'  edge  [fontname="Helvetica" fontsize=9 arrowsize=0.7];',
        "",
    ]

    # Legend subgraph
    if label_index:
        lines.append("  subgraph cluster_legend {")
        lines.append('    label="Legend" fontsize=10 style=filled fillcolor="#f5f5f5";')
        for lbl, idx in label_index.items():
            fill, font = _label_style(lbl, idx)
            lines.append(
                f'    legend_{idx} [label="{html.escape(lbl)}" shape=box '
                f'style=filled fillcolor="{fill}" fontcolor="{font}" fontsize=9];'
            )
        lines.append("  }")
        lines.append("")

    # Nodes
    for key, info in nodes.items():
        nid = node_ids[key]
        label = info["label"]
        props = info["props"]
        fill, font = _label_style(label, info["label_index"])
        dot_label = _make_node_label(label, props)
        lines.append(
            f"  n{nid} [label={dot_label} style=filled "
            f'fillcolor="{fill}" fontcolor="{font}"];'
        )

    lines.append("")

    # Edges
    for src_key, dst_key, rel_type in edges:
        src_id = node_ids.get(src_key)
        dst_id = node_ids.get(dst_key)
        if src_id is None or dst_id is None:
            continue
        lines.append(f'  n{src_id} -> n{dst_id} [label="{html.escape(rel_type)}"];')

    lines.append("}")

    with open(output, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    return {"nodes": len(nodes), "edges": len(edges), "output": output}


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Export SparrowDB graph to DOT format for Graphviz.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", required=True,
                        help="Path to SparrowDB database directory")
    parser.add_argument("--output", "-o", default="graph.dot",
                        help="Output DOT file (default: graph.dot)")
    parser.add_argument("--limit", type=int, default=500,
                        help="Maximum number of nodes to export (default: 500)")
    parser.add_argument("--label", metavar="LABEL",
                        help="Only export nodes with this label")
    parser.add_argument("--labels", metavar="LABEL,...",
                        help="Comma-separated list of labels to probe")
    parser.add_argument("--rel-types", metavar="TYPE,...",
                        help="Comma-separated list of relationship types to probe")
    parser.add_argument("--format", metavar="ENGINE", default="dot",
                        choices=["dot", "neato", "fdp", "sfdp", "circo", "twopi"],
                        help="Graphviz layout engine (default: dot)")
    parser.add_argument("--render", action="store_true",
                        help="Render to SVG automatically (requires graphviz in PATH)")
    args = parser.parse_args()

    labels_probe = [l.strip() for l in args.labels.split(",")] if args.labels else None
    rels_probe = [r.strip() for r in args.rel_types.split(",")] if args.rel_types else None

    result = export_dot(
        db_path=args.db,
        output=args.output,
        limit=args.limit,
        label_filter=args.label,
        labels_to_probe=labels_probe,
        rel_types_to_probe=rels_probe,
        layout_engine=args.format,
    )

    print(f"[visualize] Exported {result['nodes']} nodes, {result['edges']} edges")
    print(f"[visualize] DOT file written to: {result['output']}")

    if args.render:
        svg_out = os.path.splitext(args.output)[0] + ".svg"
        import subprocess
        ret = subprocess.run(
            [args.format, "-Tsvg", args.output, "-o", svg_out],
            capture_output=True,
        )
        if ret.returncode == 0:
            print(f"[visualize] SVG rendered to: {svg_out}")
        else:
            print(f"[visualize] ERROR rendering SVG: {ret.stderr.decode()}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
