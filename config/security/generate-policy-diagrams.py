#!/usr/bin/env python3
"""Render simplified processing-policy and RA diagrams (requires Graphviz `dot`)."""
from __future__ import annotations

import base64
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

PRINCIPALS = [
    ("orders-svc", "OrdersService"),
    ("fraud-svc", "FraudService"),
    ("inventory-svc", "InventoryService"),
    ("order-details-svc", "OrderDetailsService"),
    ("validations-agg-svc", "ValidationsAggregator"),
]

SKIP_OPERATORS = frozenset({"to", "through", "toStream"})
TAG_OPERATORS = frozenset({"declassifyTags", "addTags"})
INTERNAL_TOPIC_MARKERS = ("KSTREAM", "repartition", "-repartition")

# Preferred left-to-right order for high-level Kafka Streams DSL operators.
HIGH_LEVEL_OP_ORDER = [
    "filter",
    "selectKey",
    "groupBy",
    "windowedBy",
    "aggregate",
    "split",
    "join",
    "mapValues",
    "flatMap",
    "merge",
]


def topic_name(node: dict) -> str:
    return str(node.get("topic") or "")


def load_policy(policy_file: Path) -> dict | None:
    if not policy_file.is_file():
        return None
    envelope = json.loads(policy_file.read_text(encoding="utf-8"))
    payload_b64 = envelope.get("signedPayloadBase64")
    if not payload_b64:
        return None
    return json.loads(base64.b64decode(payload_b64).decode("utf-8"))


def dot_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def dot_label(*parts: str) -> str:
    """Join label lines with a Graphviz line break (not a literal backslash-n)."""
    lines = [dot_escape(part) for part in parts if part]
    return "\\n".join(lines)


def normalize_operator(label: str) -> str:
    if not label:
        return "operator"
    name = label.split("(", 1)[0].strip()
    if name in ("leftJoin", "outerJoin"):
        return "join"
    if name == "groupByKey":
        return "groupBy"
    return name


def collapse_runs(items: list[str]) -> list[str]:
    out: list[str] = []
    for item in items:
        if out and out[-1] == item:
            continue
        out.append(item)
    return out


def is_external_topic(topic: str | None) -> bool:
    if not topic:
        return False
    upper = topic.upper()
    return not any(marker in upper for marker in INTERNAL_TOPIC_MARKERS)


def collect_graph_operators(policy: dict) -> list[str]:
    """Collect unique DSL operator names from the policy graph (no lambda bodies)."""
    found: set[str] = set()
    for node in (policy.get("graph") or {}).get("nodes") or []:
        if node.get("kind") != "operator":
            continue
        op = normalize_operator(node.get("label") or "")
        if op in SKIP_OPERATORS or op in TAG_OPERATORS:
            continue
        found.add(op)
    ordered = [op for op in HIGH_LEVEL_OP_ORDER if op in found]
    for op in sorted(found):
        if op not in ordered:
            ordered.append(op)
    return collapse_runs(ordered)


def discover_sources(
    policy: dict,
    egress: dict | None,
    nodes: dict[str, dict],
    rev: dict[str, list[str]],
    sink_id: str | None,
) -> list[str]:
    sources: set[str] = set()
    if egress:
        for topic in egress.get("ingressTopics") or []:
            if is_external_topic(topic):
                sources.add(topic)
    for topic in policy.get("sources") or []:
        if is_external_topic(topic):
            sources.add(topic)

    if sink_id:
        writers = [
            pred for pred in rev.get(sink_id, [])
            if nodes.get(pred, {}).get("kind") == "operator"
        ]
        if writers:
            stack = [writers[0]]
            seen: set[str] = set()
            while stack:
                current = stack.pop()
                if current in seen:
                    continue
                seen.add(current)
                node = nodes.get(current)
                if not node:
                    continue
                if node.get("kind") in ("topic", "internalTopic"):
                    topic = topic_name(node)
                    if is_external_topic(topic):
                        sources.add(topic)
                for pred in rev.get(current, []):
                    stack.append(pred)
    return sorted(sources)


def extract_processing_paths(policy: dict) -> list[dict]:
    """One high-level input→operators→output path per egress sink (or table-only path)."""
    graph = policy.get("graph") or {}
    nodes = {n["id"]: n for n in graph.get("nodes") or []}
    rev: dict[str, list[str]] = defaultdict(list)
    for edge in graph.get("edges") or []:
        rev[edge["to"]].append(edge["from"])

    paths: list[dict] = []
    egress_paths = policy.get("egressPaths") or []
    if egress_paths:
        graph_operators = collect_graph_operators(policy)
        for egress in egress_paths:
            sink = egress.get("topic")
            if not sink:
                continue
            sink_id = next(
                (n["id"] for n in nodes.values()
                 if n.get("kind") == "topic" and n.get("topic") == sink),
                None,
            )
            sources = discover_sources(policy, egress, nodes, rev, sink_id)
            paths.append({
                "sources": sources or list(policy.get("sources") or []),
                "operators": graph_operators,
                "sink": sink,
                "declassify": list(egress.get("declassifyTags") or []),
                "add": list(egress.get("addTags") or []),
            })
        return paths

    # Table-only / no external sink (OrdersService materialized view)
    sources = list(policy.get("sources") or [])
    operators = collect_graph_operators(policy)
    if sources or operators:
        paths.append({
            "sources": sources,
            "operators": operators,
            "sink": "materialized view",
            "declassify": [],
            "add": [],
        })
    return paths


def write_policy_graph_dot(policy: dict, principal: str, service: str, out: Path) -> None:
    paths = extract_processing_paths(policy)
    lines = [
        "digraph policy {",
        '  graph [rankdir=LR, bgcolor="white", fontname="Helvetica", splines=ortho, nodesep=0.35, ranksep=0.55];',
        '  node [fontname="Helvetica", fontsize=12, style=filled, height=0.35];',
        '  edge [fontname="Helvetica", fontsize=10, color="#444444", arrowsize=0.7];',
        f'  label="{dot_label(f"{service} ({principal})", "High-level processing policy")}";',
        "  labelloc=t;",
        "  fontsize=14;",
    ]

    if not paths:
        lines.append('  empty [label="No processing path documented", shape=note, fillcolor="#f8d7da"];')
        lines.append("}")
        out.write_text("\n".join(lines), encoding="utf-8")
        return

    for idx, path in enumerate(paths):
        lines.append(f"  subgraph cluster_{idx} {{")
        sink = path["sink"]
        lines.append(f'    label="{sink}"; style=rounded; color="#adb5bd"; fontsize=11;')

        node_ids: list[str] = []
        prev: str | None = None

        for s_idx, source in enumerate(path["sources"]):
            nid = f"p{idx}s{s_idx}"
            lines.append(
                f'    {nid} [label="{dot_escape(source)}", shape=ellipse, fillcolor="#d4edda"];'
            )
            node_ids.append(nid)

        if len(node_ids) > 1:
            lines.append(f"    {{ rank=same; {'; '.join(node_ids)}; }}")

        merge_id = f"p{idx}merge"
        if len(node_ids) > 1:
            lines.append(
                f'    {merge_id} [label="sources", shape=diamond, fillcolor="#fff3cd", width=0.5, height=0.5];'
            )
            for nid in node_ids:
                lines.append(f"    {nid} -> {merge_id};")
            prev = merge_id
        elif node_ids:
            prev = node_ids[0]

        for o_idx, op in enumerate(path["operators"]):
            oid = f"p{idx}o{o_idx}"
            color = "#cce5ff"
            if op in ("declassifyTags", "addTags"):
                color = "#ffe8cc"
            lines.append(
                f'    {oid} [label="{dot_escape(op)}", shape=box, fillcolor="{color}"];'
            )
            if prev:
                lines.append(f"    {prev} -> {oid};")
            prev = oid

        if path["declassify"]:
            did = f"p{idx}declassify"
            lines.append(
                f'    {did} [label="{dot_label("declassify", ", ".join(path["declassify"]))}", '
                f'shape=box, fillcolor="#ffe8cc"];'
            )
            if prev:
                lines.append(f"    {prev} -> {did};")
            prev = did

        if path["add"]:
            aid = f"p{idx}add"
            lines.append(
                f'    {aid} [label="{dot_label("addTags", ", ".join(path["add"]))}", '
                f'shape=box, fillcolor="#ffe8cc"];'
            )
            if prev:
                lines.append(f"    {prev} -> {aid};")
            prev = aid

        sid = f"p{idx}sink"
        sink_label = dot_label("orders", "(materialized table)") if sink == "materialized view" else dot_escape(sink)
        lines.append(
            f'    {sid} [label="{sink_label}", shape=ellipse, fillcolor="#d4edda"];'
        )
        if prev:
            lines.append(f"    {prev} -> {sid};")
        lines.append("  }")

    lines.append("}")
    out.write_text("\n".join(lines), encoding="utf-8")


def simplify_ra_step(token: str) -> str:
    token = token.strip()
    if token.startswith("Scan("):
        topic = token[5:-1] if token.endswith(")") else token
        return f"Scan({topic})"
    if token.startswith("Sink("):
        topic = token[5:-1] if token.endswith(")") else token
        return f"Sink({topic})"
    if "table state" in token.lower():
        return "γ  aggregate (table)"
    if token.startswith("σ") or token.startswith("σ∪"):
        return "σ  filter"
    if token.startswith("γ"):
        return "γ  aggregate"
    if token == "⋈" or token.startswith("⋈"):
        return "⋈  join"
    if token.startswith("π"):
        return "π  map"
    if token.startswith("∪") or token.startswith("σ∪"):
        return "∪  merge"
    if token.startswith("ρ"):
        return "ρ  repartition"
    head = token.split("[", 1)[0].strip()
    if head.endswith(")]"):
        head = head[:-2]
    return head


def collapse_ra_steps(steps: list[str]) -> list[str]:
    out: list[str] = []
    for step in steps:
        if not step:
            continue
        if out and out[-1] == step:
            continue
        if out and step.split()[0] == out[-1].split()[0] and step.split()[0] in {
            "σ", "γ", "π", "ρ", "∪", "⋈"
        }:
            continue
        out.append(step)
    return out


def ra_steps_from_path(path: dict) -> list[str]:
    expr = path.get("algebraExpression") or ""
    if not expr:
        steps = path.get("steps") or []
        if steps:
            out = [f"Scan({path.get('ingressTopic', '?')})"]
            for step in steps:
                sym = step.get("algebraSymbol") or "?"
                op = step.get("operator") or step.get("description") or ""
                out.append(simplify_ra_step(f"{sym}[{op}]"))
            out.append(f"Sink({path.get('egressTopic', '?')})")
            return collapse_ra_steps(collapse_runs(out))
        return []
    parts = [p.strip() for p in expr.split("→")]
    simplified = [simplify_ra_step(p) for p in parts]
    return collapse_ra_steps(collapse_runs(simplified))


def ra_tree_node_label(node: dict) -> str:
    kind = node.get("kind") or ""
    if kind == "scan":
        return dot_escape(f"Scan({node.get('topic', '?')})")
    if kind == "sink":
        return dot_escape(f"Sink({node.get('topic', '?')})")
    symbol = node.get("algebraSymbol") or "?"
    description = (node.get("description") or "").strip()
    if description:
        return dot_label(symbol, description)
    return symbol


def ra_tree_node_style(node: dict) -> tuple[str, str, str]:
    kind = node.get("kind") or ""
    symbol = node.get("algebraSymbol") or ""
    if kind in ("scan", "sink"):
        return "ellipse", "filled", "#d4edda"
    if symbol in ("⋈", "∪", "γ"):
        return "box", "rounded,filled", "#fff3cd"
    return "box", "rounded,filled", "#e8f4fc"


def append_ra_tree(
    lines: list[str],
    path_idx: int,
    node: dict,
    counter: list[int],
) -> tuple[str, list[str]]:
    """Emit nodes with data-flow edges leaf→root (Scan→…→Sink)."""
    node_id = f"r{path_idx}t{counter[0]}"
    counter[0] += 1
    label = ra_tree_node_label(node)
    shape, style, fillcolor = ra_tree_node_style(node)
    lines.append(
        f'    {node_id} [label="{label}", shape={shape}, style="{style}", '
        f'fillcolor="{fillcolor}"];'
    )

    children = node.get("children") or []
    child_ids: list[str] = []
    leaf_ids: list[str] = []
    for child in children:
        child_id, child_leaves = append_ra_tree(lines, path_idx, child, counter)
        child_ids.append(child_id)
        leaf_ids.extend(child_leaves)
        lines.append(f"    {child_id} -> {node_id};")

    if len(child_ids) > 1:
        lines.append(f"    {{ rank=same; {'; '.join(child_ids)}; }}")

    if not children:
        return node_id, [node_id]
    return node_id, leaf_ids


def write_ra_dot(policy: dict, principal: str, service: str, out: Path) -> None:
    ra_paths = (policy.get("relationalAlgebraAnalysis") or {}).get("processingPaths") or []
    lines = [
        "digraph ra {",
        '  graph [rankdir=TB, bgcolor="white", fontname="Helvetica", '
        'splines=polyline, nodesep=0.55, ranksep=0.45, ordering="out"];',
        '  node [fontname="Helvetica", fontsize=12, height=0.35];',
        '  edge [fontname="Helvetica", color="#444444", arrowsize=0.7];',
        f'  label="{dot_label(f"{service} ({principal})", "Relational algebra (tree)")}";',
        "  labelloc=t;",
        "  fontsize=14;",
    ]

    if not ra_paths:
        lines.extend([
            '  empty [label="' + dot_label("No RA analysis", "(table-only / producer-only)") + '", '
            'shape=note, fillcolor="#f8d7da"];',
            "}",
        ])
        out.write_text("\n".join(lines), encoding="utf-8")
        return

    for idx, path in enumerate(ra_paths):
        ingress_topics = path.get("ingressTopics") or []
        if not ingress_topics and path.get("ingressTopic"):
            ingress_topics = [path.get("ingressTopic")]
        ingress_label = ", ".join(ingress_topics) if ingress_topics else "?"
        egress = path.get("egressTopic", "?")
        lines.append(f"  subgraph cluster_{idx} {{")
        lines.append(
            f'    label="{dot_escape(ingress_label)} → {dot_escape(egress)}"; '
            "style=rounded; color=\"#adb5bd\"; fontsize=11;"
        )

        tree = path.get("expressionTree")
        root_id: str | None = None
        if tree:
            root_id, _ = append_ra_tree(lines, idx, tree, [0])
        else:
            steps = ra_steps_from_path(path)
            prev: str | None = None
            for s_idx, step in enumerate(steps):
                nid = f"r{idx}s{s_idx}"
                shape = "ellipse" if step.startswith(("Scan(", "Sink(")) else "box"
                style = "filled" if shape == "ellipse" else "rounded,filled"
                fillcolor = "#d4edda" if shape == "ellipse" else "#e8f4fc"
                if step.startswith("γ") or step.startswith("⋈"):
                    fillcolor = "#fff3cd"
                lines.append(
                    f'    {nid} [label="{dot_escape(step)}", shape={shape}, style="{style}", '
                    f'fillcolor="{fillcolor}"];'
                )
                if prev:
                    lines.append(f"    {prev} -> {nid};")
                prev = nid
            root_id = prev

        in_fields = ", ".join(path.get("inputFields") or [])[:60]
        out_fields = ", ".join(path.get("outputFields") or [])[:60]
        dropped = ", ".join(path.get("droppedSensitiveFields") or [])
        note_id = f"r{idx}note"
        note_parts = []
        expr = path.get("algebraExpression") or ""
        if expr:
            note_parts.append(expr[:120])
        if in_fields:
            note_parts.append(f"in: {in_fields}")
        if out_fields:
            note_parts.append(f"out: {out_fields}")
        if dropped:
            note_parts.append(f"dropped: {dropped}")
        note = dot_label(*note_parts) if note_parts else dot_escape("field summary unavailable")
        anchor_id = f"r{idx}anchor"
        lines.append(f'    {anchor_id} [style=invis, width=0, height=0, label=""];')
        if root_id:
            lines.append(f"    {root_id} -> {anchor_id} [style=invis];")
        lines.append(f"    {anchor_id} -> {note_id} [style=invis];")
        lines.append(
            f'    {note_id} [label="{note}", shape=note, fillcolor="#f8f9fa", fontsize=10];'
        )
        lines.append(f"    {{ rank=max; {note_id}; }}")
        lines.append("  }")

    lines.append("}")
    out.write_text("\n".join(lines), encoding="utf-8")


def render_dot(dot_file: Path, png_file: Path) -> None:
    subprocess.run(
        ["dot", "-Tpng", str(dot_file), "-o", str(png_file)],
        check=True,
        capture_output=True,
    )


def main() -> int:
    policy_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/kafka-streams-examples/policy")
    out_dir = Path(sys.argv[2] if len(sys.argv) > 2 else "/tmp/ms-workflow-logs/policy-diagrams")
    out_dir.mkdir(parents=True, exist_ok=True)

    if subprocess.run(["which", "dot"], capture_output=True).returncode != 0:
        print("ERROR: Graphviz `dot` not found", file=sys.stderr)
        return 1

    for principal, service in PRINCIPALS:
        policy = load_policy(policy_dir / principal / "processing-policy.json")
        if policy is None:
            print(f"skip {principal}: no policy")
            continue
        policy_dot = out_dir / f"{principal}-policy-graph.dot"
        ra_dot = out_dir / f"{principal}-ra-flow.dot"
        write_policy_graph_dot(policy, principal, service, policy_dot)
        write_ra_dot(policy, principal, service, ra_dot)
        policy_png = out_dir / f"{principal}-policy-graph.png"
        ra_png = out_dir / f"{principal}-ra-flow.png"
        render_dot(policy_dot, policy_png)
        render_dot(ra_dot, ra_png)
        print(f"wrote {policy_png}")
        print(f"wrote {ra_png}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
