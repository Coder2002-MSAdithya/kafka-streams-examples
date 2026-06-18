#!/usr/bin/env python3
"""Render simplified processing-policy and RA diagrams (requires Graphviz `dot`)."""
from __future__ import annotations

import base64
import copy
import hashlib
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
    ("email-svc", "EmailService"),
]

SKIP_OPERATORS = frozenset({"to", "through", "toStream"})
TAG_OPERATORS = frozenset({"declassifyTags", "addTags"})
INTERNAL_TOPIC_MARKERS = ("KSTREAM", "repartition", "-repartition", "changelog", "JOINOTHER", "JOINTHIS", "REDUCE", "AGGREGATE")
MAX_RA_FIELDS_IN_LABEL = 5
MAX_RA_RANK_SAME_CHILDREN = 2
INFRA_OUTPUT_FIELDS = frozenset({"_aggregate_value", "_selection", "simpleMerge"})


def is_ra_internal_topic(topic: str | None) -> bool:
    if not topic:
        return False
    if "$$" in topic or "Lambda" in topic or "@" in topic and "/" in topic:
        return True
    upper = topic.upper()
    return any(marker in upper for marker in INTERNAL_TOPIC_MARKERS)


def is_external_egress_topic(topic: str | None) -> bool:
    if not topic:
        return False
    return not is_ra_internal_topic(topic)


def format_ra_topic(topic: str | None) -> str:
    if not topic:
        return "?"
    if is_ra_internal_topic(topic):
        lower = topic.lower()
        if "repartition" in lower:
            return "ρ repartition"
        if "changelog" in lower:
            return "ω changelog"
        return "ω internal"
    return shorten_topic(topic, 28)


def truncate_field_list(fields: list[str], limit: int = MAX_RA_FIELDS_IN_LABEL) -> str:
    visible = [f for f in fields if f and f not in INFRA_OUTPUT_FIELDS]
    if not visible:
        return ""
    if len(visible) <= limit:
        return ", ".join(visible)
    remaining = len(visible) - limit
    return ", ".join(visible[:limit]) + f", … (+{remaining} more)"


def html_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def ra_symbol_html(symbol: str) -> str:
    return {
        "γ_g": "γ<SUB>g</SUB>",
        "σ∪": "σ<SUB>∪</SUB>",
        "π_k": "π<SUB>k</SUB>",
    }.get(symbol, symbol)


def ra_html_label(symbol: str, annotation: str = "") -> str:
    sym = html_escape(ra_symbol_html(symbol))
    lines = [f"<B>{sym}</B>"]
    if annotation:
        lines.append(html_escape(annotation))
    return "<<" + "<BR ALIGN='LEFT'/>".join(lines) + ">>"


def lineage_sanitization_kind(lineage: dict) -> str:
    return str(lineage.get("sanitizationKind") or "").upper()


def lineage_projection_annotation(lineages: list[dict], limit: int = 6) -> str:
    """π labels: show derivations and predicates; omit pure constants and infra fields."""
    parts: list[str] = []
    for lineage in lineages:
        if len(parts) >= limit:
            break
        out = lineage.get("outputField") or ""
        if not out or out in INFRA_OUTPUT_FIELDS:
            expr = lineage.get("expression") or ""
            if expr and expr not in parts:
                parts.append(expr)
            continue
        kind = lineage_sanitization_kind(lineage)
        if kind == "CONSTANT":
            continue
        expr = (lineage.get("expression") or "").strip()
        sources = [s for s in (lineage.get("sourceFields") or []) if s]
        if kind == "BOOLEAN_PREDICATE" and expr:
            parts.append(f"{out}: {expr}")
        elif kind == "AGGREGATE" and expr:
            parts.append(expr if out in INFRA_OUTPUT_FIELDS else f"{out}={expr}")
        elif kind == "PASSTHROUGH" and sources:
            src = sources[0]
            parts.append(f"{out}←{src}" if out != src else out)
        elif expr and expr != out:
            parts.append(f"{out}={expr}")
        elif out:
            parts.append(out)
    return ", ".join(parts)


def lineage_aggregate_annotation(lineages: list[dict]) -> str:
    parts: list[str] = []
    for lineage in lineages[:3]:
        out = lineage.get("outputField") or ""
        expr = lineage.get("expression") or "γ"
        if out in INFRA_OUTPUT_FIELDS:
            parts.append(expr)
        else:
            parts.append(f"{out}={expr}")
    return "; ".join(parts)


def overlay_validation_sink_projection(tree: dict | None, path: dict) -> None:
    """When mapValues π nodes lack lineages, use path-level validation sink fields."""
    if tree is None:
        return
    outs = [
        f
        for f in (path.get("outputFields") or [])
        if f and f not in INFRA_OUTPUT_FIELDS
    ]
    if not outs or path.get("egressTopic") != "order-validations":
        return

    def walk(node: dict) -> None:
        if node.get("kind") != "operator":
            for child in node.get("children") or []:
                walk(child)
            return
        symbol = node.get("algebraSymbol") or ""
        desc = (node.get("description") or "").lower()
        if symbol == "π" and (not node.get("fieldLineages")) and (
            "projection" in desc or "map" in (node.get("topic") or "")
        ):
            node["outputFields"] = list(outs)
        for child in node.get("children") or []:
            walk(child)

    walk(tree)


def enrich_ra_tree_callbacks(node: dict | None, callbacks: dict[str, list[dict]]) -> None:
    """Overlay manifest callback lineages onto operator nodes missing fieldLineages."""
    if node is None:
        return
    if node.get("kind") == "operator":
        op = normalize_operator(node.get("topic") or "")
        if not node.get("fieldLineages") and op in callbacks:
            for cb in callbacks[op]:
                if cb.get("fieldLineages"):
                    node["fieldLineages"] = copy.deepcopy(cb["fieldLineages"])
                    break
                if cb.get("outputFields") and not node.get("outputFields"):
                    node["outputFields"] = list(cb["outputFields"])
                if cb.get("selectionExpression") and not node.get("selectionExpression"):
                    node["selectionExpression"] = cb["selectionExpression"]
                if cb.get("keyFields") and not node.get("keyFields"):
                    node["keyFields"] = list(cb["keyFields"])
    for child in node.get("children") or []:
        enrich_ra_tree_callbacks(child, callbacks)


def prune_ra_tree_for_display(node: dict | None) -> dict | None:
    """Drop internal-topic scans and collapse redundant unary chains for layout."""
    if node is None:
        return None
    kind = node.get("kind") or ""
    if kind == "scan":
        topic = node.get("topic") or ""
        if is_ra_internal_topic(topic):
            return None
        return copy.deepcopy(node)

    pruned = copy.deepcopy(node)
    pruned["children"] = []
    for child in node.get("children") or []:
        pc = prune_ra_tree_for_display(child)
        if pc is not None:
            pruned["children"].append(pc)

    op = (pruned.get("topic") or "").lower()
    symbol = pruned.get("algebraSymbol") or ""
    children = pruned["children"]

    # Collapse pass-through unary operators with a single child (never collapse ω/γ_g/γ/σ∪/⋈/∪).
    if kind == "operator" and len(children) == 1 and symbol not in ("⋈", "∪", "σ∪", "γ", "γ_g", "ω", "σ"):
        child = children[0]
        if not pruned.get("outputFields") and not pruned.get("selectionExpression"):
            return child
        if not pruned.get("outputFields") and not pruned.get("selectionFields"):
            if symbol not in ("σ", "π") and op not in ("filter", "mapvalues", "process", "selectkey"):
                return child

    if kind == "operator" and symbol in ("⋈",) and not children:
        return None
    if kind == "sink" and len(children) == 0:
        return pruned
    if kind == "operator" and len(children) == 0 and symbol not in ("⋈", "∪"):
        return None
    return pruned


def filter_ra_paths_for_display(paths: list[dict]) -> list[dict]:
    """Prefer external egress topics; drop duplicate lambda/internal sink paths."""
    if not paths:
        return paths
    external = [p for p in paths if is_external_egress_topic(p.get("egressTopic"))]
    chosen = external or paths
    by_egress: dict[str, dict] = {}
    for path in chosen:
        egress = path.get("egressTopic") or "?"
        existing = by_egress.get(egress)
        if existing is None:
            by_egress[egress] = path
            continue
        # Prefer the path with fewer ingress topics (usually the pruned manifest path).
        if len(path.get("ingressTopics") or []) < len(existing.get("ingressTopics") or []):
            by_egress[egress] = path
    return list(by_egress.values())


def external_ingress_topics(path: dict) -> list[str]:
    topics = path.get("ingressTopics") or []
    if not topics and path.get("ingressTopic"):
        topics = [path.get("ingressTopic")]
    return [t for t in topics if t and not is_ra_internal_topic(t)]

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


def dot_node_id(raw_id: str) -> str:
    safe = "".join(c if c.isalnum() else "_" for c in raw_id)
    return f"n_{safe}"


def index_callbacks(policy: dict) -> dict[str, list[dict]]:
    """Map normalized operator name -> callback projection metadata."""
    indexed: dict[str, list[dict]] = defaultdict(list)
    for egress in policy.get("egressPaths") or []:
        for cb in egress.get("callbackProjections") or []:
            op = normalize_operator(cb.get("operator") or "")
            if op:
                indexed[op].append(cb)
    return indexed


def shorten_topic(topic: str, max_len: int = 36) -> str:
    if len(topic) <= max_len:
        return topic
    return topic[: max_len - 3] + "..."


def graph_node_display_label(node: dict, callbacks: dict[str, list[dict]]) -> str:
    kind = node.get("kind") or ""
    if kind == "topic":
        return dot_escape(node.get("topic") or node.get("label") or "?")
    if kind == "internalTopic":
        topic = node.get("topic") or ""
        kind_tag = node.get("internalTopicKind") or "internal"
        return dot_label(kind_tag, shorten_topic(topic))
    if kind == "stream":
        label = (node.get("label") or "stream").replace("\n", " ")
        return dot_escape(label[:48])
    if kind == "stateStore":
        return dot_escape("state store")
    if kind == "operator":
        raw = (node.get("label") or "operator").replace("\\n", "\n")
        op = normalize_operator(raw)
        parts = [raw.split("\n", 1)[0].strip()]
        for cb in callbacks.get(op, []):
            sel = cb.get("selectionExpression") or ""
            if not sel and cb.get("selectionFields"):
                sel = " ∧ ".join(cb["selectionFields"])
            if sel:
                parts.append(f"σ: {sel}")
            outs = cb.get("outputFields") or []
            if outs:
                parts.append("π: " + ", ".join(outs))
            keys = cb.get("keyFields") or []
            if keys:
                parts.append("key: " + ", ".join(keys))
            break
        return dot_label(*parts)
    return dot_escape(node.get("label") or kind or "?")


def graph_node_style(node: dict) -> tuple[str, str]:
    kind = node.get("kind") or ""
    if kind in ("topic",):
        return "ellipse", "#d4edda"
    if kind == "internalTopic":
        return "ellipse", "#fff3cd"
    if kind == "stream":
        return "ellipse", "#fdebd0"
    if kind == "stateStore":
        return "cylinder", "#e8daef"
    if kind == "operator":
        op = normalize_operator(node.get("label") or "")
        if op in ("split", "branch"):
            return "diamond", "#fff3cd"
        if op == "merge":
            return "diamond", "#ffe8cc"
        if op in ("join", "leftJoin", "outerJoin") or "join" in op:
            return "hexagon", "#fff3cd"
        if op in ("declassifyTags", "addTags"):
            return "box", "#ffe8cc"
        return "box", "#cce5ff"
    return "box", "#f0f0f0"


def write_policy_graph_dot(policy: dict, principal: str, service: str, out: Path) -> None:
    graph = policy.get("graph") or {}
    nodes_list = graph.get("nodes") or []
    edges = graph.get("edges") or []
    callbacks = index_callbacks(policy)

    lines = [
        "digraph policy {",
        '  graph [rankdir=TB, bgcolor="white", fontname="Helvetica", '
        'splines=polyline, nodesep=0.45, ranksep=0.55];',
        '  node [fontname="Helvetica", fontsize=11, style=filled, height=0.35];',
        '  edge [fontname="Helvetica", fontsize=9, color="#444444", arrowsize=0.7];',
        f'  label="{dot_label(f"{service} ({principal})", "Processing policy graph (agent topology)")}";',
        "  labelloc=t;",
        "  fontsize=14;",
    ]

    if not nodes_list:
        paths = extract_processing_paths(policy)
        if not paths:
            lines.append('  empty [label="No processing graph documented", shape=note, fillcolor="#f8d7da"];')
            lines.append("}")
            out.write_text("\n".join(lines), encoding="utf-8")
            return
        lines.append(
            '  note [label="Graph empty; showing egress summary only", shape=note, fillcolor="#f8f9fa"];'
        )
        lines.append("}")
        out.write_text("\n".join(lines), encoding="utf-8")
        return

    for node in nodes_list:
        nid = dot_node_id(node["id"])
        label = graph_node_display_label(node, callbacks)
        shape, fillcolor = graph_node_style(node)
        lines.append(
            f'  {nid} [label="{label}", shape={shape}, fillcolor="{fillcolor}"];'
        )

    for edge in edges:
        src = dot_node_id(edge["from"])
        dst = dot_node_id(edge["to"])
        elabel = edge.get("label") or ""
        if elabel:
            lines.append(f'  {src} -> {dst} [label="{dot_escape(elabel)}"];')
        else:
            lines.append(f"  {src} -> {dst};")

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
    if "[" in token and "]" in token:
        head, rest = token.split("[", 1)
        annotation = rest.split("]", 1)[0].strip()
        head = head.strip()
        if head in {"σ", "σ∪", "π", "⋈", "∪", "γ", "ρ"} and annotation:
            return f"{head}  {annotation}"
    if token.startswith("σ∪"):
        return token
    if token.startswith("σ"):
        return token if len(token) > 1 else "σ  filter"
    if token.startswith("γ"):
        return token if len(token) > 1 else "γ  aggregate"
    if token == "⋈" or token.startswith("⋈"):
        return token if len(token) > 1 else "⋈  join"
    if token.startswith("π"):
        return token if len(token) > 1 else "π  map"
    if token.startswith("∪"):
        return token if len(token) > 1 else "∪  merge"
    if token.startswith("ρ"):
        return token if len(token) > 1 else "ρ  repartition"
    return token


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


def id_slug(value: str, max_len: int = 20) -> str:
    slug = "".join(ch if ch.isalnum() else "_" for ch in value.lower())
    while "__" in slug:
        slug = slug.replace("__", "_")
    slug = slug.strip("_")
    if not slug:
        slug = "op"
    return slug[:max_len]


def symbol_slug(symbol: str) -> str:
    return {
        "σ": "sigma",
        "π": "pi",
        "γ": "gamma",
        "γ_g": "group",
        "ω": "window",
        "⋈": "join",
        "∪": "union",
        "π_k": "rekey",
        "σ∪": "branch",
    }.get(symbol or "", id_slug(symbol or "op", 8))


def semantic_ra_node_id(path_idx: int, node: dict, seq: list[int]) -> str:
    """Human-readable Graphviz node id (layout identifier only — not an RA operator)."""
    kind = node.get("kind") or "n"
    if kind == "scan":
        return f"p{path_idx}_scan_{id_slug(format_ra_topic(node.get('topic') or 'topic'))}"
    if kind == "sink":
        return f"p{path_idx}_sink_{id_slug(format_ra_topic(node.get('topic') or 'sink'))}"
    seq[0] += 1
    symbol = node.get("algebraSymbol") or "?"
    sym = symbol_slug(symbol)
    op = id_slug(node.get("topic") or "op", 12)
    sel = (node.get("selectionExpression") or "").strip()
    if symbol == "σ" and sel and not sel.startswith("window:"):
        return f"p{path_idx}_{sym}_{id_slug(sel, 16)}_{seq[0]}"
    if symbol == "π":
        lineages = lineage_projection_annotation(node.get("fieldLineages") or [], limit=1)
        if lineages:
            return f"p{path_idx}_{sym}_{id_slug(lineages, 16)}_{seq[0]}"
        outs = [f for f in (node.get("outputFields") or []) if f not in INFRA_OUTPUT_FIELDS]
        if outs:
            return f"p{path_idx}_{sym}_{id_slug(outs[0])}_{seq[0]}"
    if symbol == "γ" and sel:
        return f"p{path_idx}_{sym}_{id_slug(sel, 16)}"
    if symbol == "ω":
        return f"p{path_idx}_{sym}_{seq[0]}"
    if symbol == "γ_g":
        keys = node.get("keyFields") or []
        if keys:
            return f"p{path_idx}_{sym}_{id_slug(keys[0])}"
    if symbol == "⋈":
        return f"p{path_idx}_{sym}_{seq[0]}"
    if symbol == "∪":
        return f"p{path_idx}_{sym}_{seq[0]}"
    return f"p{path_idx}_{sym}_{op}_{seq[0]}"


def join_scan_topics(node: dict) -> str:
    scans: list[str] = []
    for child in node.get("children") or []:
        if child.get("kind") == "scan":
            scans.append(format_ra_topic(child.get("topic")))
        else:
            nested = join_scan_topics(child)
            if nested:
                scans.append(nested)
    return ", ".join(scans)


def ra_structural_key(node: dict) -> str:
    """Hash tree shape + operator metadata so shared prefixes render once."""
    child_keys = [ra_structural_key(child) for child in (node.get("children") or [])]
    payload = json.dumps(
        {
            "kind": node.get("kind"),
            "topic": node.get("topic"),
            "symbol": node.get("algebraSymbol"),
            "description": node.get("description"),
            "selectionExpression": node.get("selectionExpression"),
            "outputFields": node.get("outputFields"),
            "children": child_keys,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ra_operator_annotation(node: dict) -> str:
    op = (node.get("topic") or "").lower()
    symbol = node.get("algebraSymbol") or ""
    sel = node.get("selectionExpression") or ""
    if sel.startswith("window:"):
        return sel[7:].strip()[:56]
    if not sel and node.get("selectionFields"):
        sel = " ∧ ".join(node["selectionFields"])
    if op == "filter" or symbol == "σ":
        if sel:
            return sel[:72]
        return "filter"
    if op == "windowedby" or symbol == "ω":
        return (sel or node.get("description") or "session/window")[:56]
    if op in ("groupby", "groupbykey") or symbol == "γ_g":
        keys = node.get("keyFields") or []
        if keys:
            return "group by " + ", ".join(keys[:4])
        return (node.get("description") or "group by key")[:56]
    if op in ("mapvalues", "map", "process", "flatmapvalues", "transformvalues",
              "forstockcheck", "forvalidation", "forbilling") or symbol == "π":
        lineages = node.get("fieldLineages") or []
        if lineages:
            projected = lineage_projection_annotation(lineages)
            if projected:
                return projected[:80]
        outs = [f for f in (node.get("outputFields") or []) if f not in INFRA_OUTPUT_FIELDS]
        if outs:
            return truncate_field_list(outs)
    if op == "selectkey" or symbol == "π_k":
        keys = node.get("keyFields") or []
        if keys:
            return "key→" + ", ".join(keys[:4])
        return "rekey"
    if symbol == "⋈" or "join" in op:
        topics = join_scan_topics(node)
        if topics:
            return topics[:72]
    if op == "merge" or symbol == "∪":
        return "union branches"
    if op == "aggregate" or (symbol == "γ" and op in ("aggregate", "reduce", "count")):
        lineages = node.get("fieldLineages") or []
        if lineages:
            agg = lineage_aggregate_annotation(lineages)
            if agg:
                return agg[:72]
        return (node.get("description") or "aggregate")[:56]
    parts: list[str] = []
    if sel:
        parts.append(sel[:48])
    outs = [f for f in (node.get("outputFields") or []) if f not in INFRA_OUTPUT_FIELDS]
    if outs:
        parts.append(truncate_field_list(outs))
    keys = node.get("keyFields") or []
    if keys:
        parts.append("key:" + ",".join(keys[:4]))
    return "; ".join(parts)


def ra_plain_label(symbol: str, annotation: str = "") -> str:
    sym = {
        "γ_g": "γ_g",
        "σ∪": "σ∪",
        "π_k": "π_k",
    }.get(symbol, symbol)
    if annotation:
        return f'"{dot_escape(sym + ": " + annotation)}"'
    return f'"{dot_escape(sym)}"'


def ra_tree_node_label(node: dict, egress_topic: str | None = None) -> str:
    kind = node.get("kind") or ""
    if kind == "scan":
        return f'"Scan({dot_escape(format_ra_topic(node.get("topic")))})"'
    if kind == "sink":
        topic = node.get("topic") or egress_topic or "?"
        if is_ra_internal_topic(topic) and egress_topic:
            topic = egress_topic
        return f'"Sink({dot_escape(format_ra_topic(topic))})"'
    symbol = node.get("algebraSymbol") or "?"
    op = (node.get("topic") or "").lower()
    annotation = ra_operator_annotation(node)
    description = (node.get("description") or "").strip()
    if op == "windowedby" or symbol == "ω":
        return ra_plain_label("ω", annotation or description or "session/window")
    if op in ("groupby", "groupbykey") or symbol == "γ_g":
        return ra_plain_label("γ_g", annotation or description or "group by key")
    if op in ("aggregate", "reduce", "count") or (symbol == "γ" and op in ("aggregate", "reduce", "count")):
        return ra_plain_label("γ", annotation or description or "aggregate")
    if op == "merge" or symbol == "∪":
        return ra_plain_label("∪", annotation or "union branches")
    if op in ("split", "branch") or symbol == "σ∪":
        return ra_plain_label("σ∪", annotation or description or "branch")
    if op == "selectkey" or symbol == "π_k":
        return ra_plain_label("π_k", annotation or description or "rekey")
    if annotation:
        return ra_plain_label(symbol, annotation)
    if description:
        return ra_plain_label(symbol, description[:56])
    if symbol == "⋈" and len(node.get("children") or []) > 1:
        return ra_plain_label("⋈", "join")
    return f'"{dot_escape(symbol)}"'


def ra_tree_node_style(node: dict) -> tuple[str, str, str]:
    kind = node.get("kind") or ""
    symbol = node.get("algebraSymbol") or ""
    op = (node.get("topic") or "").lower()
    if kind in ("scan", "sink"):
        return "ellipse", "filled", "#d4edda"
    if symbol in ("∪",) or op == "merge":
        return "diamond", "filled", "#ffe8cc"
    if symbol in ("⋈",) or "join" in op:
        return "hexagon", "filled", "#fff3cd"
    if symbol in ("σ∪",) or op in ("split", "branch"):
        return "diamond", "filled", "#fff3cd"
    if symbol == "ω" or op == "windowedby":
        return "box", "rounded,filled", "#fff8e1"
    if symbol == "γ_g" or op in ("groupby", "groupbykey"):
        return "box", "rounded,filled", "#fff3cd"
    if symbol in ("γ",) or op in ("aggregate", "reduce", "count"):
        return "box", "rounded,filled", "#fff3cd"
    if symbol in ("σ",) or op == "filter":
        return "box", "rounded,filled", "#e8f4fc"
    return "box", "rounded,filled", "#e8f4fc"


def append_ra_tree(
    lines: list[str],
    path_idx: int,
    node: dict,
    counter: list[int],
    egress_topic: str | None = None,
    emitted: dict[str, str] | None = None,
) -> tuple[str, list[str]]:
    """Emit nodes with data-flow edges leaf→root (Scan→…→Sink)."""
    if emitted is None:
        emitted = {}
    struct_key = ra_structural_key(node)
    if struct_key in emitted:
        return emitted[struct_key], []

    node_id = semantic_ra_node_id(path_idx, node, counter)
    emitted[struct_key] = node_id
    label = ra_tree_node_label(node, egress_topic)
    shape, style, fillcolor = ra_tree_node_style(node)
    lines.append(
        f'    {node_id} [label={label}, shape={shape}, style="{style}", '
        f'fillcolor="{fillcolor}"];'
    )

    children = node.get("children") or []
    child_ids: list[str] = []
    for child in children:
        child_id, _ = append_ra_tree(
            lines, path_idx, child, counter, egress_topic, emitted
        )
        child_ids.append(child_id)
        lines.append(f"    {child_id} -> {node_id};")

    # Forcing many siblings onto one rank creates an unreadably wide, blurry PNG.
    if 1 < len(child_ids) <= MAX_RA_RANK_SAME_CHILDREN:
        lines.append(f"    {{ rank=same; {'; '.join(child_ids)}; }}")

    if not children:
        return node_id, [node_id]
    return node_id, child_ids


def write_ra_dot(policy: dict, principal: str, service: str, out: Path) -> None:
    ra_paths = filter_ra_paths_for_display(
        (policy.get("relationalAlgebraAnalysis") or {}).get("processingPaths") or []
    )
    lines = [
        "digraph ra {",
        '  graph [rankdir=TB, bgcolor="white", fontname="Helvetica", '
        'splines=ortho, nodesep=0.35, ranksep=0.5, ordering="out"];',
        '  node [fontname="Helvetica", fontsize=11, style=filled, height=0.3, margin="0.08,0.04"];',
        '  edge [fontname="Helvetica", color="#444444", arrowsize=0.65];',
        f'  label="{dot_label(f"{service} ({principal})", "Relational algebra (tree)", "σ=select  π=project  γ=aggregate  γ_g=group-by-key  ω=window  ⋈=join  ∪=union")}";',
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
        ingress_topics = external_ingress_topics(path)
        ingress_label = ", ".join(format_ra_topic(t) for t in ingress_topics) if ingress_topics else "?"
        egress = path.get("egressTopic", "?")
        lines.append(f"  subgraph cluster_{idx} {{")
        lines.append(
            f'    label="{dot_escape(ingress_label)} → {dot_escape(format_ra_topic(egress))}"; '
            "style=rounded; color=\"#adb5bd\"; fontsize=10;"
        )

        tree = prune_ra_tree_for_display(path.get("expressionTree"))
        if tree:
            overlay_validation_sink_projection(tree, path)
            enrich_ra_tree_callbacks(tree, index_callbacks(policy))
        root_id: str | None = None
        if tree:
            root_id, _ = append_ra_tree(lines, idx, tree, [0], egress)
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

        in_fields = truncate_field_list(path.get("inputFields") or [])
        out_fields = truncate_field_list(path.get("outputFields") or [])
        dropped = truncate_field_list(path.get("droppedSensitiveFields") or [])
        note_id = f"r{idx}note"
        note_parts = []
        if out_fields:
            note_parts.append(f"out: {out_fields}")
        if dropped:
            note_parts.append(f"dropped: {dropped}")
        elif in_fields:
            note_parts.append(f"in: {in_fields}")
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


def discover_principals(policy_dir: Path) -> list[tuple[str, str]]:
    """Discover (principal, service) pairs from signed policy files."""
    found: list[tuple[str, str]] = []
    if not policy_dir.is_dir():
        return found
    for principal_dir in sorted(policy_dir.iterdir()):
        if not principal_dir.is_dir():
            continue
        policy = load_policy(principal_dir / "processing-policy.json")
        if policy is None:
            continue
        service = str(policy.get("service") or principal_dir.name)
        found.append((principal_dir.name, service))
    return found


def principals_for(policy_dir: Path) -> list[tuple[str, str]]:
    discovered = discover_principals(policy_dir)
    if not discovered:
        return PRINCIPALS
    # Preserve kafka-streams-examples ordering when both lists overlap.
    order = {p: i for i, (p, _) in enumerate(PRINCIPALS)}
    discovered.sort(key=lambda item: (order.get(item[0], 999), item[0]))
    return discovered


def render_dot(dot_file: Path, png_file: Path) -> None:
    result = subprocess.run(
        ["dot", "-Tpng", "-Gdpi=150", str(dot_file), "-o", str(png_file)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"WARN: dot failed for {dot_file}: {result.stderr.strip()}", file=sys.stderr)


def main() -> int:
    policy_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/kafka-streams-examples/policy")
    out_dir = Path(sys.argv[2] if len(sys.argv) > 2 else "/tmp/ms-workflow-logs/policy-diagrams")
    out_dir.mkdir(parents=True, exist_ok=True)

    if subprocess.run(["which", "dot"], capture_output=True).returncode != 0:
        print("ERROR: Graphviz `dot` not found", file=sys.stderr)
        return 1

    for principal, service in principals_for(policy_dir):
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
