"""行为路径分析引擎：构建用户行为路径的Sankey图数据。

预演能力：消费会话切割发布的边界诊断数据，预演关闭一条切割规则后
哪些会话连边会变化，并给出可核对的影响范围；预演只写预演产物，
不覆盖已发布的会话与路径产物。
"""
from __future__ import annotations

from typing import Any
from collections import defaultdict, Counter

from app.store import store
from app.schemas import PathResult
from app.services.session_cutting import (
    RULE_PRIORITY,
    CuttingRules,
    diff_boundaries,
    edge_key,
    fingerprint,
    split_event_stream,
)
from app.services.session_split import _generate_raw_events


def _generate_path_events(start_event: str, target_event: str, count: int) -> list[dict[str, Any]]:
    """生成模拟路径事件数据。"""
    events = []
    base_time = 1700000000
    for user_idx in range(1, count + 1):
        user_id = f"U{user_idx:05d}"
        # 生成随机深度的路径
        path_len = 3 + (user_idx % 4)
        for step in range(path_len):
            if step == 0:
                event_id = start_event
            elif step == path_len - 1 and user_idx % 3 == 0:
                event_id = target_event
            else:
                event_id = ["EV-001", "EV-002", "EV-003", "EV-004"][(user_idx + step) % 4]
            events.append({
                "user_id": user_id,
                "event_id": event_id,
                "timestamp": base_time + user_idx * 1000 + step * 200,
                "session_id": f"S{user_idx:03d}",
            })
    return sorted(events, key=lambda x: (x["user_id"], x["timestamp"]))


def build_path_sankey(model_id: str) -> PathResult:
    """构建行为路径的Sankey数据。"""
    model = store.find_by_field("behavior_path", "model_id", model_id)
    if model is None:
        raise ValueError(f"Path model not found: {model_id}")

    start_event = model["start_event"]
    target_event = model["target_event"]
    time_window = model.get("time_window", 86400)
    path_depth = model.get("path_depth", 5)
    path_filter = model.get("path_filter", "")

    events = _generate_path_events(start_event, target_event, 200)

    # 按用户分组构建路径
    user_paths: dict[str, list[str]] = defaultdict(list)
    for e in events:
        user_paths[e["user_id"]].append(e["event_id"])

    # 统计路径转换
    edge_counts: dict[tuple[str, str], int] = defaultdict(int)
    node_counts: Counter[str] = Counter()
    conversion_paths: list[list[str]] = []
    total_conversions = 0

    for user_id, path in user_paths.items():
        # 限制路径深度
        path = path[:path_depth]
        if len(path) < 2:
            continue

        # 统计节点
        for node in path:
            node_counts[node] += 1

        # 统计边
        for i in range(len(path) - 1):
            edge = (path[i], path[i + 1])
            edge_counts[edge] += 1

        # 检查是否到达目标
        if target_event in path:
            conversion_paths.append(path)
            total_conversions += 1

    # 构建Sankey节点
    nodes = []
    node_index: dict[str, int] = {}
    for node, count in sorted(node_counts.items(), key=lambda x: -x[1]):
        node_index[node] = len(nodes)
        nodes.append({
            "name": node,
            "value": count,
            "category": _get_event_category(node),
        })

    # 构建Sankey边
    links = []
    for (source, target), count in sorted(edge_counts.items(), key=lambda x: -x[1]):
        if source in node_index and target in node_index:
            links.append({
                "source": node_index[source],
                "target": node_index[target],
                "value": count,
                "source_name": source,
                "target_name": target,
            })

    result = PathResult(
        model_id=model_id,
        start_event=start_event,
        target_event=target_event,
        sankey_nodes=nodes,
        sankey_links=links,
        conversion_paths=conversion_paths[:50],
        total_conversions=total_conversions,
    )

    # 存储产物
    store.set_artifact("behavior_path", model_id, result.model_dump())

    # 更新状态
    store.update("behavior_path", model["id"], {
        "status": "已完成",
        "pending": False,
    })

    return result


def _get_event_category(event_id: str) -> str:
    """获取事件分类。"""
    event = store.find_by_field("event_track", "event_id", event_id)
    if event:
        return event.get("category", "unknown")
    return "unknown"


def get_path_result(model_id: str) -> dict[str, Any] | None:
    """获取路径分析结果。"""
    artifact = store.get_artifact("behavior_path", model_id)
    if artifact:
        return artifact
    try:
        result = build_path_sankey(model_id)
        return result.model_dump()
    except ValueError:
        return None


def find_common_paths(model_id: str, min_count: int = 3) -> list[dict[str, Any]]:
    """查找高频路径模式。"""
    result = get_path_result(model_id)
    if result is None:
        return []

    path_counter: Counter[tuple[str, ...]] = Counter()
    for path in result.get("conversion_paths", []):
        path_counter[tuple(path)] += 1

    return [
        {"path": list(path), "count": count}
        for path, count in path_counter.most_common(20)
        if count >= min_count
    ]


# ========== 策略关闭预演：消费会话切割的边界诊断数据 ==========
def _preview_id(model_id: str, strategy_id: str, disabled_rule: str) -> str:
    return f"{model_id}:{strategy_id}:{disabled_rule}"


def _load_baseline(strategy_id: str) -> dict[str, Any]:
    """装载预演基准：优先消费已发布的边界诊断产物，否则按当前策略临时构建。

    返回的基准包含同一份事件与规则快照，预演两侧都从这里取数。
    """
    artifact = store.get_artifact("session_boundaries", strategy_id)
    if artifact is not None:
        return {
            "events": artifact["events"],
            "rules": artifact["rules"],
            "fingerprint": artifact["fingerprint"],
            "source": "published",
        }
    strategy = store.find_by_field("session_split", "strategy_id", strategy_id)
    if strategy is None:
        raise ValueError(f"Strategy not found: {strategy_id}")
    rules = CuttingRules.from_strategy(strategy)
    events = _generate_raw_events()
    rules_snapshot = rules.to_snapshot()
    return {
        "events": events,
        "rules": rules_snapshot,
        "fingerprint": fingerprint(events, rules_snapshot),
        "source": "ephemeral",
    }


def _compute_impact(events: list[dict[str, Any]], rules_snapshot: dict[str, Any], disabled_rule: str) -> dict[str, Any]:
    """在同一基准上分别按原规则与关闭规则后的规则切分，对比边界变化。"""
    base_rules = CuttingRules.from_snapshot(rules_snapshot)
    scenario_rules = base_rules.without(disabled_rule)
    base = split_event_stream(events, base_rules)
    scenario = split_event_stream(events, scenario_rules)
    return diff_boundaries(base, scenario, disabled_rule)


def preview_policy_disable(model_id: str, disabled_rule: str) -> dict[str, Any]:
    """预演关闭一条切割规则：哪些会话断边会连起来，影响范围可核对。

    所有校验通过后只写预演产物（path_preview），不触碰已发布的
    会话切割与行为路径产物。
    """
    model = store.find_by_field("behavior_path", "model_id", model_id)
    if model is None:
        raise ValueError(f"Path model not found: {model_id}")
    if disabled_rule not in RULE_PRIORITY:
        raise ValueError(f"Unknown cutting rule: {disabled_rule}")
    strategy_id = model.get("session_split_rule")
    if store.find_by_field("session_split", "strategy_id", strategy_id) is None:
        raise ValueError(f"Strategy not found: {strategy_id}")

    baseline = _load_baseline(strategy_id)
    impact = _compute_impact(baseline["events"], baseline["rules"], disabled_rule)

    preview = {
        "preview_id": _preview_id(model_id, strategy_id, disabled_rule),
        "model_id": model_id,
        "strategy_id": strategy_id,
        "disabled_rule": disabled_rule,
        "baseline": baseline,
        "impact": impact,
    }
    # 唯一写入：预演产物。失败路径在此之前已全部抛出。
    store.set_artifact("path_preview", preview["preview_id"], preview)
    return preview


def get_preview(model_id: str, disabled_rule: str) -> dict[str, Any] | None:
    """获取已生成的预演产物。"""
    model = store.find_by_field("behavior_path", "model_id", model_id)
    if model is None:
        return None
    strategy_id = model.get("session_split_rule")
    return store.get_artifact("path_preview", _preview_id(model_id, strategy_id, disabled_rule))


def _require_preview(model_id: str, disabled_rule: str) -> dict[str, Any]:
    preview = get_preview(model_id, disabled_rule)
    if preview is None:
        raise ValueError(f"Preview not found: {model_id}/{disabled_rule}")
    return preview


def verify_preview(model_id: str, disabled_rule: str) -> dict[str, Any]:
    """核对预演：用预演留存的同一份历史事件实际重切，核对实际断边。"""
    preview = _require_preview(model_id, disabled_rule)
    baseline = preview["baseline"]

    base_rules = CuttingRules.from_snapshot(baseline["rules"])
    scenario_rules = base_rules.without(disabled_rule)
    base = split_event_stream(baseline["events"], base_rules)
    scenario = split_event_stream(baseline["events"], scenario_rules)

    impact = preview["impact"]
    scenario_by_key = {edge_key(b): b for b in scenario.boundaries}
    actual_broken = sorted(edge_key(b) for b in scenario.boundaries if b["split"])
    actual_base_broken = sorted(edge_key(b) for b in base.boundaries if b["split"])
    actual_reconnected = sorted(
        edge_key(b) for b in base.boundaries
        if b["split"] and not scenario_by_key[edge_key(b)]["split"]
    )

    checks = {
        "scenario_broken_edges": {
            "predicted": impact["scenario_broken_edge_keys"],
            "actual": actual_broken,
            "match": impact["scenario_broken_edge_keys"] == actual_broken,
        },
        "baseline_broken_edges": {
            "predicted": impact["baseline_broken_edge_keys"],
            "actual": actual_base_broken,
            "match": impact["baseline_broken_edge_keys"] == actual_base_broken,
        },
        "reconnected_edges": {
            "predicted": sorted(e["edge_key"] for e in impact["reconnected_edges"]),
            "actual": actual_reconnected,
            "match": sorted(e["edge_key"] for e in impact["reconnected_edges"]) == actual_reconnected,
        },
    }
    return {
        "preview_id": preview["preview_id"],
        "model_id": model_id,
        "disabled_rule": disabled_rule,
        "verified": all(c["match"] for c in checks.values()),
        "checks": checks,
        "baseline_fingerprint": baseline["fingerprint"],
    }


def replay_preview(model_id: str, disabled_rule: str) -> dict[str, Any]:
    """历史重放：只用预演留存的基准重算影响范围，核对与历史预演一致。

    规则与事件此后发生变化（重新切割、改阈值）不影响本重放结果。
    """
    preview = _require_preview(model_id, disabled_rule)
    baseline = preview["baseline"]
    recomputed = _compute_impact(baseline["events"], baseline["rules"], disabled_rule)
    return {
        "preview_id": preview["preview_id"],
        "model_id": model_id,
        "disabled_rule": disabled_rule,
        "consistent": recomputed == preview["impact"],
        "baseline_fingerprint": baseline["fingerprint"],
        "baseline_source": baseline["source"],
        "recomputed_impact": recomputed,
    }
