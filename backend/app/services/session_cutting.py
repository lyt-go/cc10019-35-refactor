"""会话切分核心：可复用的事件流切分、边界诊断与结果拼装。

规则判断（哪些相邻事件对触发切分）与结果拼装（会话字典的构建）在本模块分离，
session_split 服务与 behavior_path 预演共用同一套实现，保证切割口径一致。

切割规则的判定优先级与重构前 split_sessions 的判断顺序一致：
timeout → cross_day → cross_channel。同一对相邻事件可能同时命中多条规则，
边界诊断会记录全部命中规则以及按优先级生效的规则。
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass, field, replace
from typing import Any

# 切割规则的命中优先级：与重构前 split_sessions 中的判断顺序一致。
RULE_PRIORITY: tuple[str, ...] = ("timeout", "cross_day", "cross_channel")


@dataclass(frozen=True)
class CuttingRules:
    """一次切分使用的规则集；disabled_rules 用于预演关闭某条规则。"""

    timeout_sec: int
    cross_day_policy: str = "keep"  # split/merge/keep
    cross_channel_policy: str = "merge"  # split/merge
    session_key: str = "user_id"
    disabled_rules: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def from_strategy(cls, strategy: dict[str, Any]) -> "CuttingRules":
        """从切割策略记录构造规则集。"""
        return cls(
            timeout_sec=int(strategy["timeout_sec"]),
            cross_day_policy=strategy.get("cross_day_policy", "keep"),
            cross_channel_policy=strategy.get("cross_channel_policy", "merge"),
            session_key=strategy.get("session_key", "user_id"),
        )

    @classmethod
    def from_snapshot(cls, snapshot: dict[str, Any]) -> "CuttingRules":
        """从基准快照还原规则集（历史重放使用）。"""
        return cls(
            timeout_sec=int(snapshot["timeout_sec"]),
            cross_day_policy=snapshot.get("cross_day_policy", "keep"),
            cross_channel_policy=snapshot.get("cross_channel_policy", "merge"),
            session_key=snapshot.get("session_key", "user_id"),
            disabled_rules=frozenset(snapshot.get("disabled_rules", [])),
        )

    def to_snapshot(self) -> dict[str, Any]:
        """序列化为可存储、可回放的快照。"""
        return {
            "timeout_sec": self.timeout_sec,
            "cross_day_policy": self.cross_day_policy,
            "cross_channel_policy": self.cross_channel_policy,
            "session_key": self.session_key,
            "disabled_rules": sorted(self.disabled_rules),
        }

    def without(self, *rules: str) -> "CuttingRules":
        """返回关闭指定规则后的规则集；未知规则直接报错。"""
        unknown = [r for r in rules if r not in RULE_PRIORITY]
        if unknown:
            raise ValueError(f"Unknown cutting rules: {unknown}")
        return replace(self, disabled_rules=self.disabled_rules | frozenset(rules))

    def is_active(self, rule: str) -> bool:
        """规则是否参与本次切分判定。"""
        if rule in self.disabled_rules:
            return False
        if rule == "timeout":
            return True
        if rule == "cross_day":
            return self.cross_day_policy == "split"
        if rule == "cross_channel":
            return self.cross_channel_policy == "split"
        return False


@dataclass(frozen=True)
class BoundaryDecision:
    """一对相邻事件的切分判定结果。"""

    time_diff: int
    hit_rules: tuple[str, ...]  # 全部命中的规则（可能多条）
    applied_rule: str | None  # 按优先级实际生效的规则；None 表示不切分

    @property
    def split(self) -> bool:
        return self.applied_rule is not None


def evaluate_boundary(prev: dict[str, Any], curr: dict[str, Any], rules: CuttingRules) -> BoundaryDecision:
    """评估一对相邻事件是否触发切分，记录全部命中规则与生效规则。"""
    time_diff = curr["timestamp"] - prev["timestamp"]
    hits: list[str] = []
    if rules.is_active("timeout") and time_diff > rules.timeout_sec:
        hits.append("timeout")
    if rules.is_active("cross_day") and prev["timestamp"] // 86400 != curr["timestamp"] // 86400:
        hits.append("cross_day")
    if rules.is_active("cross_channel") and prev.get("channel") != curr.get("channel"):
        hits.append("cross_channel")
    applied = next((r for r in RULE_PRIORITY if r in hits), None)
    return BoundaryDecision(time_diff=time_diff, hit_rules=tuple(hits), applied_rule=applied)


def assemble_session(session_key: str, session_events: list[dict[str, Any]], session_start: int) -> dict[str, Any]:
    """拼装一个会话结果（收拢后的唯一拼装入口）。"""
    return {
        "session_key": session_key,
        "events": [dict(e) for e in session_events],
        "event_count": len(session_events),
        "start_time": session_start,
        "end_time": session_events[-1]["timestamp"],
        "duration": session_events[-1]["timestamp"] - session_start,
    }


@dataclass
class CutOutcome:
    """一次切分的完整产出：会话列表 + 逐边界的诊断数据。"""

    sessions: list[dict[str, Any]]
    boundaries: list[dict[str, Any]]
    total_events: int


def split_event_stream(events: list[dict[str, Any]], rules: CuttingRules) -> CutOutcome:
    """按规则切分事件流，输出会话与可回放的边界诊断数据。"""
    keyed_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for e in events:
        key = e.get(rules.session_key, e["user_id"])
        keyed_events[key].append(e)

    sessions: list[dict[str, Any]] = []
    boundaries: list[dict[str, Any]] = []

    for key, evts in keyed_events.items():
        evts.sort(key=lambda x: x["timestamp"])
        current_session: list[dict[str, Any]] = []
        session_start = evts[0]["timestamp"] if evts else 0
        seq = 0

        for evt in evts:
            if not current_session:
                current_session = [evt]
                session_start = evt["timestamp"]
                continue

            prev = current_session[-1]
            decision = evaluate_boundary(prev, evt, rules)
            boundaries.append({
                "seq": seq,
                "session_key": key,
                "prev": dict(prev),
                "curr": dict(evt),
                "time_diff": decision.time_diff,
                "hit_rules": list(decision.hit_rules),
                "applied_rule": decision.applied_rule,
                "split": decision.split,
            })
            seq += 1

            if decision.split:
                sessions.append(assemble_session(key, current_session, session_start))
                current_session = [evt]
                session_start = evt["timestamp"]
            else:
                current_session.append(evt)

        if current_session:
            sessions.append(assemble_session(key, current_session, session_start))

    return CutOutcome(sessions=sessions, boundaries=boundaries, total_events=len(events))


# ========== 边界诊断的消费：连边/断边与影响范围 ==========
def edge_key(boundary: dict[str, Any]) -> str:
    """一条相邻事件对（连边/断边）的稳定标识。"""
    return f"{boundary['session_key']}#{boundary['seq']}"


def edge_detail(base: dict[str, Any], scenario: dict[str, Any]) -> dict[str, Any]:
    """一条边界在基准与预演两侧的对照明细。"""
    return {
        "edge_key": edge_key(base),
        "session_key": base["session_key"],
        "prev_event_id": base["prev"].get("event_id"),
        "curr_event_id": base["curr"].get("event_id"),
        "prev_timestamp": base["prev"]["timestamp"],
        "curr_timestamp": base["curr"]["timestamp"],
        "time_diff": base["time_diff"],
        "baseline_hit_rules": list(base["hit_rules"]),
        "baseline_applied_rule": base["applied_rule"],
        "scenario_hit_rules": list(scenario["hit_rules"]),
        "scenario_applied_rule": scenario["applied_rule"],
        "scenario_split": scenario["split"],
    }


def diff_boundaries(
    base: CutOutcome,
    scenario: CutOutcome,
    disabled_rule: str,
) -> dict[str, Any]:
    """对比同一基准事件流在两种规则下的边界，给出可核对的影响范围。"""
    base_by_key = {edge_key(b): b for b in base.boundaries}
    scen_by_key = {edge_key(b): b for b in scenario.boundaries}

    reconnected: list[dict[str, Any]] = []  # 断 -> 连
    newly_broken: list[dict[str, Any]] = []  # 连 -> 断
    still_broken: list[dict[str, Any]] = []  # 断 -> 断（被其他规则顶住）

    ordered = sorted(base_by_key.values(), key=lambda b: (b["session_key"], b["seq"]))
    for b in ordered:
        s = scen_by_key.get(edge_key(b))
        if s is None:
            continue
        if b["split"] and not s["split"]:
            reconnected.append(edge_detail(b, s))
        elif not b["split"] and s["split"]:
            newly_broken.append(edge_detail(b, s))
        elif b["split"] and s["split"]:
            still_broken.append(edge_detail(b, s))

    still_broken_by_rule: dict[str, int] = {}
    for edge in still_broken:
        rule = edge["scenario_applied_rule"] or "unknown"
        still_broken_by_rule[rule] = still_broken_by_rule.get(rule, 0) + 1

    return {
        "disabled_rule": disabled_rule,
        "baseline_sessions": len(base.sessions),
        "scenario_sessions": len(scenario.sessions),
        "baseline_broken_edges": sum(1 for b in base.boundaries if b["split"]),
        "scenario_broken_edges": sum(1 for b in scenario.boundaries if b["split"]),
        "baseline_broken_edge_keys": sorted(edge_key(b) for b in base.boundaries if b["split"]),
        "scenario_broken_edge_keys": sorted(edge_key(b) for b in scenario.boundaries if b["split"]),
        "reconnected_edges": reconnected,
        "newly_broken_edges": newly_broken,
        "still_broken_edges": still_broken,
        "still_broken_by_rule": still_broken_by_rule,
    }


def fingerprint(events: list[dict[str, Any]], rules_snapshot: dict[str, Any]) -> str:
    """对基准（事件 + 规则）计算稳定指纹，用于核对两侧是否同一基准。"""
    canonical = json.dumps(
        {"events": events, "rules": rules_snapshot},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
