"""会话切割引擎：按超时阈值和跨天/跨渠道策略切割用户会话。

切分逻辑复用 app.services.session_cutting；本模块负责策略装载、
结果产物发布以及可回放边界诊断数据的输出。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.schemas import SessionResult
from app.services.session_cutting import (
    CuttingRules,
    fingerprint,
    split_event_stream,
)


def _generate_raw_events() -> list[dict[str, Any]]:
    """生成原始事件流用于会话切割。"""
    events = []
    base_time = 1700000000
    for user_idx in range(1, 51):  # 50个用户
        user_id = f"U{user_idx:05d}"
        device_id = f"D{user_idx % 10 + 1:03d}"
        # 每个用户生成3-8个事件
        num_events = 3 + (user_idx % 6)
        for ev_idx in range(num_events):
            # 模拟会话间隔：有时短（会话内），有时长（新会话）
            if ev_idx > 0 and user_idx % 7 == 0:
                # 长间隔，触发新会话
                offset = ev_idx * 3600 + 7200
            else:
                offset = ev_idx * 300  # 5分钟间隔，同一会话
            events.append({
                "user_id": user_id,
                "device_id": device_id,
                "event_id": ["EV-001", "EV-002", "EV-003"][ev_idx % 3],
                "timestamp": base_time + user_idx * 10000 + offset,
                "channel": ["app", "web"][ev_idx % 2],
                "page_url": f"/page{ev_idx % 5 + 1}",
            })
    return sorted(events, key=lambda x: x["timestamp"])


def _session_summary(sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """会话清单摘要（不含事件明细），用于边界诊断产物与回放核对。"""
    return [
        {
            "session_key": s["session_key"],
            "event_count": s["event_count"],
            "start_time": s["start_time"],
            "end_time": s["end_time"],
            "duration": s["duration"],
        }
        for s in sessions
    ]


def split_sessions(strategy_id: str, events: list[dict[str, Any]] | None = None) -> SessionResult:
    """执行会话切割，并发布可回放的边界诊断产物。"""
    strategy = store.find_by_field("session_split", "strategy_id", strategy_id)
    if strategy is None:
        raise ValueError(f"Strategy not found: {strategy_id}")

    rules = CuttingRules.from_strategy(strategy)
    if events is None:
        events = _generate_raw_events()

    outcome = split_event_stream(events, rules)
    sessions = outcome.sessions

    # 计算统计
    avg_duration = sum(s["duration"] for s in sessions) / len(sessions) if sessions else 0

    result = SessionResult(
        strategy_id=strategy_id,
        total_events=outcome.total_events,
        session_count=len(sessions),
        avg_session_duration=round(avg_duration, 2),
        sessions=sessions[:20],  # 只返回前20个会话
    )

    # 存储产物
    store.set_artifact("session_split", strategy_id, result.model_dump())

    # 存储可回放的边界诊断产物：同一基准的事件 + 规则 + 逐边界判定
    rules_snapshot = rules.to_snapshot()
    store.set_artifact("session_boundaries", strategy_id, {
        "strategy_id": strategy_id,
        "rules": rules_snapshot,
        "events": [dict(e) for e in events],
        "boundaries": outcome.boundaries,
        "sessions": _session_summary(sessions),
        "session_count": len(sessions),
        "total_events": outcome.total_events,
        "fingerprint": fingerprint(events, rules_snapshot),
    })

    # 更新状态
    store.update("session_split", strategy["id"], {
        "status": "已完成",
        "pending": False,
    })

    return result


def get_session_result(strategy_id: str) -> dict[str, Any] | None:
    """获取会话切割结果。"""
    artifact = store.get_artifact("session_split", strategy_id)
    if artifact:
        return artifact
    try:
        result = split_sessions(strategy_id)
        return result.model_dump()
    except ValueError:
        return None


def get_boundary_diagnostics(strategy_id: str) -> dict[str, Any] | None:
    """获取可回放的边界诊断数据（事件、规则基准 + 逐边界判定）。"""
    artifact = store.get_artifact("session_boundaries", strategy_id)
    if artifact:
        return artifact
    try:
        split_sessions(strategy_id)
        return store.get_artifact("session_boundaries", strategy_id)
    except ValueError:
        return None


def replay_session_boundaries(strategy_id: str) -> dict[str, Any]:
    """用诊断产物中留存的同一基准（事件 + 规则）重放切分并核对。"""
    artifact = get_boundary_diagnostics(strategy_id)
    if artifact is None:
        raise ValueError(f"Strategy not found: {strategy_id}")

    rules = CuttingRules.from_snapshot(artifact["rules"])
    outcome = split_event_stream(artifact["events"], rules)

    boundaries_match = outcome.boundaries == artifact["boundaries"]
    sessions_match = _session_summary(outcome.sessions) == artifact["sessions"]
    fingerprint_match = fingerprint(artifact["events"], artifact["rules"]) == artifact["fingerprint"]

    return {
        "strategy_id": strategy_id,
        "consistent": boundaries_match and sessions_match and fingerprint_match,
        "checks": {
            "boundaries": {"match": boundaries_match, "count": len(outcome.boundaries)},
            "sessions": {"match": sessions_match, "count": len(outcome.sessions)},
            "fingerprint": {"match": fingerprint_match, "value": artifact["fingerprint"]},
        },
    }


def resegment_with_strategy(strategy_id: str, new_timeout: int | None = None) -> SessionResult:
    """使用新参数重新切割会话。"""
    strategy = store.find_by_field("session_split", "strategy_id", strategy_id)
    if strategy is None:
        raise ValueError(f"Strategy not found: {strategy_id}")

    if new_timeout is not None:
        store.update("session_split", strategy["id"], {"timeout_sec": new_timeout})
        store.invalidate_compute_cache(f"session_split:{strategy_id}")

    return split_sessions(strategy_id)
