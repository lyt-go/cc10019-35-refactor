"""会话切割路由：执行会话切割并获取结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.schemas import ActionPayload, ActionResult
from app.services.session_split import (
    split_sessions,
    get_session_result,
    get_boundary_diagnostics,
    replay_session_boundaries,
    resegment_with_strategy,
)
from app.store import store, query_filters

router = APIRouter(prefix="/api/session-split", tags=["session_split"])


@router.get("")
def index(request: Request, page: int = 1, size: int = 20) -> dict[str, Any]:
    rows = store.rows("session_split")
    filters=query_filters(request)
    if filters:
        rows = [r for r in rows if all(str(r.get(k)) == str(v) for k, v in filters.items())]
    total = len(rows)
    start = (page - 1) * size
    return {"items": rows[start : start + size], "total": total, "page": page, "size": size}


@router.get("/{entry_id}")
def detail(entry_id: int) -> dict[str, Any]:
    row = store.find("session_split", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    return row


@router.post("/{strategy_id}/run")
def run(strategy_id: str) -> ActionResult:
    try:
        result = split_sessions(strategy_id)
        return ActionResult(ok=True, message="Split", entry=result.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{strategy_id}/result")
def result(strategy_id: str) -> dict[str, Any]:
    res = get_session_result(strategy_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return res


@router.get("/{strategy_id}/boundaries")
def boundaries(strategy_id: str) -> dict[str, Any]:
    """可回放的边界诊断数据：同一基准的事件、规则与逐边界判定。"""
    res = get_boundary_diagnostics(strategy_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Boundaries not found")
    return res


@router.post("/{strategy_id}/replay")
def replay(strategy_id: str) -> ActionResult:
    """用诊断产物中的基准重放切分，核对边界与会话是否一致。"""
    try:
        report = replay_session_boundaries(strategy_id)
        return ActionResult(ok=report["consistent"], message="Replayed", entry=report)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{strategy_id}/resegment")
def resegment(strategy_id: str, payload: dict[str, Any]) -> ActionResult:
    timeout = payload.get("timeout_sec")
    try:
        result = resegment_with_strategy(strategy_id, timeout)
        return ActionResult(ok=True, message="Resegmented", entry=result.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    row = store.find("session_split", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    action = payload.action
    row["status"] = action
    row["pending"] = False
    return ActionResult(ok=True, message=action, entry=row)
