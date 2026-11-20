"""行为路径路由：构建路径Sankey并获取分析结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.schemas import ActionPayload, ActionResult
from app.services.behavior_path import (
    build_path_sankey,
    get_path_result,
    find_common_paths,
    preview_policy_disable,
    get_preview,
    verify_preview,
    replay_preview,
)
from app.store import store

router = APIRouter(prefix="/api/behavior-path", tags=["behavior_path"])


@router.get("")
def index(page: int = 1, size: int = 20) -> dict[str, Any]:
    rows = store.rows("behavior_path")
    total = len(rows)
    start = (page - 1) * size
    return {"items": rows[start : start + size], "total": total, "page": page, "size": size}




@router.get("/export")
def export_entries() -> dict[str, Any]:
    rows = store.rows("behavior_path")
    return {"items": rows, "total": len(rows)}
@router.get("/{entry_id}")
def detail(entry_id: int) -> dict[str, Any]:
    row = store.find("behavior_path", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    return row


@router.post("/{model_id}/build")
def build(model_id: str) -> ActionResult:
    try:
        result = build_path_sankey(model_id)
        return ActionResult(ok=True, message="Built", entry=result.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{model_id}/result")
def result(model_id: str) -> dict[str, Any]:
    res = get_path_result(model_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return res


@router.get("/{model_id}/common-paths")
def common_paths(model_id: str, min_count: int = 3) -> list[dict[str, Any]]:
    return find_common_paths(model_id, min_count)


@router.post("/{model_id}/preview-disable")
def preview_disable(model_id: str, payload: dict[str, Any]) -> ActionResult:
    """预演关闭一条切割规则后会话连边的变化；失败不覆盖已发布产物。"""
    rule = payload.get("rule") or payload.get("disabled_rule")
    try:
        preview = preview_policy_disable(model_id, rule)
        return ActionResult(ok=True, message="Preview", entry=preview)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{model_id}/preview/{rule}")
def preview_detail(model_id: str, rule: str) -> dict[str, Any]:
    preview = get_preview(model_id, rule)
    if preview is None:
        raise HTTPException(status_code=404, detail="Preview not found")
    return preview


@router.post("/{model_id}/preview/{rule}/verify")
def preview_verify(model_id: str, rule: str) -> ActionResult:
    """用同一份历史事件实际重切，核对预演给出的断边变化。"""
    try:
        report = verify_preview(model_id, rule)
        return ActionResult(ok=report["verified"], message="Verified", entry=report)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{model_id}/preview/{rule}/replay")
def preview_replay(model_id: str, rule: str) -> ActionResult:
    """历史重放：用预演留存的基准重算，核对与历史预演一致。"""
    try:
        report = replay_preview(model_id, rule)
        return ActionResult(ok=report["consistent"], message="Replayed", entry=report)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    row = store.find("behavior_path", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    action = payload.action
    row["status"] = action
    row["pending"] = False
    return ActionResult(ok=True, message=action, entry=row)
