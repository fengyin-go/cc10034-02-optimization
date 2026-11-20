"""过滤批次接口：接收变更事件批次、查看过滤结果集、触发分发与一致性核对。

这些接口只做参数编排，判定口径全部在 app/filter_engine.py 和 pipeline_bus 里。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.pipeline import pipeline_bus
from app.services import service_for

router = APIRouter(prefix="/api/filter", tags=["事件过滤批次"])


def filter_service():
    service = service_for("event_filter")
    assert service is not None
    return service


class BatchPayload(BaseModel):
    events: list[dict[str, Any]] = Field(default_factory=list)
    source: str = "change_event"
    batch_id: str | None = None


class DispatchPayload(BaseModel):
    batch_id: str
    channel: str | None = None


@router.get("/batches")
def list_batches(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, Any]:
    batches = pipeline_bus.list_filter_batches(limit=limit)
    return {"items": batches, "total": len(pipeline_bus.filter_batches)}


@router.post("/batches")
def receive_batch(payload: BatchPayload) -> dict[str, Any]:
    if not payload.events:
        raise HTTPException(status_code=400, detail="批次事件不能为空")
    result = filter_service().receive_batch(  # type: ignore[attr-defined]
        payload.events, source=payload.source, batch_id=payload.batch_id)
    return result


@router.get("/batches/{batch_id}/result-set")
def result_set(batch_id: str) -> dict[str, Any]:
    try:
        return filter_service().result_set(batch_id)  # type: ignore[attr-defined]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"过滤批次 {batch_id} 不存在") from None


@router.get("/batches/{batch_id}/reconcile")
def reconcile(batch_id: str) -> dict[str, Any]:
    try:
        return filter_service().reconcile(batch_id)  # type: ignore[attr-defined]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"过滤批次 {batch_id} 不存在") from None


@router.get("/batches/{batch_id}/rule-diff")
def rule_diff(batch_id: str) -> dict[str, Any]:
    try:
        return filter_service().compare_rule_versions(batch_id)  # type: ignore[attr-defined]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"过滤批次 {batch_id} 不存在") from None


@router.post("/dispatch")
def dispatch(payload: DispatchPayload) -> dict[str, Any]:
    try:
        resolved = pipeline_bus.resolve_batch_id(payload.batch_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"过滤批次 {payload.batch_id} 不存在") from None
    service = service_for("route_dispatcher")
    return service.dispatch_batch(resolved, channel=payload.channel)  # type: ignore[union-attr]


@router.get("/outbound")
def outbound(batch_id: str | None = Query(default=None)) -> dict[str, Any]:
    entries = pipeline_bus.outbound_list(batch_id)
    return {"items": entries, "total": len(entries)}


@router.get("/rule-version")
def rule_version() -> dict[str, Any]:
    return filter_service().rule_version()  # type: ignore[attr-defined]
