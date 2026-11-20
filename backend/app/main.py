"""实时数仓CDC变更捕获中心 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.pipeline import pipeline_bus
from app.routers import ROUTERS
from app.store import store


def bootstrap_reference_batch() -> None:
    """用示例变更事件预置一个参考批次，打开页面即可看到判定口径。

    参考批次只做接收和留档，不自动分发；分发动作留给运营在页面上确认。
    """
    events = store.rows("change_event")
    if events and not pipeline_bus.filter_batches:
        pipeline_bus.receive_filter_batch(events, source="seed-change_event")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    bootstrap_reference_batch()
    yield


app = FastAPI(title="实时数仓CDC变更捕获中心", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# 资源路由含 /api/{module}，必须放在 health / overview 之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)
