"""去重窗口领域服务：基于时间窗口对变更事件进行去重。

上游：change_event + scd_handler 产出事件 → dedup_window 去重
消费：route_dispatcher 消费去重后的纯净事件流
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class DedupWindowService(BaseModuleService):
    MODULE = "dedup_window"
    ENTITY = "窗口配置"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["配置名称", "去重键", "窗口大小", "去重策略", "保留版本", "过期清理", "水位字段"]
    REQUIRED_FIELDS = ["配置名称", "去重键", "窗口大小"]
    STATUS_ORDER = ["草稿", "已启用", "已过期"]
    ACTION_RULES = {"启用窗口": "草稿", "标记过期": "已启用"}
    NEGATIVE_ACTIONS = []

    def dedup_events(self, events: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
        """对事件列表执行窗口去重。

        基于去重键和窗口大小，使用 pipeline_bus 的滑动窗口去重能力。
        去重策略支持 first/retain_first、last/retain_last、compact 等。
        """
        dedup_key_field = config.get("去重键", "事件ID")
        window_size_str = config.get("窗口大小", "3600")
        window_sec = self._parse_window(window_size_str)
        strategy = config.get("去重策略", "last").lower()
        watermark_field = config.get("水位字段", "事件时间")
        expire_cleanup = config.get("过期清理", "自动").lower() == "自动"

        seen: set[str] = set()
        grouped: dict[str, list[dict[str, Any]]] = {}
        for evt in events:
            key = str(self._get_key(evt, dedup_key_field))
            grouped.setdefault(key, []).append(evt)

        result: list[dict[str, Any]] = []
        duplicated = 0
        for key, group in grouped.items():
            if not self._within_window(group, waterm_field=watermark_field, window_sec=window_sec):
                result.append(group[-1])
                continue
            if key in seen:
                duplicated += 1
                continue
            if strategy in ("first", "retain_first"):
                pipeline_bus.window_dedup(key, group[0].get("事件ID", ""), window_sec)
                seen.add(key)
                result.append(group[0])
            elif strategy in ("last", "retain_last"):
                pipeline_bus.window_dedup(key, group[-1].get("事件ID", ""), window_sec)
                seen.add(key)
                result.append(group[-1])
            elif strategy == "compact":
                compacted = group[-1]
                for earlier in group[:-1]:
                    compacted.setdefault("_historical_actions", []).append(earlier.get("操作类型", ""))
                seen.add(key)
                result.append(compacted)
            else:
                seen.add(key)
                result.append(group[0])

        if expire_cleanup:
            for key in list(pipeline_bus.dedup_state.keys()):
                pipeline_bus.dedup_state[key] = [
                    e for e in pipeline_bus.dedup_state[key]
                    if time.time() - e["ts"] < window_sec * 2
                ]

        return result

    def _get_key(self, event: dict[str, Any], key_field: str) -> str:
        val = event.get(key_field, "")
        return str(val) if val else ""

    def _within_window(self, events: list[dict[str, Any]], waterm_field: str, window_sec: float) -> bool:
        """判断事件组是否在水位窗口内。"""
        if len(events) <= 1:
            return True
        try:
            t0 = self._parse_ts(events[0].get(waterm_field, ""))
            tn = self._parse_ts(events[-1].get(waterm_field, ""))
            return abs(tn - t0) <= window_sec
        except (ValueError, TypeError):
            return True

    def _parse_ts(self, ts_str: str) -> float:
        """解析时间戳字符串为秒时间戳。"""
        try:
            dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
            return dt.timestamp()
        except (ValueError, TypeError):
            return time.time()

    def _parse_window(self, window_str: str) -> float:
        """解析窗口大小字符串为秒。"""
        w = str(window_str).strip().lower()
        if w.endswith("s"):
            return float(w[:-1])
        if w.endswith("min"):
            return float(w[:-3]) * 60
        if w.endswith("h") or w.endswith("hour"):
            return float(w.replace("hour", "").replace("h", "")) * 3600
        try:
            return float(w)
        except ValueError:
            return 3600.0

    def compute_dedup_rate(self, total_events: int, deduped_count: int) -> dict[str, Any]:
        """计算去重率。"""
        dedup_rate = round(deduped_count / max(total_events, 1) * 100, 2)
        return {
            "total": total_events,
            "deduplicated": deduped_count,
            "dedup_rate_pct": dedup_rate,
            "state_buffer_size": sum(len(v) for v in pipeline_bus.dedup_state.values()),
        }

    def watermark_lag_check(self, config: dict[str, Any]) -> dict[str, Any]:
        """检查水位延迟。"""
        watermark_col = config.get("水位字段", "事件时间")
        window_sec = self._parse_window(config.get("窗口大小", "3600"))
        now = time.time()
        buffer_count = sum(len(v) for v in pipeline_bus.dedup_state.values())
        lag = now - max((e["ts"] for v in pipeline_bus.dedup_state.values() for e in v), default=now)
        return {
            "waterm_field": watermark_col,
            "window_sec": window_sec,
            "current_lag_sec": round(lag, 1),
            "buffer_entries": buffer_count,
            "status": "正常" if lag < window_sec else "水位滞后",
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用窗口":
            key = entry.get("去重键", "事件ID")
            pipeline_bus.write_audit(
                "去重窗口启用", key,
                f"窗口 {entry.get('配置名称')} 已启用，去重键 {key}",
                "dedup_window", "已启用"
            )
        return entry, msg