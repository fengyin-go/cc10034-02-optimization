"""缓慢变化维领域服务：处理维度数据的SCD Type 1/2/3更新策略。

上游：change_event + format_adapter 产出已适配事件 → scd_handler 应用SCD策略
消费：pipeline_bus 中的 SCD 状态存储为下游分析提供维度快照
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class ScdHandlerService(BaseModuleService):
    MODULE = "scd_handler"
    ENTITY = "SCD配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置名称", "维度表", "SCD类型", "自然键", "代理键", "生效时间列", "失效时间列"]
    REQUIRED_FIELDS = ["配置名称", "维度表", "SCD类型"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用配置": "草稿", "停用配置": "已启用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    def apply_scd(self, config: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
        """根据配置的SCD类型处理变更事件。

        读取事件中的变更前后镜像和自然键，
        对维度表执行 Type1(覆盖)、Type2(历史)、Type3(前值列) 策略。
        """
        scd_type = config.get("SCD类型", "Type2")
        dim_table = config["维度表"]
        natural_keys = config.get("自然键", "id").split(",")
        effective_col = config.get("生效时间列", "effective_from")
        expire_col = config.get("失效时间列", "effective_to")
        surrogate_key = config.get("代理键", f"sk_{dim_table}")

        dim_key = self._build_dim_key(event, natural_keys)
        row = self._extract_dim_row(event, dim_table)
        row["effective_from"] = datetime.now().isoformat()
        row["effective_to"] = "9999-12-31"

        result: dict[str, Any] = {"dim_table": dim_table, "scd_type": scd_type, "natural_keys": dim_key}

        if scd_type in ("Type1", "ScdType1"):
            updated = pipeline_bus.apply_scd_type1(dim_key, row)
            result["action"] = "overwrite"
            result["row"] = updated
        elif scd_type in ("Type2", "ScdType2"):
            # 关闭旧版本
            existing = pipeline_bus.scd_type2_store.get(dim_key, [])
            if existing:
                old_row = existing[-1]
                old_row["effective_to"] = datetime.now().isoformat()
            history = pipeline_bus.apply_scd_type2(dim_key, row)
            result["action"] = "append_version"
            result["version"] = len(history)
            result["row"] = history[-1]
        elif scd_type in ("Type3", "ScdType3"):
            col_name = effective_col if effective_col else "status"
            updated = pipeline_bus.apply_scd_type3(dim_key, col_name, row)
            result["action"] = "add_previous_column"
            result["row"] = updated
        else:
            result["action"] = "skip"
            result["reason"] = "unknown_scd_type"

        pipeline_bus.pipeline_metrics["scd_processed"] += 1
        pipeline_bus.write_audit(
            f"SCD{scd_type}", dim_key,
            f"维度表{dim_table}应用{scd_type}策略 {result.get('action')}",
            "scd_handler", "已处理"
        )
        return result

    def _build_dim_key(self, event: dict[str, Any], natural_keys: list[str]) -> str:
        parts: list[str] = []
        for nk in natural_keys:
            nk = nk.strip()
            val = event.get(nk, event.get("变更后镜像", {}).get(nk, ""))
            parts.append(str(val))
        return "|".join(parts) if parts else event.get("事件ID", "unknown")

    def _extract_dim_row(self, event: dict[str, Any], dim_table: str) -> dict[str, Any]:
        import json as _json
        after = event.get("变更后镜像", {})
        if isinstance(after, str):
            try:
                after = _json.loads(after)
            except (_json.JSONDecodeError, TypeError):
                after = {}
        row = dict(after) if isinstance(after, dict) else {}
        row["_source_table"] = dim_table
        row["_event_id"] = event.get("事件ID", "")
        row["_operation"] = event.get("操作类型", "")
        return row

    def query_dimension_snapshot(self, dim_table: str, scd_type: str) -> dict[str, Any]:
        """查询维度表的SCD快照。"""
        if scd_type in ("Type1", "ScdType1"):
            store = pipeline_bus.scd_type1_store
        elif scd_type in ("Type2", "ScdType2"):
            current_only = {k: v[-1] if v else {} for k, v in pipeline_bus.scd_type2_store.items()}
            return {"type": "Type2", "dim_table": dim_table, "current_count": len(current_only), "total_history": sum(len(v) for v in pipeline_bus.scd_type2_store.values()), "snapshot": current_only}
        elif scd_type in ("Type3", "ScdType3"):
            store = pipeline_bus.scd_type3_store
        else:
            return {"type": scd_type, "dim_table": dim_table, "count": 0, "snapshot": {}}
        return {"type": scd_type, "dim_table": dim_table, "count": len(store), "snapshot": dict(store)}

    def detect_scd_conflicts(self, natural_keys: list[str], event_batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """检测同一自然键在批次内的冲突更新。"""
        key_counts: dict[str, list[dict[str, Any]]] = {}
        for evt in event_batch:
            key = self._build_dim_key(evt, natural_keys)
            key_counts.setdefault(key, []).append(evt)
        conflicts = []
        for key, evts in key_counts.items():
            if len(evts) > 1:
                conflicts.append({
                    "natural_key": key,
                    "update_count": len(evts),
                    "resolves_to": "last_write_wins",
                    "event_ids": [e.get("事件ID", "") for e in evts],
                })
        return conflicts

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用配置":
            dim_table = entry.get("维度表", "")
            pipeline_bus.write_audit(
                "SCD启用", dim_table,
                f"启用{dim_table}的{entry.get('SCD类型','Type2')}SCD策略",
                "scd_handler", "已启用"
            )
        return entry, msg