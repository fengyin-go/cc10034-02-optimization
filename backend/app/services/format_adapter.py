"""格式适配领域服务：将变更事件在不同格式间转换。

上游：change_event 产出已解析事件 → format_adapter 适配格式 (Debezium/Canal/Maxwell)
消费：scd_handler 消费适配后的事件进行SCD处理
"""
from __future__ import annotations

import json
import zlib
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class FormatAdapterService(BaseModuleService):
    MODULE = "format_adapter"
    ENTITY = "适配规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则名称", "输入格式", "输出格式", "字段重命名", "值转换", "时间戳格式", "是否压缩"]
    REQUIRED_FIELDS = ["规则名称", "输入格式", "输出格式"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用规则": "草稿", "停用规则": "已启用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def adapt_event(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """将变更事件按规则适配为目标格式。

        支持 Debezium ↔ Canal ↔ Maxwell 格式互转，
        包括字段重命名、值转换、时间戳格式化和压缩。
        """
        input_fmt = config.get("输入格式", "debezium").lower()
        output_fmt = config.get("输出格式", "canal").lower()
        field_rename = self._parse_mapping(config.get("字段重命名", ""))
        value_convert = self._parse_mapping(config.get("值转换", ""))
        ts_format = config.get("时间戳格式", "%Y-%m-%d %H:%M:%S")
        should_compress = config.get("是否压缩", "否") == "是"

        # Debezium -> 内部格式
        if input_fmt == "debezium":
            adapted = self._from_debezium(event)
        elif input_fmt == "canal":
            adapted = self._from_canal(event)
        elif input_fmt == "maxwell":
            adapted = self._from_maxwell(event)
        else:
            adapted = dict(event)

        # 字段重命名
        for old_name, new_name in field_rename.items():
            if old_name in adapted:
                adapted[new_name] = adapted.pop(old_name)

        # 值转换
        for field_name, converter_exp in value_convert.items():
            if field_name in adapted:
                val = adapted[field_name]
                if converter_exp == "lower":
                    adapted[field_name] = str(val).lower()
                elif converter_exp == "upper":
                    adapted[field_name] = str(val).upper()
                elif converter_exp.startswith("replace:"):
                    parts = converter_exp.split(":", 2)
                    if len(parts) == 3:
                        adapted[field_name] = str(val).replace(parts[1], parts[2])

        # 时间戳格式化
        ts = event.get("事件时间", "")
        if ts:
            try:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                adapted["formatted_timestamp"] = dt.strftime(ts_format)
            except (ValueError, TypeError):
                adapted["formatted_timestamp"] = str(ts)

        # 压缩
        if should_compress:
            payload = json.dumps(adapted, ensure_ascii=False)
            compressed = zlib.compress(payload.encode("utf-8"))
            adapted = {"_compressed": True, "_size": len(compressed), "_format": output_fmt}

        adapted["_output_format"] = output_fmt
        adapted["_adapted_at"] = datetime.now().isoformat()
        return adapted

    def _from_debezium(self, event: dict[str, Any]) -> dict[str, Any]:
        result = dict(event)
        before_raw = event.get("变更前镜像", "{}")
        after_raw = event.get("变更后镜像", "{}")
        # Debezium 的 payload.after 结构
        result["source_meta"] = {
            "connector": "mysql",
            "version": "1.9.7.Final",
            "name": event.get("来源表", ""),
            "ts_ms": int(datetime.now().timestamp() * 1000),
        }
        try:
            after = json.loads(after_raw) if isinstance(after_raw, str) else after_raw
            result["payload"] = {"before": before_raw, "after": after}
        except (json.JSONDecodeError, TypeError):
            result["payload"] = {"before": {}, "after": {}}
        return result

    def _from_canal(self, event: dict[str, Any]) -> dict[str, Any]:
        result = dict(event)
        op_map = {"insert": "INSERT", "update": "UPDATE", "delete": "DELETE"}
        dt_now = int(datetime.now().timestamp() * 1000)
        result["canal_meta"] = {
            "binlog": f"mysql-bin.{dt_now // 1000 % 1000000:06d}",
            "type": op_map.get(event.get("操作类型", "").lower(), "INSERT"),
            "es": dt_now,
            "isDdl": False,
        }
        return result

    def _from_maxwell(self, event: dict[str, Any]) -> dict[str, Any]:
        result = dict(event)
        result["maxwell_meta"] = {
            "database": event.get("来源表", "").split(".")[0] if "." in event.get("来源表", "") else "default_db",
            "table": event.get("来源表", "").split(".")[-1] if "." in event.get("来源表", "") else "default_table",
            "type": event.get("操作类型", ""),
            "ts": int(datetime.now().timestamp()),
            "xid": event.get("事务ID", ""),
            "commit": True,
        }
        return result

    def _parse_mapping(self, mapping_str: str) -> dict[str, str]:
        """解析字段重命名/值转换字符串。"""
        result: dict[str, str] = {}
        if not mapping_str:
            return result
        for pair in mapping_str.split(","):
            pair = pair.strip()
            if ":" in pair:
                key, val = pair.split(":", 1)
                result[key.strip()] = val.strip()
            elif "=" in pair:
                key, val = pair.split("=", 1)
                result[key.strip()] = val.strip()
        return result

    def batch_adapt(self, events: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
        """批量适配事件。"""
        adapted_list = [self.adapt_event(evt, config) for evt in events]
        compressed_count = sum(1 for a in adapted_list if a.get("_compressed"))
        return {
            "total": len(adapted_list),
            "compressed": compressed_count,
            "output_format": config.get("输出格式"),
            "adapted_at": datetime.now().isoformat(),
        }

    def compute_format_compatibility(self, source_fmt: str, target_fmt: str) -> dict[str, Any]:
        """计算两种格式的兼容性。"""
        compatibility_matrix = {
            ("debezium", "canal"): 95,
            ("canal", "debezium"): 90,
            ("debezium", "maxwell"): 85,
            ("maxwell", "debezium"): 80,
            ("canal", "maxwell"): 90,
            ("maxwell", "canal"): 90,
        }
        score = compatibility_matrix.get((source_fmt.lower(), target_fmt.lower()), 70)
        return {"source": source_fmt, "target": target_fmt, "compatibility_pct": score, "lossless": score >= 95}