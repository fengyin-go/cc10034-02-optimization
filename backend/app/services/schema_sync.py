"""Schema同步领域服务：管理源表到目标表的列级Schema同步。

上游：ddl_tracker 产出列变更分析结果 → schema_sync 消费并生成同步策略
下游：format_adapter 参考同步后的列映射进行格式适配
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class SchemaSyncService(BaseModuleService):
    MODULE = "schema_sync"
    ENTITY = "同步任务"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["任务名称", "源表", "目标表", "列映射", "类型转换", "同步模式", "冲突策略"]
    REQUIRED_FIELDS = ["任务名称", "源表", "目标表"]
    STATUS_ORDER = ["草稿", "同步中", "已完成", "同步失败"]
    ACTION_RULES = {"开始同步": "草稿", "完成同步": "同步中", "标记失败": "同步中"}
    NEGATIVE_ACTIONS = []

    def build_sync_plan(self, config: dict[str, Any]) -> dict[str, Any]:
        """根据源Schema和DDL变更结果构建同步计划。

        读取 ddl_tracker 产出的 schema_registry 中已注册的列变更，
        结合类型转换策略、冲突策略生成同步任务。
        """
        source_table = config.get("源表", "")
        target_table = config.get("目标表", "")
        type_conversion = config.get("类型转换", "直接映射")
        conflict_strategy = config.get("冲突策略", "覆盖目标")

        source_cols = pipeline_bus.schema_registry.get(source_table, [])
        col_mapping: list[dict[str, Any]] = []
        for col_info in source_cols:
            target_type = self.convert_type(col_info.get("type", "VARCHAR"), type_conversion)
            col_mapping.append({
                "source_column": col_info.get("column"),
                "source_type": col_info.get("type"),
                "target_type": target_type,
                "action": col_info.get("action"),
            })

        plan = {
            "task_name": config.get("任务名称"),
            "source_table": source_table,
            "target_table": target_table,
            "column_mapping": col_mapping,
            "mapping_count": len(col_mapping),
            "conflict_strategy": conflict_strategy,
            "generated_ddl": self.generate_sync_ddl(col_mapping, target_table),
            "estimated_impact": self.estimate_sync_impact(col_mapping),
            "generated_at": datetime.now().isoformat(),
        }
        return plan

    def convert_type(self, source_type: str, strategy: str) -> str:
        """类型转换策略执行。

        支持的转换：mysql->hive/postgresql/doris 等目标格式。
        """
        source_upper = source_type.upper()
        type_map = {
            "VARCHAR": {"直接映射": "STRING", "hive": "STRING", "doris": "VARCHAR(65533)"},
            "INT": {"直接映射": "INT", "hive": "INT", "doris": "INT"},
            "BIGINT": {"直接映射": "BIGINT", "hive": "BIGINT", "doris": "BIGINT"},
            "DECIMAL": {"直接映射": "DECIMAL(38,18)", "hive": "DECIMAL(38,18)", "doris": "DECIMAL(38,18)"},
            "DATETIME": {"直接映射": "TIMESTAMP", "hive": "TIMESTAMP", "doris": "DATETIME"},
            "TEXT": {"直接映射": "STRING", "hive": "STRING", "doris": "STRING"},
            "JSON": {"直接映射": "STRING", "hive": "STRING", "doris": "JSON"},
        }
        lower_strategy = strategy.lower()
        for base, targets in type_map.items():
            if base in source_upper:
                return targets.get(lower_strategy, targets.get("直接映射", source_type))
        return "STRING"

    def estimate_sync_impact(self, col_mapping: list[dict[str, Any]]) -> dict[str, Any]:
        """估算同步对下游的影响范围。"""
        add_count = sum(1 for c in col_mapping if c.get("action") == "add")
        drop_count = sum(1 for c in col_mapping if c.get("action") == "drop")
        modify_count = sum(1 for c in col_mapping if c.get("action") == "modify")
        risk_level = "低"
        if drop_count > 0:
            risk_level = "高"
        elif modify_count > 0:
            risk_level = "中"
        return {
            "columns_added": add_count,
            "columns_dropped": drop_count,
            "columns_modified": modify_count,
            "risk_level": risk_level,
            "breaking_change": drop_count > 0,
        }

    def generate_sync_ddl(self, col_mapping: list[dict[str, Any]], target_table: str) -> str:
        """根据列映射生成目标DDL。"""
        cols: list[str] = []
        for cm in col_mapping:
            if cm.get("action") == "drop":
                continue
            cols.append(f"  {cm['source_column']} {cm['target_type']}")
        if not cols:
            return f"-- 无新增列，目标表 {target_table} 不变"
        return f"ALTER TABLE {target_table} ADD COLUMNS (\n" + ",\n".join(cols) + "\n);"

    def detect_type_conflict(self, col_name: str, source_type: str, target_type: str) -> dict[str, Any]:
        """检测列类型冲突。"""
        has_conflict = source_type.upper() != target_type.upper()
        resolution = ""
        if has_conflict:
            if "VARCHAR" in source_type.upper() and "STRING" in target_type.upper():
                resolution = "字符串宽度调整，无数据丢失"
            elif "DECIMAL" in source_type.upper() and "BIGINT" in target_type.upper():
                resolution = "精度可能丢失，建议保留DECIMAL"
            else:
                resolution = "建议人工确认类型转换"
        return {
            "column": col_name,
            "source_type": source_type,
            "target_type": target_type,
            "conflict": has_conflict,
            "resolution": resolution,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "开始同步":
            source_table = entry.get("源表", "")
            existing = pipeline_bus.schema_registry.get(source_table)
            if existing:
                pipeline_bus.write_audit(
                    "Schema同步", source_table,
                    f"检测到 {len(existing)} 条Schema记录，生成列映射同步计划",
                    "schema_sync", "同步中"
                )
        return entry, msg