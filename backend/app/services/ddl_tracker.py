"""DDL追踪领域服务：追踪和解析DDL变更对Schema的影响。

上游：从管道总线消费 DdlRecord 进行自动分析
下游：schema_sync(列级变更)、partition_sync(分区变更) 消费分析结果
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import DdlRecord, pipeline_bus


class DdlTrackerService(BaseModuleService):
    MODULE = "ddl_tracker"
    ENTITY = "DDL记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录ID", "变更数据库", "变更表名", "DDL语句", "执行时间", "影响列数", "回滚语句"]
    REQUIRED_FIELDS = ["记录ID", "变更数据库", "变更表名"]
    STATUS_ORDER = ["已记录", "已分析", "已通知", "已处理"]
    ACTION_RULES = {"分析变更": "已记录", "发送通知": "已分析", "完成处理": "已分析"}
    NEGATIVE_ACTIONS = []

    def analyze_ddl_impact(self, ddl: DdlRecord) -> dict[str, Any]:
        """分析DDL变更对目标表的影响范围。

        解析 DDL 语句中的列变化、分区变化、索引变化，
        产出影响报告供 schema_sync 和 partition_sync 消费。
        """
        ddl_text = ddl.ddl_statement.upper()
        column_changes: list[dict[str, Any]] = []
        partition_changes: list[dict[str, Any]] = []

        # 检测 ADD COLUMN
        add_cols = re.findall(r'ADD\s+COLUMN\s+(\w+)\s+(\w+)', ddl_text, re.IGNORECASE)
        for col_name, col_type in add_cols:
            column_changes.append({"action": "add", "column": col_name, "type": col_type})
            ddl.column_changes.append({"action": "add", "column": col_name, "type": col_type})

        # 检测 DROP COLUMN
        drop_cols = re.findall(r'DROP\s+(?:COLUMN\s+)?(\w+)', ddl_text, re.IGNORECASE)
        for col_name in drop_cols:
            column_changes.append({"action": "drop", "column": col_name})
            ddl.column_changes.append({"action": "drop", "column": col_name})

        # 检测 MODIFY COLUMN / CHANGE COLUMN
        modify_cols = re.findall(r'(?:MODIFY|CHANGE)\s+(?:COLUMN\s+)?(\w+)\s+(\w+)\s+(\w+)',
                                 ddl_text, re.IGNORECASE)
        for old_name, new_name, col_type in modify_cols:
            column_changes.append({"action": "modify", "column": old_name, "new_type": col_type})
            ddl.column_changes.append({"action": "modify", "column": old_name, "new_type": col_type})

        # 检测 ADD PARTITION
        add_part = re.findall(r'ADD\s+PARTITION', ddl_text, re.IGNORECASE)
        if add_part:
            partition_changes.append({"action": "add_partition"})
            ddl.partition_changes.append({"action": "add_partition"})

        # 检测 DROP PARTITION
        drop_part = re.findall(r'DROP\s+PARTITION\s+(\w+)', ddl_text, re.IGNORECASE)
        for part_name in drop_part:
            partition_changes.append({"action": "drop_partition", "name": part_name})
            ddl.partition_changes.append({"action": "drop_partition", "name": part_name})

        affected_cols = len(column_changes)
        rollback = self.generate_rollback(ddl.ddl_statement)

        impact = {
            "record_id": ddl.record_id,
            "database": ddl.database,
            "table": ddl.table_name,
            "column_changes": column_changes,
            "partition_changes": partition_changes,
            "affected_columns": affected_cols,
            "rollback_statement": rollback,
            "has_schema_impact": len(column_changes) > 0,
            "has_partition_impact": len(partition_changes) > 0,
            "analyzed_at": datetime.now().isoformat(),
        }

        # 对 schema_sync 有影响的DDL，写入管道供冲突检测消费
        if impact["has_schema_impact"]:
            for cc in column_changes:
                pipeline_bus.schema_registry.setdefault(ddl.table_name, []).append({
                    "column": cc.get("column"),
                    "action": cc.get("action"),
                    "type": cc.get("type", ""),
                    "timestamp": datetime.now().isoformat(),
                })

        pipeline_bus.add_ddl(ddl)
        pipeline_bus.write_audit(
            "DDL分析", f"{ddl.database}.{ddl.table_name}",
            f"分析DDL影响：列变更{affected_cols}，分区变更{len(partition_changes)}",
            "ddl_tracker", "已分析"
        )
        return impact

    def track_temporal_ddl(self, table_name: str, from_dt: str, to_dt: str) -> list[dict[str, Any]]:
        """查询指定表在时间范围内的DDL变更序列。"""
        results: list[dict[str, Any]] = []
        recent = pipeline_bus.get_recent_ddls()
        for ddl in recent:
            if ddl.table_name == table_name and from_dt <= ddl.executed_at.isoformat() <= to_dt:
                results.append({
                    "record_id": ddl.record_id,
                    "ddl": ddl.ddl_statement[:200],
                    "executed_at": ddl.executed_at.isoformat(),
                    "affected_columns": ddl.affected_columns,
                })
        return results

    def generate_rollback(self, ddl_stmt: str) -> str:
        """根据DDL语句生成回滚语句。"""
        ddl_upper = ddl_stmt.upper()
        if "ADD COLUMN" in ddl_upper:
            match = re.search(r'ADD\s+COLUMN\s+(\w+)', ddl_stmt, re.IGNORECASE)
            if match:
                return f"ALTER TABLE ... DROP COLUMN {match.group(1)};"
        if "DROP COLUMN" in ddl_upper:
            return "-- 回滚需从备份恢复列定义"
        if "MODIFY" in ddl_upper:
            return "-- 回滚需记录修改前的列类型"
        if "ADD PARTITION" in ddl_upper:
            return "ALTER TABLE ... DROP PARTITION ...;"
        return "-- 自动回滚生成中"

    def record_ddl(self, database: str, table_name: str, ddl_stmt: str) -> DdlRecord:
        """记录并分析一条DDL。"""
        ddl = DdlRecord(
            record_id=f"ddl_{uuid.uuid4().hex[:8]}",
            database=database,
            table_name=table_name,
            ddl_statement=ddl_stmt,
            executed_at=datetime.now(),
            affected_columns=0,
            rollback_statement=self.generate_rollback(ddl_stmt),
        )
        self.analyze_ddl_impact(ddl)
        return ddl

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "发送通知":
            pipeline_bus.write_audit(
                "DDL通知", f"{entry.get('变更数据库', '')}.{entry.get('变更表名', '')}",
                f"DDL记录 {entry_id} 变更通知已发送",
                "ddl_tracker", "已通知"
            )
        return entry, msg