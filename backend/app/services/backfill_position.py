"""回填位点领域服务：基于位点范围执行历史数据回填。

上游：position_store 提供位点范围 → backfill_position 执行回填
消费：pipeline_bus 回填行数统计供 lag_monitor 消费
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class BackfillPositionService(BaseModuleService):
    MODULE = "backfill_position"
    ENTITY = "回填任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务名称", "源位点", "目标位点", "扫描范围", "批次大小", "并发度", "预估记录数"]
    REQUIRED_FIELDS = ["任务名称", "源位点", "目标位点"]
    STATUS_ORDER = ["待回填", "回填中", "已完成", "已中断"]
    ACTION_RULES = {"开始回填": "待回填", "中断回填": "回填中", "完成回填": "回填中"}
    NEGATIVE_ACTIONS = []

    def create_backfill_task(self, config: dict[str, Any]) -> dict[str, Any]:
        """创建回填任务并计算预估指标。

        基于源位点和目标位点的差值，结合平均行大小参数
        估算需回填的记录数和批次数。
        """
        task_name = config["任务名称"]
        source_pos_str = config.get("源位点", "0")
        target_pos_str = config.get("目标位点", "0")
        batch_size = int(config.get("批次大小", "1000"))
        parallelism = int(config.get("并发度", "2"))
        avg_row_bytes = 256

        source_pos = self._parse_pos(source_pos_str)
        target_pos = self._parse_pos(target_pos_str)

        estimate = pipeline_bus.compute_backfill_estimate(
            source_pos, target_pos, batch_size, avg_row_bytes
        )

        task = {
            "task_name": task_name,
            "source_pos": source_pos,
            "target_pos": target_pos,
            "batch_size": batch_size,
            "parallelism": parallelism,
            "estimated_rows": estimate["estimated_rows"],
            "estimated_batches": estimate["estimated_batches"],
            "position_diff_bytes": estimate["position_diff_bytes"],
            "created_at": datetime.now().isoformat(),
        }

        pipeline_bus.write_audit(
            "回填任务创建", task_name,
            f"回填任务创建，预估 {estimate['estimated_rows_str']} 条记录",
            "backfill_position", "已记录"
        )
        return task

    def execute_batch(self, task: dict[str, Any], batch_no: int) -> dict[str, Any]:
        """执行单批回填。"""
        batch_size = task["batch_size"]
        pipeline_bus.pipeline_metrics["backfill_rows"] += batch_size
        return {
            "batch_no": batch_no,
            "rows_processed": batch_size,
            "task": task["task_name"],
            "processed_at": datetime.now().isoformat(),
            "cumulative": pipeline_bus.pipeline_metrics["backfill_rows"],
        }

    def estimate_progress(self, task: dict[str, Any], completed_batches: int) -> dict[str, Any]:
        """估算回填进度。"""
        total_batches = task.get("estimated_batches", 1)
        pct = min(round(completed_batches / max(total_batches, 1) * 100, 1), 100.0)
        parallelism = task.get("parallelism", 1)
        eta_batches = max(total_batches - completed_batches, 0) / max(parallelism, 1)
        return {
            "task": task["task_name"],
            "progress_pct": pct,
            "completed_batches": completed_batches,
            "total_batches_estimate": total_batches,
            "eta_remaining_batches": int(eta_batches),
        }

    def validate_position_range(self, source_pos: int, target_pos: int) -> dict[str, Any]:
        """校验位点范围合法性。"""
        if source_pos < 0 or target_pos < 0:
            return {"valid": False, "reason": "位点不能为负"}
        if source_pos > target_pos:
            return {"valid": False, "reason": "源位点不能大于目标位点"}
        return {"valid": True, "range_size_bytes": target_pos - source_pos}

    def _parse_pos(self, pos_str: str) -> int:
        try:
            pos_str = pos_str.replace(",", "").replace("_", "")
            return int(pos_str)
        except (ValueError, TypeError):
            return 0

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "开始回填":
            self.create_backfill_task(entry)
            pipeline_bus.write_audit(
                "回填开始", entry.get("任务名称", ""),
                f"回填任务 {entry_id} 已开始",
                "backfill_position", "回填中"
            )
        elif entry and action == "完成回填":
            pipeline_bus.write_audit(
                "回填完成", entry.get("任务名称", ""),
                f"回填任务 {entry_id} 已完成，累计回填 {pipeline_bus.pipeline_metrics['backfill_rows']} 行",
                "backfill_position", "已完成"
            )
        return entry, msg