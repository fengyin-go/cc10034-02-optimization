"""延迟监控领域服务：监控CDC通道的同步延迟和吞吐量。

上游：cdc_channel 产生通道状态 → lag_monitor 监控
消费：snapshot_compare 根据延迟情况触发快照比对
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class LagMonitorService(BaseModuleService):
    MODULE = "lag_monitor"
    ENTITY = "延迟指标"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["指标时间", "所属通道", "延迟秒数", "积压事件数", "吞吐量", "源位点", "目标位点"]
    REQUIRED_FIELDS = ["指标时间", "所属通道", "延迟秒数"]
    STATUS_ORDER = ["正常", "延迟告警", "严重延迟"]
    ACTION_RULES = {"确认告警": "延迟告警", "标记恢复": "严重延迟"}
    NEGATIVE_ACTIONS = []

    def collect_lag_metrics(self, channel_name: str) -> dict[str, Any]:
        """采集通道延迟指标。

        从 pipeline_bus 获取事件队列深度和当前位点，
        计算延迟秒数、积压事件数、吞吐量。
        """
        queue_depth = len(pipeline_bus.event_queue)
        parse_total = pipeline_bus.parse_stats["total_parsed"]
        output_total = pipeline_bus.pipeline_metrics["total_output"]
        lag_events = max(parse_total - output_total, 0)
        lag_seconds = max(lag_events / 100.0, 0.5)  # 估算延迟
        throughput = output_total / max(lag_seconds, 1) if lag_seconds > 0 else 0

        source_pos_str = f"mysql-bin.{int(datetime.now().timestamp() % 10000):06d}:{parse_total * 100}"
        target_pos_str = f"mysql-bin.{int(datetime.now().timestamp() % 10000):06d}:{output_total * 100}"

        metric = pipeline_bus.compute_lag(
            channel_name, lag_seconds, lag_events,
            source_pos_str, target_pos_str
        )

        pipeline_bus.write_audit(
            "延迟采集", channel_name,
            f"通道 {channel_name} 延迟 {metric['延迟秒数']}s 积压 {lag_events} 事件",
            "lag_monitor", "已记录"
        )
        return metric

    def evaluate_lag_threshold(self, metric: dict[str, Any]) -> dict[str, Any]:
        """评估延迟阈值是否超标。"""
        lag_sec = float(metric.get("延迟秒数", 0))
        backlog = int(metric.get("积压事件数", 0))
        status = "正常"
        if lag_sec >= 300:
            status = "严重延迟"
        elif lag_sec >= 60:
            status = "延迟告警"
        severity = 0 if status == "正常" else (1 if status == "延迟告警" else 2)
        return {
            "channel": metric.get("所属通道"),
            "lag_sec": lag_sec,
            "backlog": backlog,
            "status": status,
            "severity_level": severity,
            "threshold_breached": severity > 0,
        }

    def compute_lag_trend(self, buffer_count: int = 20) -> dict[str, Any]:
        """计算延迟趋势。"""
        recent = pipeline_bus.lag_buffer[-buffer_count:]
        if len(recent) < 2:
            return {"trend": "insufficient_data", "count": len(recent)}
        first_lag = float(recent[0].get("延迟秒数", 0))
        last_lag = float(recent[-1].get("延迟秒数", 0))
        diff = last_lag - first_lag
        trend = "稳定"
        if diff > 10:
            trend = "恶化"
        elif diff < -10:
            trend = "改善"
        return {
            "window_count": len(recent),
            "first_lag": first_lag,
            "last_lag": last_lag,
            "delta": round(diff, 1),
            "trend": trend,
            "avg_lag": round(sum(float(m.get("延迟秒数", 0)) for m in recent) / len(recent), 1),
        }

    def alert_if_needed(self, channel_name: str, threshold_sec: float = 60.0) -> dict[str, Any]:
        """按需触发告警。"""
        metric = self.collect_lag_metrics(channel_name)
        evaluation = self.evaluate_lag_threshold(metric)
        if evaluation["severity_level"] >= 1:
            pipeline_bus.write_audit(
                "延迟告警", channel_name,
                f"通道 {channel_name} 延迟 {evaluation['lag_sec']}s 超过阈值",
                "lag_monitor", "已告警"
            )
        return {
            "metric": metric,
            "evaluation": evaluation,
            "alert_triggered": evaluation["severity_level"] >= 1,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "确认告警":
            pipeline_bus.write_audit(
                "延迟确认", entry.get("所属通道", ""),
                f"延迟告警 {entry_id} 已确认",
                "lag_monitor", "已确认"
            )
        return entry, msg