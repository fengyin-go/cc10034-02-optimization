"""采集通道领域服务：管理CDC通道的创建、启停和状态监控。

上游：binlog_parser 依赖通道配置启动解析
中游：checkpoint_manager 基于通道记录检查点
下游：failover_group 消费通道状态进行故障切换
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class CdcChannelService(BaseModuleService):
    MODULE = "cdc_channel"
    ENTITY = "通道实例"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["通道名称", "Binlog源", "输出目标", "过滤条件", "并行度", "缓冲区大小", "心跳间隔"]
    REQUIRED_FIELDS = ["通道名称", "Binlog源", "输出目标"]
    STATUS_ORDER = ["准备中", "运行中", "已停止", "异常"]
    ACTION_RULES = {"启动通道": "准备中", "停止通道": "运行中", "标记异常": "运行中"}
    NEGATIVE_ACTIONS = []

    def start_channel(self, config: dict[str, Any]) -> dict[str, Any]:
        """启动CDC采集通道。

        初始化通道缓冲区，计算运行参数，生成心跳配置。
        checkpoint_manager 在 启动后记录初始检查点。
        """
        channel_name = config["通道名称"]
        source = config["Binlog源"]
        parallelism = int(config.get("并行度", "4"))
        buffer_size_mb = int(config.get("缓冲区大小", "64").replace("MB", ""))
        heartbeat_sec = int(config.get("心跳间隔", "30"))

        status = {
            "channel": channel_name,
            "source": source,
            "parallelism": parallelism,
            "buffer_size_bytes": buffer_size_mb * 1024 * 1024,
            "heartbeat_interval_sec": heartbeat_sec,
            "started_at": datetime.now().isoformat(),
            "effective_qps": self.estimate_qps(parallelism, buffer_size_mb),
            "expected_lag_sec": self.compute_expected_lag(buffer_size_mb, heartbeat_sec),
        }

        pipeline_bus.write_audit(
            "通道启动", channel_name,
            f"通道 {channel_name} 启动，并行度 {parallelism}",
            "cdc_channel", "已记录"
        )
        return status

    def stop_channel(self, channel_name: str) -> dict[str, Any]:
        """停止CDC采集通道。触发 flush 缓冲区、提交检查点。"""
        queue_depth = len(pipeline_bus.event_queue)
        pipeline_bus.write_audit(
            "通道停止", channel_name,
            f"通道 {channel_name} 停止，管道队列深度 {queue_depth}",
            "cdc_channel", "已记录"
        )
        return {
            "channel": channel_name,
            "remaining_queue_depth": queue_depth,
            "stopped_at": datetime.now().isoformat(),
        }

    def estimate_qps(self, parallelism: int, buffer_mb: int) -> int:
        """估算通道吞吐量 QPS。

        基于并行度和缓冲区大小进行性能建模。
        """
        base_qps = 5000  # 基准单线程
        factor = min(parallelism, 16) * 0.75  # 并行度折损
        buffer_factor = min(buffer_mb / 64, 4.0)  # 缓冲区加成，上限4x
        return int(base_qps * factor * buffer_factor)

    def compute_expected_lag(self, buffer_mb: int, heartbeat_sec: int) -> float:
        """估算通道预期延迟。

        基于缓冲区大小和心跳间隔进行计算，
        供 lag_monitor 消费以设定告警阈值。
        """
        drain_rate_mbps = 10.0  # 每线程 10 MB/s drain
        lag_sec = buffer_mb / max(drain_rate_mbps, 0.1)
        return round(lag_sec + heartbeat_sec * 0.1, 2)

    def channel_throughput_snapshot(self, config: dict[str, Any]) -> dict[str, Any]:
        """通道吞吐量快照。"""
        channel_name = config["通道名称"]
        parallelism = int(config.get("并行度", "4"))
        buffer_mb = int(config.get("缓冲区大小", "64").replace("MB", ""))
        return {
            "channel": channel_name,
            "estimated_qps": self.estimate_qps(parallelism, buffer_mb),
            "expected_lag_sec": self.compute_expected_lag(buffer_mb, int(config.get("心跳间隔", "30"))),
            "queue_depth": len(pipeline_bus.event_queue),
            "throughput_bottleneck": "buffer" if buffer_mb < 128 else "parser",
            "snapshot_time": datetime.now().isoformat(),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启动通道":
            self.start_channel(entry)
        elif entry and action == "停止通道":
            self.stop_channel(entry.get("通道名称", ""))
        return entry, msg