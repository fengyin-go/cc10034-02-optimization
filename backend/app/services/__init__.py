"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.binlog_parser import BinlogParserService
from app.services.change_event import ChangeEventService
from app.services.ddl_tracker import DdlTrackerService
from app.services.schema_sync import SchemaSyncService
from app.services.cdc_channel import CdcChannelService
from app.services.checkpoint_manager import CheckpointManagerService
from app.services.position_store import PositionStoreService
from app.services.format_adapter import FormatAdapterService
from app.services.scd_handler import ScdHandlerService
from app.services.dedup_window import DedupWindowService
from app.services.route_dispatcher import RouteDispatcherService
from app.services.backfill_position import BackfillPositionService
from app.services.lag_monitor import LagMonitorService
from app.services.snapshot_compare import SnapshotCompareService
from app.services.event_filter import EventFilterService
from app.services.partition_sync import PartitionSyncService
from app.services.audit_trail import AuditTrailService
from app.services.failover_group import FailoverGroupService

SERVICES: dict[str, BaseModuleService] = {
    "binlog_parser": BinlogParserService(),
    "change_event": ChangeEventService(),
    "ddl_tracker": DdlTrackerService(),
    "schema_sync": SchemaSyncService(),
    "cdc_channel": CdcChannelService(),
    "checkpoint_manager": CheckpointManagerService(),
    "position_store": PositionStoreService(),
    "format_adapter": FormatAdapterService(),
    "scd_handler": ScdHandlerService(),
    "dedup_window": DedupWindowService(),
    "route_dispatcher": RouteDispatcherService(),
    "backfill_position": BackfillPositionService(),
    "lag_monitor": LagMonitorService(),
    "snapshot_compare": SnapshotCompareService(),
    "event_filter": EventFilterService(),
    "partition_sync": PartitionSyncService(),
    "audit_trail": AuditTrailService(),
    "failover_group": FailoverGroupService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
