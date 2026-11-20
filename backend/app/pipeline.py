"""跨模块管道状态总线：服务间通过管道层完成事件、位点、Schema 的发布-消费。"""
from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.filter_engine import (
    VERDICT_ALLOW,
    VERDICT_DENY,
    build_rule_version,
    evaluate_event,
    event_key,
    normalize_event,
    normalize_rule,
)


@dataclass
class ChangeEventRecord:
    """变更事件统一结构。"""
    event_id: str
    source_db: str
    source_table: str
    operation: str  # INSERT / UPDATE / DELETE
    before_image: dict[str, Any] | None
    after_image: dict[str, Any] | None
    tx_id: str
    binlog_file: str
    binlog_pos: int
    gtid: str
    event_time: datetime
    raw_format: str = "debezium"
    columns: list[str] = field(default_factory=list)


@dataclass
class DdlRecord:
    """DDL 变更结构。"""
    record_id: str
    database: str
    table_name: str
    ddl_statement: str
    executed_at: datetime
    affected_columns: int
    rollback_statement: str
    column_changes: list[dict[str, Any]] = field(default_factory=list)
    partition_changes: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PositionSnapshot:
    """位点快照结构。"""
    instance: str
    binlog_name: str
    position: int
    gtid_set: str
    updated_at: datetime
    source: str
    channel_id: int | None = None


@dataclass
class CheckpointRecord:
    """检查点结构。"""
    checkpoint_id: str
    channel_name: str
    binlog_file: str
    offset: int
    confirmed_at: datetime
    committed: bool
    data_checksum: str


class PipelineBus:
    """管道级事件总线：持有事件队列、Schema 注册表、位点注册表、SCD 状态存储。

    各领域服务通过 pipeline_bus 单例完成跨模块数据流通。
    """

    def __init__(self) -> None:
        # 事件队列 — binlog_parser 产出，change_event 消费
        self.event_queue: list[ChangeEventRecord] = []
        self.event_log: list[dict[str, Any]] = []
        # DDL 记录 — ddl_tracker 产出，schema_sync/partition_sync 消费
        self.ddl_log: list[DdlRecord] = []
        # 位点注册 — checkpoint_manager/position_store 写入，cdc_channel/failover_group 消费
        self.positions: dict[str, PositionSnapshot] = {}
        self.checkpoints: list[CheckpointRecord] = []
        # Schema 注册 — schema_sync 写入，ddl_tracker/format_adapter 消费
        self.schema_registry: dict[str, list[dict[str, Any]]] = {}
        # SCD 状态 — scd_handler 核心存储
        self.scd_type1_store: dict[str, dict[str, Any]] = {}  # snapshot
        self.scd_type2_store: dict[str, list[dict[str, Any]]] = {}  # history
        self.scd_type3_store: dict[str, dict[str, Any]] = {}  # prev-col
        # 去重状态 — dedup_window 核心存储
        self.dedup_state: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # 路由规则 — route_dispatcher 核心配置
        self.routing_table: list[dict[str, Any]] = []
        # 过滤规则 — event_filter 核心配置
        self.filter_rules: list[dict[str, Any]] = []
        # 过滤批次 — event_filter 产出，route_dispatcher 消费
        self.filter_batches: dict[str, dict[str, Any]] = {}
        self.batch_sequence: int = 0
        # 出站清单 — route_dispatcher 回写，一条有效事件一笔处置
        self.outbound: list[dict[str, Any]] = []
        # 快照比对结果
        self.compare_results: list[dict[str, Any]] = []
        # 审计日志
        self.audit_log: list[dict[str, Any]] = []
        # 延迟指标缓冲
        self.lag_buffer: list[dict[str, Any]] = []
        # 解析统计
        self.parse_stats: dict[str, Any] = {
            "total_parsed": 0,
            "total_events": 0,
            "parse_errors": 0,
            "last_batch_time": None,
        }
        # 管道运行时统计
        self.pipeline_metrics: dict[str, Any] = {
            "total_input": 0,
            "total_output": 0,
            "filtered": 0,
            "routed": 0,
            "scd_processed": 0,
            "dedup_hits": 0,
            "backfill_rows": 0,
            "batch_conflicts": 0,
            "batch_replays": 0,
        }

    def publish_event(self, event: ChangeEventRecord) -> dict[str, Any]:
        """将解析产出的事件发布到队列。返回发布确认。"""
        self.event_queue.append(event)
        self.pipeline_metrics["total_input"] += 1
        self.parse_stats["total_parsed"] += 1
        self.parse_stats["last_batch_time"] = datetime.now().isoformat()
        return {
            "event_id": event.event_id,
            "position": event.binlog_pos,
            "general": self.pipeline_metrics["total_input"],
        }

    def consume_events(self, batch_size: int = 100) -> list[ChangeEventRecord]:
        """从事件队列消费变更事件。返回批量事件。"""
        batch = self.event_queue[:batch_size]
        self.event_queue = self.event_queue[batch_size:]
        self.pipeline_metrics["total_output"] += len(batch)
        return batch

    def register_position(self, pos: PositionSnapshot) -> None:
        """注册存储位点。"""
        self.positions[pos.instance] = pos

    def get_position(self, instance: str) -> PositionSnapshot | None:
        """查询当前位点。"""
        return self.positions.get(instance)

    def calc_position_lag(self, source_pos: PositionSnapshot, target_pos: PositionSnapshot) -> int:
        """计算两个位点之间的落后字节数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1  # 跨文件无法直接比较
        return abs(source_pos.position - target_pos.position)

    def add_ddl(self, ddl: DdlRecord) -> None:
        """添加 DDL 记录。"""
        self.ddl_log.append(ddl)

    def get_recent_ddls(self, limit: int = 50) -> list[DdlRecord]:
        """查询最近 DDL 记录。"""
        return self.ddl_log[-limit:]

    def compute_event_hash(self, event: ChangeEventRecord) -> str:
        """计算事件的业务去重哈希。"""
        raw = f"{event.event_id}:{event.source_table}:{event.operation}:{event.binlog_file}:{event.binlog_pos}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def apply_scd_type1(self, dim_key: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type1：覆盖式更新。"""
        existing = self.scd_type1_store.get(dim_key)
        if existing:
            existing.update(row)
        else:
            existing = row
        self.scd_type1_store[dim_key] = existing
        return existing

    def apply_scd_type2(self, dim_key: str, row: dict[str, Any]) -> list[dict[str, Any]]:
        """SCD Type2：保留完整历史。"""
        history = self.scd_type2_store.setdefault(dim_key, [])
        row["version"] = len(history) + 1
        history.append(dict(row))
        return list(history)

    def apply_scd_type3(self, dim_key: str, col_name: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type3：保留前值列。"""
        existing = self.scd_type3_store.get(dim_key)
        result = dict(row)
        if existing:
            result[f"_previous_{col_name}"] = existing.get(col_name)
        self.scd_type3_store[dim_key] = result
        return result

    def window_dedup(self, dedup_key: str, event_id: str, window_sec: float) -> bool:
        """窗口去重：检查指定 key 在窗口期内是否已有事件。"""
        now = time.time()
        buffer = self.dedup_state[dedup_key]
        buffer[:] = [e for e in buffer if now - e["ts"] < window_sec]
        for existing in buffer:
            if existing["event_id"] == event_id:
                self.pipeline_metrics["dedup_hits"] += 1
                return True  # 重复
        buffer.append({"event_id": event_id, "ts": now})
        return False

    def evaluate_filter(self, event: ChangeEventRecord) -> tuple[bool, list[str]]:
        """按当前启用的规则评估单个事件，返回 (是否放行, 命中规则名列表)。

        判定逻辑统一收敛到 filter_engine，冲突口径与其他入口保持一致。
        """
        verdict = evaluate_event(
            normalize_event({
                "事件ID": event.event_id,
                "来源表": f"{event.source_db}.{event.source_table}".strip("."),
                "操作类型": event.operation,
                "变更前镜像": event.before_image,
                "变更后镜像": event.after_image,
                "事务ID": event.tx_id,
                "binlog_file": event.binlog_file,
                "binlog_pos": event.binlog_pos,
            }),
            [normalize_rule(rule) for rule in self._enabled_filter_rules()],
        )
        if not verdict["allowed"]:
            self.pipeline_metrics["filtered"] += 1
        return verdict["allowed"], [hit["rule_name"] for hit in verdict["hits"]]

    def _enabled_filter_rules(self) -> list[dict[str, Any]]:
        """合并规则源：优先取 event_filter 模块登记的规则行。

        规则行是唯一事实源；只有模块里还没有任何规则时才回退到
        pipeline_bus.filter_rules（兼容早期直接塞配置的调用方）。
        """
        try:
            from app.store import store
            rows = store.rows("event_filter")
        except Exception:  # pragma: no cover - 循环导入兜底
            rows = []
        if rows:
            return [dict(row) for row in rows]
        return [dict(rule) for rule in self.filter_rules]

    def freeze_rule_version(self) -> dict[str, Any]:
        """冻结当前有效规则版本，供批次接收时绑定。"""
        return build_rule_version(self._enabled_filter_rules())

    def receive_filter_batch(
        self,
        events: list[dict[str, Any]],
        *,
        source: str = "change_event",
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """接收一个变更事件批次并产出过滤结果清单。

        - 批次接收时冻结规则版本，后续规则改动不影响本批结论；
        - 同批同源的重复提交只在结果清单里留一笔有效处置，
          重复事件记入 replayed 列表并指向首次处置的记录号。
        """
        version = self.freeze_rule_version()
        normalized = [normalize_event(row) for row in events]
        signature = hashlib.sha256(
            json.dumps(
                {
                    "source": source,
                    "events": sorted(event_key(event) for event in normalized),
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:16]

        existing = None
        if batch_id is None:
            # 同源同事件集视为同一批次：规则更新期间重复提交也只留一笔有效处置，
            # 结论仍按首次接收时冻结的规则版本，不被后来的规则改动漂移。
            for batch in self.filter_batches.values():
                if (
                    batch["source"] == source
                    and batch.get("event_signature") == signature
                ):
                    existing = batch
                    break
        else:
            existing = self.filter_batches.get(batch_id)

        if existing is not None and existing.get("event_signature") == signature:
            self.pipeline_metrics["batch_replays"] += 1
            existing["replay_count"] = int(existing.get("replay_count", 0)) + 1
            existing["last_replayed_at"] = datetime.now().isoformat()
            return {
                "batch_id": existing["batch_id"],
                "replayed": True,
                "replay_of": existing["batch_id"],
                "records": existing["records"],
                "summary": existing["summary"],
                "rule_version": existing["batch_rule_version"],
            }

        self.batch_sequence += 1
        resolved_id = batch_id or (
            f"fb_{datetime.now().strftime('%Y%m%d%H%M%S')}_{self.batch_sequence:04d}"
        )

        records: list[dict[str, Any]] = []
        record_index: dict[str, dict[str, Any]] = {}
        deduped: list[dict[str, Any]] = []
        for position, event in enumerate(normalized, start=1):
            key = event_key(event)
            if key in record_index:
                first = record_index[key]
                self.pipeline_metrics["batch_replays"] += 1
                deduped.append({
                    "event_id": event["event_id"],
                    "source_table": event["source_table"],
                    "operation": event["operation"],
                    "duplicate_of": first["record_no"],
                    "reason": "同批同源事件重复提交，沿用首次处置",
                })
                continue
            verdict = evaluate_event(event, version["rules"])
            if verdict["conflict"]["present"]:
                self.pipeline_metrics["batch_conflicts"] += 1
            record = {
                "record_no": len(records) + 1,
                "batch_id": resolved_id,
                "event_id": event["event_id"],
                "source_table": event["source_table"],
                "operation": event["operation"],
                "tx_id": event["tx_id"],
                "binlog_position": f"{event['binlog_file']}:{event['binlog_pos']}",
                "rule_version": version["version_id"],
                "verdict": verdict["verdict"],
                "allowed": verdict["allowed"],
                "decisive_rule": verdict["decisive_rule"],
                "default_allow": verdict["default_allow"],
                "effective_priority": verdict["effective_priority"],
                "conflict": verdict["conflict"],
                "hits": verdict["hits"],
                "scope_out": verdict["scope_out"],
                "checked_at": datetime.now().isoformat(),
            }
            records.append(record)
            record_index[key] = record

        allowed_records = [record for record in records if record["allowed"]]
        blocked_records = [record for record in records if not record["allowed"]]
        batch = {
            "batch_id": resolved_id,
            "source": source,
            "received_at": datetime.now().isoformat(),
            "event_signature": signature,
            "batch_rule_version": {
                "version_id": version["version_id"],
                "digest": version["digest"],
                "rule_count": version["rule_count"],
                "rule_ids": version["rule_ids"],
                "excluded_rule_ids": version["excluded_rule_ids"],
                "frozen_at": version["frozen_at"],
            },
            "records": records,
            "deduped": deduped,
            "replay_count": 0,
            "summary": {
                "total_submitted": len(events),
                "effective_count": len(records),
                "allowed_count": len(allowed_records),
                "blocked_count": len(blocked_records),
                "duplicate_count": len(deduped),
                "conflict_count": sum(
                    1 for record in records if record["conflict"]["present"]
                ),
                "scope_out_count": sum(len(record["scope_out"]) for record in records),
            },
        }
        self.filter_batches[resolved_id] = batch
        self.pipeline_metrics["filtered"] += len(blocked_records)
        self.write_audit(
            "批次过滤", resolved_id,
            f"批次 {resolved_id} 按规则版本 {version['version_id']} 判定："
            f"有效 {len(records)} 条，放行 {len(allowed_records)} 条，"
            f"拦截 {len(blocked_records)} 条，重复 {len(deduped)} 条",
            "event_filter", "已校验",
            remark=f"conflicts={batch['summary']['conflict_count']}",
        )
        return {
            "batch_id": resolved_id,
            "replayed": False,
            "replay_of": "",
            "records": records,
            "summary": batch["summary"],
            "rule_version": batch["batch_rule_version"],
        }

    def get_filter_batch(self, batch_id: str) -> dict[str, Any] | None:
        return self.filter_batches.get(batch_id)

    def list_filter_batches(self, limit: int = 50) -> list[dict[str, Any]]:
        batches = sorted(
            self.filter_batches.values(),
            key=lambda batch: batch["batch_id"],
            reverse=True,
        )
        return [self.batch_view(batch) for batch in batches[:limit]]

    def batch_view(self, batch: dict[str, Any]) -> dict[str, Any]:
        return {
            "batch_id": batch["batch_id"],
            "source": batch["source"],
            "received_at": batch["received_at"],
            "rule_version": batch["batch_rule_version"]["version_id"],
            "rule_digest": batch["batch_rule_version"]["digest"],
            "rule_count": batch["batch_rule_version"]["rule_count"],
            "summary": batch["summary"],
            "replay_count": batch.get("replay_count", 0),
            "dispatch": batch.get("dispatch"),
        }

    def filter_result_set(self, batch_id: str) -> dict[str, Any]:
        """生成供路由分发消费的过滤结果集。"""
        batch = self.filter_batches.get(batch_id)
        if batch is None:
            raise KeyError(batch_id)
        return {
            "batch_id": batch["batch_id"],
            "rule_version": batch["batch_rule_version"],
            "summary": batch["summary"],
            "allowed": [record for record in batch["records"] if record["allowed"]],
            "blocked": [record for record in batch["records"] if not record["allowed"]],
            "deduped": batch["deduped"],
            "dispatch": batch.get("dispatch"),
        }

    def resolve_batch_id(self, batch_id: str) -> str:
        """允许用「记录号」或「批次号」定位：纯数字按记录号解析到所属批次。"""
        if batch_id in self.filter_batches:
            return batch_id
        for batch in self.filter_batches.values():
            for record in batch["records"]:
                if str(record["record_no"]) == str(batch_id):
                    return batch["batch_id"]
        raise KeyError(batch_id)

    def recompute_batch(self, batch_id: str) -> dict[str, Any]:
        """按当前启用规则重判批次，返回与冻结版本的差异对照。

        只做对照，不改写已冻结批次，避免同一批次出现两套结论。
        """
        batch = self.filter_batches[batch_id]
        current = build_rule_version(self._enabled_filter_rules())
        comparison: list[dict[str, Any]] = []
        for record in batch["records"]:
            event = normalize_event({
                "事件ID": record["event_id"],
                "来源表": record["source_table"],
                "操作类型": record["operation"],
                "binlog_file": record["binlog_position"].rsplit(":", 1)[0],
                "binlog_pos": record["binlog_position"].rsplit(":", 1)[-1],
            })
            verdict = evaluate_event(event, current["rules"])
            comparison.append({
                "record_no": record["record_no"],
                "event_id": record["event_id"],
                "frozen_verdict": record["verdict"],
                "current_verdict": verdict["verdict"],
                "changed": record["verdict"] != verdict["verdict"],
                "frozen_decisive_rule": record["decisive_rule"],
                "current_decisive_rule": verdict["decisive_rule"],
            })
        return {
            "batch_id": batch_id,
            "frozen_rule_version": batch["batch_rule_version"]["version_id"],
            "current_rule_version": current["version_id"],
            "rule_changed": batch["batch_rule_version"]["digest"] != current["digest"],
            "changed_count": sum(1 for item in comparison if item["changed"]),
            "comparison": comparison,
        }

    def dispatch_filter_batch(
        self,
        batch_id: str,
        *,
        channel: str = "default_sink",
        operator: str = "route_dispatcher",
        event_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """路由分发消费过滤结果集：只投递已明确放行的事件，并回写出站清单。

        同一批次重复分发不会重复投递：出站清单按 (批次, 事件, 通道) 唯一。
        过滤结果清单与出站清单的放行事件一一对应。
        """
        batch = self.filter_batches[batch_id]
        selected = set(event_ids) if event_ids is not None else None
        allowed = [
            record for record in batch["records"]
            if record["allowed"] and (selected is None or record["event_id"] in selected)
        ]

        existing = {
            (entry["event_id"], entry["target_channel"])
            for entry in self.outbound
            if entry["batch_id"] == batch_id
        }
        dispatched_at = datetime.now().isoformat()
        newly_dispatched: list[dict[str, Any]] = []
        for record in allowed:
            if (record["event_id"], channel) in existing:
                continue
            entry = {
                "outbound_id": f"ob_{len(self.outbound) + 1:05d}",
                "batch_id": batch_id,
                "record_no": record["record_no"],
                "event_id": record["event_id"],
                "source_table": record["source_table"],
                "operation": record["operation"],
                "verdict": VERDICT_ALLOW,
                "decisive_rule": record["decisive_rule"],
                "rule_version": record["rule_version"],
                "target_channel": channel,
                "dispatched_at": dispatched_at,
                "operator": operator,
            }
            self.outbound.append(entry)
            newly_dispatched.append(entry)

        outbound_ids = {entry["event_id"] for entry in self.outbound if entry["batch_id"] == batch_id}
        blocked_ids = {record["event_id"] for record in batch["records"] if not record["allowed"]}
        all_allowed = {record["event_id"] for record in batch["records"] if record["allowed"]}
        reported = batch.get("dispatch") or {"dispatched_count": 0}
        total_dispatched = int(reported.get("dispatched_count", 0)) + len(newly_dispatched)
        missing = sorted(all_allowed - outbound_ids)
        leaked = sorted(blocked_ids & outbound_ids)

        batch["dispatch"] = {
            "channel": (
                channel if not batch.get("dispatch")
                else batch["dispatch"]["channel"]
                if channel in batch["dispatch"]["channel"].split(",")
                else f"{batch['dispatch']['channel']},{channel}"
            ),
            "dispatched_at": dispatched_at,
            "dispatched_count": total_dispatched,
            "last_round_count": len(newly_dispatched),
            "skipped_count": len(allowed) - len(newly_dispatched),
            "outbound_total": len(outbound_ids),
            "consistent": not missing and not leaked,
            "missing_from_outbound": missing,
            "blocked_but_dispatched": leaked,
        }
        self.pipeline_metrics["routed"] += len(newly_dispatched)
        self.write_audit(
            "路由分发", channel,
            f"批次 {batch_id} 分发 {len(newly_dispatched)} 条放行事件，"
            f"出站清单累计 {len(outbound_ids)} 条，"
            f"一致性={'通过' if batch['dispatch']['consistent'] else '不通过'}",
            operator, "已路由",
            remark=f"skipped={batch['dispatch']['skipped_count']}",
        )
        return {
            "batch_id": batch_id,
            "channel": channel,
            "dispatched": newly_dispatched,
            "skipped": len(allowed) - len(newly_dispatched),
            "dispatch": batch["dispatch"],
        }

    def outbound_list(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        entries = self.outbound
        if batch_id:
            entries = [entry for entry in entries if entry["batch_id"] == batch_id]
        return [dict(entry) for entry in entries]

    def reconcile_batch(self, batch_id: str) -> dict[str, Any]:
        """核对过滤结果清单与出站清单：放行事件必须都有且仅有对应出站记录。

        批次尚未分发时返回 status=未分发；分发后要求放行集合与出站集合一一对应。
        """
        batch = self.filter_batches[batch_id]
        allowed = [record for record in batch["records"] if record["allowed"]]
        blocked = [record for record in batch["records"] if not record["allowed"]]
        outbound = [entry for entry in self.outbound if entry["batch_id"] == batch_id]
        allowed_ids = {record["event_id"] for record in allowed}
        blocked_ids = {record["event_id"] for record in blocked}
        outbound_ids = {entry["event_id"] for entry in outbound}
        dispatched = bool(outbound) or batch.get("dispatch") is not None
        missing = sorted(allowed_ids - outbound_ids) if dispatched else []
        leaked = sorted(blocked_ids & outbound_ids)
        duplicated = sorted(
            event_id for event_id in outbound_ids
            if sum(1 for entry in outbound if entry["event_id"] == event_id) > 1
        )
        consistent = (not missing and not leaked and not duplicated) if dispatched else False
        return {
            "batch_id": batch_id,
            "rule_version": batch["batch_rule_version"]["version_id"],
            "status": "未分发" if not dispatched else ("一致" if consistent else "不一致"),
            "allowed_count": len(allowed_ids),
            "blocked_count": len(blocked_ids),
            "outbound_count": len(outbound_ids),
            "consistent": consistent,
            "missing_from_outbound": missing,
            "blocked_but_dispatched": leaked,
            "duplicated_outbound": duplicated,
            "checked_at": datetime.now().isoformat(),
        }

    def evaluate_route(self, event: ChangeEventRecord) -> list[str]:
        """评估路由规则，返回目标通道列表。"""
        targets: list[str] = []
        for rule in self.routing_table:
            if not rule.get("enabled"):
                continue
            if self._match_route(event, rule):
                targets.append(rule["target"])
        self.pipeline_metrics["routed"] += len(targets)
        return targets

    def _match_route(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配路由规则。"""
        cond = rule.get("condition", "")
        if not cond:
            return False
        return (
            cond in event.source_table
            or cond in event.source_db
            or cond in event.operation
        )

    def write_audit(self, operation: str, target: str, summary: str,
                    source_info: str, result: str, remark: str = "") -> dict[str, Any]:
        """写入审计日志。"""
        entry = {
            "id": len(self.audit_log) + 1,
            "审计时间": datetime.now().isoformat(),
            "操作类型": operation,
            "目标对象": target,
            "变更摘要": summary,
            "操作来源": source_info,
            "结果状态": result,
            "审计备注": remark,
        }
        self.audit_log.append(entry)
        return entry

    def compute_backfill_estimate(self, source_pos: int, target_pos: int,
                                  batch_size: int, avg_row_bytes: int = 256) -> dict[str, Any]:
        """计算回填任务的预估指标。"""
        diff = abs(target_pos - source_pos) if target_pos >= source_pos else 0
        estimated_rows = diff // avg_row_bytes
        estimated_batches = (estimated_rows + batch_size - 1) // batch_size
        return {
            "position_diff_bytes": diff,
            "estimated_rows": estimated_rows,
            "estimated_batches": estimated_batches,
            "estimated_rows_str": f"{estimated_rows:,}",
        }

    def compare_snapshots(self, source_rows: int, target_rows: int,
                          diff_rows: int, sample_ratio: float) -> dict[str, Any]:
        """执行快照比对并计算差异指标。"""
        total = max(source_rows, target_rows, 1)
        diff_pct = round(diff_rows / total * 100, 2)
        return {
            "source_count": source_rows,
            "target_count": target_rows,
            "diff_count": diff_rows,
            "diff_percentage": diff_pct,
            "sample_ratio": sample_ratio,
            "status": "正常" if diff_pct < 5 else "差异发现",
            "checked_at": datetime.now().isoformat(),
        }

    def compute_lag(self, channel_name: str, lag_seconds: float,
                    backlog_events: int, source_pos: str, target_pos: str) -> dict[str, Any]:
        """计算延迟指标并缓冲。"""
        throughput = backlog_events / max(lag_seconds, 1) if lag_seconds > 0 else 0
        metric = {
            "指标时间": datetime.now().isoformat(),
            "所属通道": channel_name,
            "延迟秒数": round(lag_seconds, 1),
            "积压事件数": backlog_events,
            "吞吐量": f"{throughput:.1f}",
            "源位点": source_pos,
            "目标位点": target_pos,
        }
        self.lag_buffer.append(metric)
        if len(self.lag_buffer) > 1000:
            self.lag_buffer = self.lag_buffer[-1000:]
        return metric

    def snapshot(self) -> dict[str, Any]:
        """全管道状态快照。"""
        return {
            "parse_stats": dict(self.parse_stats),
            "pipeline_metrics": dict(self.pipeline_metrics),
            "queue_depth": len(self.event_queue),
            "ddl_count": len(self.ddl_log),
            "position_count": len(self.positions),
            "checkpoint_count": len(self.checkpoints),
            "register_count": len(self.schema_registry),
            "scd_type1_keys": len(self.scd_type1_store),
            "scd_type2_keys": len(self.scd_type2_store),
            "scd_type3_keys": len(self.scd_type3_store),
            "audit_count": len(self.audit_log),
            "lag_buffer_size": len(self.lag_buffer),
            "filter_batch_count": len(self.filter_batches),
            "outbound_count": len(self.outbound),
        }


pipeline_bus = PipelineBus()