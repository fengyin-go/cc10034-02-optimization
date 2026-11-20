"""路由分发领域服务：消费过滤结果集，把明确放行的事件投递到目标通道。

上游：dedup_window(去重) → event_filter(过滤批次) → route_dispatcher(分发)
下游：Kafka/MQ 消费端，出站清单回写到 pipeline_bus.outbound

分发口径：
- 只消费 event_filter 产出的过滤结果集，拦截记录一律不投递；
- 目标通道来自路由规则匹配，没有命中路由时落到默认通道；
- 出站清单按 (批次, 事件) 唯一，重复分发只跳过、不重复投递。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.pipeline import pipeline_bus


class RouteDispatcherService(BaseModuleService):
    MODULE = "route_dispatcher"
    ENTITY = "路由规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "匹配条件", "目标通道", "分发策略", "是否广播", "权重", "备用目标"]
    REQUIRED_FIELDS = ["规则名称", "匹配条件", "目标通道"]
    STATUS_ORDER = ["草稿", "已启用", "已暂停"]
    ACTION_RULES = {"启用规则": "已启用", "暂停规则": "已启用"}
    NEGATIVE_ACTIONS = []

    def resolve_targets(self, event: dict[str, Any]) -> list[str]:
        """按启用的路由规则匹配事件的目标通道。"""
        targets: list[str] = []
        for row in store.rows(self.MODULE):
            if row.get("status") != "已启用":
                continue
            condition = str(row.get("匹配条件", "")).strip()
            if not condition:
                continue
            haystack = " ".join([
                str(event.get("source_table", "")),
                str(event.get("operation", "")),
                str(event.get("event_id", "")),
            ])
            if condition in haystack and row.get("目标通道") not in targets:
                targets.append(str(row.get("目标通道")))
        return targets

    def route_event(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """对单个事件执行路由分发。

        根据规则匹配条件和分发策略，将事件定向到目标通道。
        支持广播、加权分发和备通道降级。
        """
        condition = config.get("匹配条件", "")
        target = config.get("目标通道", "default_sink")
        strategy = config.get("分发策略", "round_robin")
        broadcast = config.get("是否广播", "否") == "是"
        weight = int(config.get("权重", "1") or 1)
        backup = config.get("备用目标", "")

        haystack = " ".join([
            str(event.get("来源表", "")),
            str(event.get("操作类型", "")),
            str(event.get("事件ID", "")),
        ])
        if condition and condition not in haystack:
            return {"routed": False, "reason": "condition_mismatch"}

        route_result: dict[str, Any] = {
            "routed": True,
            "event_id": event.get("事件ID", ""),
            "rule": config.get("规则名称"),
            "strategy": strategy,
            "broadcast": broadcast,
            "targets": [target],
            "routed_at": datetime.now().isoformat(),
        }
        if not broadcast:
            route_result["weight"] = weight
        if backup and not broadcast:
            route_result["backup_target"] = backup

        pipeline_bus.pipeline_metrics["routed"] += 1
        pipeline_bus.write_audit(
            "路由分发", target,
            f"事件 {event.get('事件ID','')} 路由到 {target}",
            "route_dispatcher", "已路由"
        )
        return route_result

    def dispatch_batch(self, batch_id: str, channel: str | None = None) -> dict[str, Any]:
        """按过滤结果集分发一个批次，结果回写出站清单。"""
        resolved = pipeline_bus.resolve_batch_id(batch_id)
        result_set = pipeline_bus.filter_result_set(resolved)

        if channel:
            return pipeline_bus.dispatch_filter_batch(resolved, channel=channel)

        dispatch: dict[str, Any] = {
            "batch_id": resolved,
            "channel": "",
            "dispatched": [],
            "skipped": 0,
            "by_channel": {},
        }
        for record in result_set["allowed"]:
            targets = self.resolve_targets({
                "event_id": record["event_id"],
                "source_table": record["source_table"],
                "operation": record["operation"],
            }) or ["default_sink"]
            for target in targets:
                single = pipeline_bus.dispatch_filter_batch(
                    resolved, channel=target, event_ids=[record["event_id"]])
                dispatch["dispatched"].extend(single["dispatched"])
                dispatch["by_channel"][target] = (
                    dispatch["by_channel"].get(target, 0)
                    + len(single["dispatched"])
                )
                dispatch["skipped"] += single["skipped"]
        batch = pipeline_bus.filter_batches[resolved]
        dispatch["dispatch"] = batch.get("dispatch")
        return dispatch

    def batch_route(self, events: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
        """批量路由事件。

        按策略对事件流执行批量分发，支持负载均衡模式。
        """
        strategy = config.get("分发策略", "round_robin")
        targets = config.get("目标通道", "default").split(",")
        weight = int(config.get("权重", "1") or 1)
        broadcast = config.get("是否广播", "否") == "是"

        by_target: dict[str, int] = {}
        for i, event in enumerate(events):
            if broadcast:
                target = targets[0]
            else:
                idx = (i // weight) % len(targets)
                target = targets[idx]
            by_target[target] = by_target.get(target, 0) + 1

        pipeline_bus.write_audit(
            "批量路由", f"{len(events)}条事件",
            f"批量路由 {len(events)} 条事件到 {len(by_target)} 个目标",
            "route_dispatcher", "已路由"
        )
        return {
            "total_routed": len(events),
            "by_target": by_target,
            "strategy": strategy,
            "broadcast": broadcast,
        }

    def compute_route_distribution(self, configs: list[dict[str, Any]]) -> dict[str, Any]:
        """计算路由分发比例的预期分布。"""
        total_weight = sum(int(c.get("权重", "1") or 1) for c in configs if c.get("status") == "已启用")
        dist: dict[str, Any] = {}
        for c in configs:
            if c.get("status") == "已启用":
                w = int(c.get("权重", "1") or 1)
                name = c.get("规则名称", "")
                dist[name] = {
                    "target": c.get("目标通道"),
                    "weight": w,
                    "share_pct": round(w / max(total_weight, 1) * 100, 1),
                }
        return {"total_weight": total_weight, "distribution": dist}

    def find_fallback_route(self, event: dict[str, Any], primary_target: str) -> dict[str, Any]:
        """查找备选路由：取第一条启用且目标不同的规则。"""
        for row in store.rows(self.MODULE):
            if row.get("status") != "已启用":
                continue
            if row.get("目标通道") == primary_target:
                continue
            return {
                "found": True,
                "fallback": row.get("目标通道"),
                "condition": row.get("匹配条件"),
            }
        return {"found": False}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        if entry.get("status") == target:
            return entry, f"{self.ENTITY}当前已是「{target}」，无需重复{action}"
        entry["status"] = target
        entry["pending"] = False
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        self._sync_pipeline_route(entry)
        pipeline_bus.write_audit(
            "路由启用" if action == "启用规则" else "路由暂停",
            entry.get("目标通道", ""),
            f"路由规则 {entry.get('规则名称')} 已{'启用' if action == '启用规则' else '暂停'}，"
            f"指向 {entry.get('目标通道')}",
            "route_dispatcher",
            "已启用" if action == "启用规则" else "已暂停",
        )
        return entry, f"{self.ENTITY}已{action}"

    def _sync_pipeline_route(self, entry: dict[str, Any]) -> None:
        name = entry.get("规则名称")
        for rule in pipeline_bus.routing_table:
            if rule.get("name") == name:
                rule["enabled"] = entry.get("status") == "已启用"
                return
        pipeline_bus.routing_table.append({
            "name": name,
            "condition": entry.get("匹配条件"),
            "target": entry.get("目标通道"),
            "enabled": entry.get("status") == "已启用",
        })
