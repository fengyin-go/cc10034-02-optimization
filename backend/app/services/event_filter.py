"""事件过滤领域服务：对变更事件执行黑白名单过滤，并产出批次过滤结果集。

上游：change_event + format_adapter 产出已适配事件 → event_filter 过滤
下游：route_dispatcher 消费过滤结果集，只投递已明确放行的事件
审计：audit_trail 记录过滤操作和规则版本变化

关键约束：
- 批次接收时冻结规则版本，规则更新期间重复提交同批同源事件只留一笔有效处置；
- 命中记录带出生效条件、优先级和作用域，同级白/黑名单冲突按黑名单优先并可回查；
- 过滤结果集与出站清单的放行事件必须一一对应。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.pipeline import pipeline_bus
from app.filter_engine import (
    build_rule_version,
    evaluate_event,
    match_rule,
    normalize_event,
    normalize_rule,
)


class EventFilterService(BaseModuleService):
    MODULE = "event_filter"
    ENTITY = "过滤规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "过滤类型", "匹配表达式", "是否白名单", "优先级", "生效范围", "备注说明"]
    REQUIRED_FIELDS = ["规则名称", "过滤类型", "匹配表达式"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用规则": "已启用", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def apply_filter(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """对单个变更事件应用单条过滤规则。

        单条规则只回答「这条规则怎么说」，最终是否放行由整批规则共同判定；
        这里保留单规则视图，方便规则维护时做表达式预检。
        """
        rule = normalize_rule(config)
        normalized_event = normalize_event(event)
        verdict = evaluate_event(normalized_event, [rule])
        hit = next(
            (item for item in verdict["hits"] if item["rule_id"] == rule["rule_id"]),
            None,
        )
        return {
            "event_id": normalized_event["event_id"],
            "rule": rule["rule_name"],
            "filter_type": rule["filter_type"],
            "matched": hit is not None and hit["state"] == "hit",
            "scope": rule["scope"],
            "priority": rule["priority"],
            "effective_condition": hit["effective_condition"] if hit else "",
            "verdict": hit["verdict"] if hit else "不参与",
            "passed": hit["verdict"] == "放行" if hit else True,
            "applied_at": datetime.now().isoformat(),
        }

    def batch_filter(
        self,
        events: list[dict[str, Any]],
        configs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """按给定规则集批量过滤事件流，返回逐条判定与汇总。

        不传 configs 时使用当前启用规则；规则集在进入判定前冻结成版本快照，
        同一批内不会因中途改规则而漂移。
        """
        version = build_rule_version(
            configs if configs is not None else pipeline_bus._enabled_filter_rules()
        )
        records: list[dict[str, Any]] = []
        for position, event in enumerate(events, start=1):
            verdict = evaluate_event(normalize_event(event), version["rules"])
            records.append({
                "record_no": position,
                "event_id": event.get("事件ID", ""),
                "allowed": verdict["allowed"],
                "verdict": verdict["verdict"],
                "decisive_rule": verdict["decisive_rule"],
                "conflict": verdict["conflict"],
                "hits": verdict["hits"],
                "scope_out": verdict["scope_out"],
            })
        return self._summarize(version["version_id"], records)

    def receive_batch(
        self,
        events: list[dict[str, Any]],
        *,
        source: str = "change_event",
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """接收一个变更事件批次，冻结规则版本并生成过滤结果清单。"""
        return pipeline_bus.receive_filter_batch(events, source=source, batch_id=batch_id)

    def list_batches(self, limit: int = 50) -> list[dict[str, Any]]:
        return pipeline_bus.list_filter_batches(limit=limit)

    def result_set(self, batch_id: str) -> dict[str, Any]:
        return pipeline_bus.filter_result_set(pipeline_bus.resolve_batch_id(batch_id))

    def reconcile(self, batch_id: str) -> dict[str, Any]:
        """过滤结果清单与出站清单的一致性核对。"""
        return pipeline_bus.reconcile_batch(pipeline_bus.resolve_batch_id(batch_id))

    def rule_version(self) -> dict[str, Any]:
        version = build_rule_version(pipeline_bus._enabled_filter_rules())
        return {
            "version_id": version["version_id"],
            "rule_count": version["rule_count"],
            "rule_ids": version["rule_ids"],
            "excluded_rule_ids": version["excluded_rule_ids"],
            "rules": [
                {
                    "rule_id": rule["rule_id"],
                    "rule_name": rule["rule_name"],
                    "filter_type": rule["filter_type"],
                    "expression": rule["expression"],
                    "is_whitelist": rule["is_whitelist"],
                    "priority": rule["priority"],
                    "scope": rule["scope"]["label"],
                }
                for rule in version["rules"]
            ],
            "generated_at": version["frozen_at"],
        }

    def compare_rule_versions(self, batch_id: str) -> dict[str, Any]:
        """核对规则改动前后同一批次的判定差异。"""
        return pipeline_bus.recompute_batch(pipeline_bus.resolve_batch_id(batch_id))

    def compute_filter_effectiveness(self, rule_name: str) -> dict[str, Any]:
        """计算过滤规则有效性。"""
        total_filtered = pipeline_bus.pipeline_metrics["filtered"]
        total_input = pipeline_bus.pipeline_metrics["total_input"]
        batches = pipeline_bus.list_filter_batches(limit=1000)
        hit_count = sum(
            1
            for batch in pipeline_bus.filter_batches.values()
            for record in batch["records"]
            if any(hit["rule_name"] == rule_name for hit in record["hits"])
        )
        return {
            "rule": rule_name,
            "batch_hit_count": hit_count,
            "batch_count": len(batches),
            "total_filtered": total_filtered,
            "total_input": total_input,
            "filter_rate_pct": round(total_filtered / max(total_input, 1) * 100, 1),
        }

    def validate_expression(self, expression: str, sample_event: dict[str, Any]) -> dict[str, Any]:
        """验证过滤表达式。"""
        rule = normalize_rule({
            "规则名称": "表达式预检",
            "过滤类型": "表名",
            "匹配表达式": expression,
            "是否白名单": "是",
            "优先级": "0",
            "生效范围": "全部",
            "status": "已启用",
        })
        state, reason = match_rule(rule, normalize_event(sample_event))
        return {
            "expression": expression,
            "syntax_valid": len(expression) > 0,
            "matches_sample": state == "hit",
            "sample_used": sample_event.get("来源表", "none"),
            "rule_state": state,
            "reason": reason,
        }

    def _summarize(self, version_id: str, records: list[dict[str, Any]]) -> dict[str, Any]:
        allowed = [record for record in records if record["allowed"]]
        blocked = [record for record in records if not record["allowed"]]
        return {
            "rule_version": version_id,
            "total": len(records),
            "passed": len(allowed),
            "blocked": len(blocked),
            "conflict_count": sum(1 for record in records if record["conflict"]["present"]),
            "pass_rate_pct": round(len(allowed) / max(len(records), 1) * 100, 1),
            "records": records,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        # 幂等：已处于目标状态时不重复流转，也不产生新的规则版本
        if entry.get("status") == target:
            return entry, f"{self.ENTITY}当前已是「{target}」，无需重复{action}"
        entry["status"] = target
        entry["pending"] = False
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        self._sync_pipeline_rule(entry)

        version = build_rule_version(pipeline_bus._enabled_filter_rules())
        pipeline_bus.write_audit(
            "过滤启用" if action == "启用规则" else "过滤停用",
            entry.get("规则名称", ""),
            f"过滤规则 {entry.get('规则名称')} 已{'启用' if action == '启用规则' else '停用'}，"
            f"当前规则版本 {version['version_id']}",
            "event_filter",
            "已启用" if action == "启用规则" else "已停用",
            remark=f"rule_count={version['rule_count']}",
        )
        return entry, f"{self.ENTITY}已{action}"

    def _sync_pipeline_rule(self, entry: dict[str, Any]) -> None:
        """把模块里的规则行同步到管道规则表（兼容直接读取 filter_rules 的消费方）。"""
        name = entry.get("规则名称")
        for rule in pipeline_bus.filter_rules:
            if rule.get("name") == name:
                rule["enabled"] = entry.get("status") == "已启用"
                rule["status"] = entry.get("status")
                return
        pipeline_bus.filter_rules.append({
            "name": name,
            "type": entry.get("过滤类型"),
            "expression": entry.get("匹配表达式"),
            "is_whitelist": entry.get("是否白名单", "否") == "是",
            "priority": int(entry.get("优先级", "0") or 0),
            "enabled": entry.get("status") == "已启用",
            "status": entry.get("status"),
        })
