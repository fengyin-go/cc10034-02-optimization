"""过滤批次的处理口径验证：同级交叠、作用域外、两次重投、规则改动前后对照。

运行：backend/.venv/bin/python -m unittest discover -s backend/tests -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.filter_engine import evaluate_event, normalize_event, normalize_rule  # noqa: E402
from app.pipeline import PipelineBus  # noqa: E402


def rule(**kwargs) -> dict:
    base = {
        "id": 1,
        "status": "已启用",
        "规则名称": "规则",
        "过滤类型": "表名",
        "匹配表达式": "ods.orders",
        "是否白名单": "是",
        "优先级": "10",
        "生效范围": "全部",
    }
    base.update(kwargs)
    return normalize_rule(base)


def event(**kwargs) -> dict:
    base = {
        "事件ID": "evt_1",
        "来源表": "ods.orders",
        "操作类型": "INSERT",
        "变更前镜像": "{}",
        "变更后镜像": "{}",
    }
    base.update(kwargs)
    return normalize_event(base)


class ConflictResolutionTest(unittest.TestCase):
    """同级交叠：白名单放行与黑名单拦截在同一优先级、作用域交叠。"""

    def setUp(self) -> None:
        self.rules = [
            rule(id=1, 规则名称="订单表放行", 过滤类型="表名",
                 匹配表达式="ods.orders", 是否白名单="是", 优先级="10", 生效范围="全部"),
            rule(id=2, 规则名称="订单删除拦截", 过滤类型="操作类型",
                 匹配表达式="DELETE", 是否白名单="否", 优先级="10",
                 生效范围="指定表:ods.orders"),
        ]

    def test_overlap_conflict_denies_and_keeps_both_hits(self) -> None:
        verdict = evaluate_event(event(操作类型="DELETE"), self.rules)
        self.assertFalse(verdict["allowed"])
        self.assertTrue(verdict["conflict"]["present"])
        self.assertTrue(verdict["conflict"]["decisive"])
        names = {hit["rule_name"] for hit in verdict["hits"]}
        self.assertEqual(names, {"订单表放行", "订单删除拦截"})
        for hit in verdict["hits"]:
            # 生效条件、优先级、作用域都要进命中记录，供回查
            self.assertTrue(hit["effective_condition"])
            self.assertEqual(hit["priority"], 10)
            self.assertTrue(hit["scope"])
        self.assertEqual(verdict["decisive_rule"], "订单删除拦截")

    def test_non_overlap_allows(self) -> None:
        verdict = evaluate_event(event(操作类型="INSERT"), self.rules)
        self.assertTrue(verdict["allowed"])
        self.assertFalse(verdict["conflict"]["present"])

    def test_same_level_conflict_on_other_table_does_not_leak(self) -> None:
        other = event(事件ID="evt_2", 来源表="ods.refunds", 操作类型="DELETE")
        verdict = evaluate_event(other, self.rules)
        scopes = {hit["rule_name"]: hit["state"] for hit in verdict["scope_out"]}
        self.assertEqual(scopes["订单删除拦截"], "out_of_scope")
        # 订单表白名单作用域内未命中 -> 拦截；不是冲突
        self.assertFalse(verdict["allowed"])
        self.assertFalse(verdict["conflict"]["present"])


class ScopeTest(unittest.TestCase):
    def test_blacklist_out_of_scope_does_not_block(self) -> None:
        rules = [rule(id=1, 规则名称="发货删除拦截", 过滤类型="操作类型",
                      匹配表达式="DELETE", 是否白名单="否", 优先级="5",
                      生效范围="指定表:ods.shipments")]
        verdict = evaluate_event(event(来源表="ods.orders", 操作类型="DELETE"), rules)
        self.assertTrue(verdict["allowed"])
        self.assertTrue(verdict["default_allow"])
        self.assertEqual(verdict["scope_out"][0]["state"], "out_of_scope")
        self.assertIn("ods.shipments", verdict["scope_out"][0]["reason"])

    def test_higher_priority_wins_across_levels(self) -> None:
        rules = [
            rule(id=1, 规则名称="审计放行", 过滤类型="表名", 匹配表达式="ods.audit_log",
                 是否白名单="是", 优先级="20", 生效范围="指定表:ods.audit_log"),
            rule(id=2, 规则名称="订单放行", 过滤类型="表名", 匹配表达式="ods.orders",
                 是否白名单="是", 优先级="10", 生效范围="全部"),
        ]
        verdict = evaluate_event(event(来源表="ods.audit_log"), rules)
        self.assertTrue(verdict["allowed"])
        self.assertEqual(verdict["effective_priority"], 20)
        lower = next(hit for hit in verdict["hits"] if hit["rule_id"] == 2)
        self.assertEqual(lower["verdict"], "拦截")


class BatchLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.bus = PipelineBus()
        self.rules = [
            {"id": 1, "status": "已启用", "规则名称": "订单表放行", "过滤类型": "表名",
             "匹配表达式": "ods.orders", "是否白名单": "是", "优先级": "10", "生效范围": "全部"},
            {"id": 2, "status": "已启用", "规则名称": "订单删除拦截", "过滤类型": "操作类型",
             "匹配表达式": "DELETE", "是否白名单": "否", "优先级": "10",
             "生效范围": "指定表:ods.orders"},
        ]
        self.bus._enabled_filter_rules = lambda: [dict(r) for r in self.rules]  # type: ignore[method-assign]
        self.events = [
            {"事件ID": "evt_insert", "来源表": "ods.orders", "操作类型": "INSERT",
             "binlog_file": "mysql-bin.000001", "binlog_pos": 100},
            {"事件ID": "evt_delete", "来源表": "ods.orders", "操作类型": "DELETE",
             "binlog_file": "mysql-bin.000001", "binlog_pos": 200},
        ]

    def test_batch_freezes_rule_version(self) -> None:
        batch = self.bus.receive_filter_batch(self.events)
        frozen = batch["rule_version"]["version_id"]
        self.rules[1]["status"] = "已停用"
        diff = self.bus.recompute_batch(batch["batch_id"])
        self.assertEqual(diff["frozen_rule_version"], frozen)
        self.assertNotEqual(diff["current_rule_version"], frozen)
        self.assertTrue(diff["rule_changed"])
        self.assertEqual(diff["changed_count"], 1)
        # 冻结批次结论不变
        self.assertEqual(
            [r["verdict"] for r in self.bus.filter_batches[batch["batch_id"]]["records"]],
            ["放行", "拦截"],
        )

    def test_resubmit_before_and_after_rule_change_keeps_one_disposition(self) -> None:
        first = self.bus.receive_filter_batch(self.events, source="change_event")
        self.rules[1]["status"] = "已停用"
        second = self.bus.receive_filter_batch(self.events, source="change_event")
        self.assertTrue(second["replayed"])
        self.assertEqual(second["batch_id"], first["batch_id"])
        self.assertEqual(len(self.bus.filter_batches), 1)
        self.assertEqual(len(self.bus.filter_batches[first["batch_id"]]["records"]), 2)
        self.assertEqual(
            self.bus.filter_batches[first["batch_id"]]["replay_count"], 1)

    def test_duplicate_event_within_payload_single_disposition(self) -> None:
        payload = [self.events[0], dict(self.events[0]), self.events[1]]
        batch = self.bus.receive_filter_batch(payload, source="manual")
        self.assertEqual(batch["summary"]["effective_count"], 2)
        self.assertEqual(batch["summary"]["duplicate_count"], 1)
        self.assertEqual(batch["records"][0]["record_no"], 1)
        self.assertEqual(
            self.bus.filter_batches[batch["batch_id"]]["deduped"][0]["duplicate_of"], 1)

    def test_dispatch_only_allowed_and_outbound_matches(self) -> None:
        batch = self.bus.receive_filter_batch(self.events)
        pending = self.bus.reconcile_batch(batch["batch_id"])
        self.assertFalse(pending["consistent"])
        self.assertEqual(pending["status"], "未分发")
        result = self.bus.dispatch_filter_batch(batch["batch_id"], channel="kafka-orders")
        self.assertEqual([entry["event_id"] for entry in result["dispatched"]], ["evt_insert"])
        reconcile = self.bus.reconcile_batch(batch["batch_id"])
        self.assertTrue(reconcile["consistent"])
        self.assertEqual(reconcile["status"], "一致")
        self.assertEqual(reconcile["allowed_count"], 1)
        self.assertEqual(reconcile["outbound_count"], 1)
        self.assertEqual(reconcile["blocked_but_dispatched"], [])

    def test_repeated_dispatch_does_not_duplicate_outbound(self) -> None:
        batch = self.bus.receive_filter_batch(self.events)
        self.bus.dispatch_filter_batch(batch["batch_id"], channel="kafka-orders")
        again = self.bus.dispatch_filter_batch(batch["batch_id"], channel="kafka-orders")
        self.assertEqual(again["dispatched"], [])
        self.assertEqual(len(self.bus.outbound), 1)
        self.assertTrue(self.bus.reconcile_batch(batch["batch_id"])["consistent"])


if __name__ == "__main__":
    unittest.main()
