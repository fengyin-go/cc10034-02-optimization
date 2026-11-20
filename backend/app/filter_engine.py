"""事件过滤判定引擎（纯函数，不依赖全局状态）。

设计要点：
1. 规则按「优先级 + 作用域」参与判定，生效条件随命中记录一起落盘；
2. 同级（同一优先级）白名单放行与黑名单拦截冲突时，黑名单优先（失败关闭），
   冲突双方都写进入批次的命中记录，保证批次交付结论可回查；
3. 白名单只在自身作用域内具备否决权：作用域外的白名单不会误伤其他表/操作的
   事件，记录里会明确标出「作用域外」；
4. 判定仅依赖调用方传入的规则快照，便于在批次接收时冻结规则版本。

作用域写法（生效范围字段）：
- `全部`：规则对全部事件生效；
- `指定表:ods.orders`：仅当事件来源表命中该表集合时参与判定；
- `指定操作:DELETE`：仅当事件操作类型命中该集合时参与判定；
- 也接受省略限定值的 `指定表` / `指定操作`，此时按规则表达式自身的维度推断。

本模块不读写 pipeline_bus，也不抛业务异常，方便单独测试和跨服务复用。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Iterable

SCOPE_ALL = "全部"
SCOPE_TABLE = "指定表"
SCOPE_OPERATION = "指定操作"

TYPE_TABLE = "表名"
TYPE_OPERATION = "操作类型"
TYPE_FIELD = "字段值"

VERDICT_ALLOW = "放行"
VERDICT_DENY = "拦截"

HIT = "hit"
MISS_IN_SCOPE = "miss_in_scope"
OUT_OF_SCOPE = "out_of_scope"

_DIMENSION_LABEL = {
    TYPE_TABLE: "来源表",
    TYPE_OPERATION: "操作类型",
    TYPE_FIELD: "字段值",
}


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip() in {"是", "true", "True", "1", "yes"}


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _loads(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def split_table(full_name: str) -> tuple[str, str]:
    full_name = str(full_name or "").strip()
    if "." in full_name:
        db, table = full_name.rsplit(".", 1)
        return db, table
    return "", full_name


def _tokens(raw: str) -> list[str]:
    return [
        token.strip()
        for token in str(raw or "").replace("；", ",").replace(";", ",").split(",")
        if token.strip()
    ]


def normalize_event(row: dict[str, Any]) -> dict[str, Any]:
    """把变更事件行（中文种子字段或管道字段）统一成判定用结构。"""
    full_table = str(row.get("来源表") or row.get("source_table") or "").strip()
    db, table = split_table(full_table)
    return {
        "event_id": str(row.get("事件ID") or row.get("event_id") or "").strip(),
        "source_table": full_table,
        "source_db": db,
        "table_name": table,
        "operation": str(row.get("操作类型") or row.get("operation") or "").strip().upper(),
        "before_image": _loads(row.get("变更前镜像", row.get("before_image"))),
        "after_image": _loads(row.get("变更后镜像", row.get("after_image"))),
        "tx_id": str(row.get("事务ID") or row.get("tx_id") or ""),
        "event_time": str(row.get("事件时间") or row.get("event_time") or ""),
        "binlog_file": str(row.get("binlog_file") or ""),
        "binlog_pos": _as_int(row.get("binlog_pos"), 0),
    }


def event_key(event: dict[str, Any]) -> str:
    """同批同源事件的稳定标识：事件ID + 来源表 + 操作类型 + 位点。"""
    position = f"{event.get('binlog_file', '')}:{event.get('binlog_pos', 0)}"
    return "|".join([
        str(event.get("event_id", "")),
        str(event.get("source_table", "")),
        str(event.get("operation", "")),
        position,
    ])


def parse_scope(raw_scope: str) -> dict[str, Any]:
    """解析生效范围。支持 `指定表:ods.orders` 这种带限定值的写法。"""
    raw_scope = str(raw_scope or "").strip()
    if not raw_scope or raw_scope == SCOPE_ALL:
        return {"kind": SCOPE_ALL, "label": SCOPE_ALL, "values": []}
    for kind in (SCOPE_TABLE, SCOPE_OPERATION):
        if raw_scope == kind:
            return {"kind": kind, "label": kind, "values": []}
        for separator in (":", "：", "="):
            prefix = f"{kind}{separator}"
            if raw_scope.startswith(prefix):
                values = _tokens(raw_scope[len(prefix):])
                return {"kind": kind, "label": raw_scope, "values": values}
    return {"kind": "unknown", "label": raw_scope, "values": []}


def normalize_rule(row: dict[str, Any]) -> dict[str, Any]:
    """把过滤规则行统一成判定用结构。

    同时兼容中文规则行字段和管道里的英文键（name/type/enabled），
    这样直接塞进 pipeline_bus.filter_rules 的规则也能参与判定。
    """
    status = str(row.get("status") or "").strip()
    if not status and "enabled" in row:
        status = "已启用" if row.get("enabled") else "已停用"
    return {
        "rule_id": _as_int(row.get("id"), 0),
        "rule_name": str(row.get("规则名称") or row.get("rule_name") or row.get("name") or "").strip(),
        "filter_type": str(
            row.get("过滤类型") or row.get("filter_type") or row.get("type") or TYPE_TABLE
        ).strip(),
        "expression": str(row.get("匹配表达式") or row.get("expression") or "").strip(),
        "is_whitelist": _as_bool(row.get("是否白名单", row.get("is_whitelist", False))),
        "priority": _as_int(row.get("优先级", row.get("priority")), 0),
        "scope": parse_scope(str(row.get("生效范围") or row.get("scope") or SCOPE_ALL)),
        "status": status,
        "remark": str(row.get("备注说明") or row.get("remark") or ""),
    }


def rule_enabled(rule: dict[str, Any]) -> bool:
    return rule.get("status") == "已启用"


def build_rule_version(rule_rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """冻结一份规则版本快照。

    版本号由「启用规则的有效字段」决定：同样的规则集得到同样的版本号，
    规则增删改后版本号随之变化，便于回查某批次到底按哪一版规则判定。
    """
    all_rules = [normalize_rule(row) for row in rule_rows]
    effective = sorted(
        (rule for rule in all_rules if rule_enabled(rule)),
        key=lambda rule: (-rule["priority"], rule["rule_id"], rule["rule_name"]),
    )
    canonical = [
        {
            "rule_id": rule["rule_id"],
            "rule_name": rule["rule_name"],
            "filter_type": rule["filter_type"],
            "expression": rule["expression"],
            "is_whitelist": rule["is_whitelist"],
            "priority": rule["priority"],
            "scope": rule["scope"]["label"],
        }
        for rule in effective
    ]
    payload = json.dumps(canonical, ensure_ascii=False, sort_keys=True)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]
    return {
        "version_id": f"rv_{digest}",
        "digest": digest,
        "rule_count": len(effective),
        "rules": effective,
        "rule_ids": [rule["rule_id"] for rule in effective],
        "excluded_rule_ids": [rule["rule_id"] for rule in all_rules if not rule_enabled(rule)],
        "frozen_at": datetime.now().isoformat(),
    }


def _table_matches(tokens: Iterable[str], event: dict[str, Any]) -> bool:
    full = str(event.get("source_table", "")).lower()
    table = str(event.get("table_name", "")).lower()
    for token in tokens:
        candidate = token.strip().lower()
        if not candidate:
            continue
        if candidate == full or candidate == table:
            return True
        if candidate.startswith("*.") and full.endswith(candidate[1:]):
            return True
        if candidate.startswith("*") and full.endswith(candidate.lstrip("*")):
            return True
    return False


def _operation_matches(tokens: Iterable[str], event: dict[str, Any]) -> bool:
    operation = str(event.get("operation", "")).upper()
    return operation in {token.strip().upper() for token in tokens if token.strip()}


def _field_matches(tokens: Iterable[str], event: dict[str, Any]) -> bool:
    """字段值匹配：支持 field=value / field:value / 值本身 / 原文子串。"""
    images = [event.get("after_image") or {}, event.get("before_image") or {}]
    for token in tokens:
        expression = token.strip()
        if not expression:
            continue
        lowered = expression.lower()
        for image in images:
            for key, value in image.items():
                if str(value).lower() == lowered:
                    return True
                if f"{key}={value}".lower() == lowered:
                    return True
                if f"{key}:{value}".lower() == lowered:
                    return True
        raw = " ".join(
            json.dumps(image, ensure_ascii=False, sort_keys=True) for image in images
        ).lower()
        if lowered in raw:
            return True
        for separator in ("=", ":"):
            if separator in expression:
                field, expected = expression.split(separator, 1)
                field, expected = field.strip(), expected.strip().lower()
                for image in images:
                    if field in image and str(image[field]).lower() == expected:
                        return True
    return False


def _dimension_matches(rule: dict[str, Any], event: dict[str, Any]) -> bool:
    tokens = _tokens(rule["expression"])
    if rule["filter_type"] == TYPE_OPERATION:
        return _operation_matches(tokens, event)
    if rule["filter_type"] == TYPE_FIELD:
        return _field_matches(tokens, event)
    return _table_matches(tokens, event)


def _scope_probe(rule: dict[str, Any], event: dict[str, Any]) -> tuple[bool, str]:
    """判断规则作用域是否覆盖事件，返回 (是否覆盖, 不覆盖原因)。"""
    scope = rule["scope"]
    kind = scope["kind"]
    if kind == SCOPE_ALL:
        return True, ""
    if kind == "unknown":
        return False, f"未知生效范围「{scope['label']}」，规则不参与判定"

    dimension_kind = SCOPE_TABLE if rule["filter_type"] == TYPE_TABLE else (
        SCOPE_OPERATION if rule["filter_type"] == TYPE_OPERATION else "字段值"
    )
    if kind == SCOPE_TABLE:
        if scope["values"]:
            if _table_matches(scope["values"], event):
                return True, ""
            return False, f"事件来源表不在作用域表集合「{scope['label']}」内"
        if dimension_kind != SCOPE_TABLE:
            return False, "生效范围=指定表但未给出表限定值，且过滤类型不是表名，规则不参与判定"
        if _table_matches(_tokens(rule["expression"]), event):
            return True, ""
        return False, "事件来源表不在规则表达式指定的表范围内"

    # SCOPE_OPERATION
    if scope["values"]:
        if _operation_matches(scope["values"], event):
            return True, ""
        return False, (
            f"事件操作类型 {event.get('operation', '')} 不在作用域操作集合"
            f"「{scope['label']}」内"
        )
    if dimension_kind != SCOPE_OPERATION:
        return False, "生效范围=指定操作但未给出操作限定值，且过滤类型不是操作类型，规则不参与判定"
    if _operation_matches(_tokens(rule["expression"]), event):
        return True, ""
    return False, "事件操作类型不在规则表达式指定的操作范围内"


def effective_condition(rule: dict[str, Any]) -> str:
    """生成可回查的生效条件文案：维度、比较值、作用域一起给出。"""
    dimension = _DIMENSION_LABEL.get(rule["filter_type"], rule["filter_type"])
    return (
        f"{dimension}命中「{rule['expression']}」；"
        f"作用域={rule['scope']['label']}；"
        f"规则类型={'白名单' if rule['is_whitelist'] else '黑名单'}"
    )


def match_rule(rule: dict[str, Any], event: dict[str, Any]) -> tuple[str, str]:
    """判断单条规则对事件的作用状态。

    返回 (状态, 说明)：
    - hit：规则命中，产生白名单放行或黑名单拦截结论；
    - miss_in_scope：白名单在自身作用域内未命中，产生拦截结论（白名单是封闭集合）；
    - out_of_scope：规则作用域不覆盖该事件，不参与本事件判定。
    """
    if not rule_enabled(rule):
        return OUT_OF_SCOPE, "规则未启用"
    if not rule["expression"]:
        return OUT_OF_SCOPE, "匹配表达式为空"

    scope_ok, scope_reason = _scope_probe(rule, event)
    dimension_ok = _dimension_matches(rule, event)

    if scope_ok and dimension_ok:
        return HIT, "命中生效条件"
    if not scope_ok and dimension_ok:
        return OUT_OF_SCOPE, f"表达式匹配但作用域不覆盖：{scope_reason}"
    if rule["is_whitelist"] and scope_ok:
        return MISS_IN_SCOPE, "白名单作用域内未命中"
    if not scope_ok:
        return OUT_OF_SCOPE, scope_reason
    return OUT_OF_SCOPE, "黑名单未命中"


def evaluate_event(event: dict[str, Any], rules: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """按冻结的规则快照判定单个事件。

    返回结构包含：最终结论、全部命中记录（含生效条件/优先级/作用域）、
    作用域外规则、每一级的同级冲突详情以及默认放行标记。
    """
    hits: list[dict[str, Any]] = []
    scope_out: list[dict[str, Any]] = []

    for rule in rules:
        if not rule_enabled(rule):
            continue
        state, reason = match_rule(rule, event)
        record = {
            "rule_id": rule["rule_id"],
            "rule_name": rule["rule_name"],
            "filter_type": rule["filter_type"],
            "expression": rule["expression"],
            "is_whitelist": rule["is_whitelist"],
            "priority": rule["priority"],
            "scope": rule["scope"]["label"],
            "effective_condition": effective_condition(rule),
            "remark": rule["remark"],
            "state": state,
            "reason": reason,
        }
        if state == OUT_OF_SCOPE:
            scope_out.append(record)
            continue
        if rule["is_whitelist"]:
            record["verdict"] = VERDICT_ALLOW if state == HIT else VERDICT_DENY
        else:
            record["verdict"] = VERDICT_DENY
        hits.append(record)

    levels: list[dict[str, Any]] = []
    for priority in sorted({record["priority"] for record in hits}, reverse=True):
        group = [record for record in hits if record["priority"] == priority]
        allow_rules = [record["rule_name"] for record in group if record["verdict"] == VERDICT_ALLOW]
        deny_rules = [record["rule_name"] for record in group if record["verdict"] == VERDICT_DENY]
        levels.append({
            "priority": priority,
            "allow_rules": allow_rules,
            "deny_rules": deny_rules,
            "conflict": bool(allow_rules and deny_rules),
        })

    decisive_level = next((level for level in levels), None)
    conflict_levels = [level for level in levels if level["conflict"]]

    if decisive_level is None:
        allowed = True
        default_allow = True
        decisive_rule = "无规则命中，默认放行"
        effective_priority = None
    else:
        default_allow = False
        effective_priority = decisive_level["priority"]
        top = [record for record in hits if record["priority"] == effective_priority]
        deny_top = [record for record in top if record["verdict"] == VERDICT_DENY]
        allow_top = [record for record in top if record["verdict"] == VERDICT_ALLOW]
        # 同级冲突时黑名单优先；否则该级只要有拦截就拦截，否则放行
        allowed = not deny_top
        decisive_rule = (deny_top or allow_top)[0]["rule_name"]

    decisive_conflict = next(
        (level for level in conflict_levels
         if effective_priority is not None and level["priority"] == effective_priority),
        None,
    )
    conflict = {
        "present": bool(conflict_levels),
        "decisive": decisive_conflict is not None,
        "resolution": "同一优先级下白名单与黑名单冲突，黑名单优先（失败关闭）",
        "levels": [
            {
                "priority": level["priority"],
                "allow_rules": level["allow_rules"],
                "deny_rules": level["deny_rules"],
                "decisive": level is decisive_conflict,
                "overridden_by_priority": (
                    effective_priority
                    if effective_priority is not None and level["priority"] < effective_priority
                    else None
                ),
            }
            for level in conflict_levels
        ],
    }

    return {
        "allowed": allowed,
        "verdict": VERDICT_ALLOW if allowed else VERDICT_DENY,
        "hits": hits,
        "scope_out": scope_out,
        "conflict": conflict,
        "levels": levels,
        "default_allow": default_allow,
        "decisive_rule": decisive_rule,
        "effective_priority": effective_priority,
    }
