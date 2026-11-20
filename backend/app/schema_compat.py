"""Schema兼容发布包的共享模型、查询口径与跨模块核对。

发布包只有一处写入点：``app.services.schema_evolve.SchemaEvolveService``。
流式计算模块只读取这里汇总的发布包并登记任务使用版本，不会另写一份发布状态，
从而避免跨模块交接时丢失来源批次。

发布包同时保留两类信息：
- 旧版读取结果：旧任务按旧版本继续读取所需字段与默认值；
- 新字段映射：新任务按新版本读取新增字段所需的映射。

发布包只能声明一个当前可读版本；切换策略决定这个版本是新映射还是旧读取。
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from app.store import store

PACKAGE_TABLE = "schema_compat_releases"
ITEM_TABLE = "schema_compat_release_items"
USAGE_TABLE = "flink_schema_usage"

# 订单主题与订单聚合任务
ORDER_TOPIC_CODE = "TOPI-0001"
ORDER_OUTPUT_TABLE = "order_agg_hourly"
ORDER_KEYWORD = "订单"

STRATEGY_NEW_FIRST = "新映射优先"
STRATEGY_OLD_FIRST = "旧读取优先"
STRATEGIES = (STRATEGY_NEW_FIRST, STRATEGY_OLD_FIRST)

STATUS_RELEASING = "发布中"
STATUS_INTERRUPTED = "已中断"
STATUS_PUBLISHED = "已发布"
STATUS_ROLLED_BACK = "已回滚"

USAGE_REGISTERED = "已登记"
USAGE_REPLACED = "已替换"
USAGE_ROLLED_BACK = "已回滚"
# 可用于任务启动的登记状态：回滚后旧读取仍然有效
USAGE_USABLE = (USAGE_REGISTERED, USAGE_ROLLED_BACK)

ITEM_PENDING = "待映射"
ITEM_MAPPED = "已映射"
ITEM_ROLLED_BACK = "已回滚"

# 各主题已知的基础列：用于说明切换前后任务可见列
TOPIC_BASE_COLUMNS: dict[str, list[str]] = {
    "TOPI-0001": ["order_id", "amount", "created_at"],
    "TOPI-0002": ["user_id", "event", "event_time"],
    "TOPI-0003": ["pay_id", "status", "amount"],
}


def ensure_tables() -> None:
    """确保发布包、任务项、使用版本登记三张产物表存在。"""
    store.rows(PACKAGE_TABLE)
    store.rows(ITEM_TABLE)
    store.rows(USAGE_TABLE)


def packages() -> list[dict[str, Any]]:
    return store.rows(PACKAGE_TABLE)


def package_items() -> list[dict[str, Any]]:
    return store.rows(ITEM_TABLE)


def usage_rows() -> list[dict[str, Any]]:
    return store.rows(USAGE_TABLE)


def find_package(package_code: str | None) -> dict[str, Any] | None:
    if not package_code:
        return None
    for row in packages():
        if str(row.get("发布包编号")) == str(package_code):
            return row
    return None


def find_package_by_batch(source_batch: str | None) -> dict[str, Any] | None:
    if not source_batch:
        return None
    for row in packages():
        if str(row.get("来源批次")) == str(source_batch):
            return row
    return None


def packages_for_rule(rule_id: int) -> list[dict[str, Any]]:
    matched = [row for row in packages() if int(row.get("规则id", 0)) == int(rule_id)]
    return sorted(matched, key=lambda row: int(row.get("id", 0)))


def latest_package_for_rule(rule_id: int) -> dict[str, Any] | None:
    rows = packages_for_rule(rule_id)
    return rows[-1] if rows else None


def incomplete_packages_for_rule(rule_id: int) -> list[dict[str, Any]]:
    return [
        row for row in packages_for_rule(rule_id)
        if row.get("状态") in (STATUS_RELEASING, STATUS_INTERRUPTED)
    ]


def packages_for_topic(topic_code: str) -> list[dict[str, Any]]:
    matched = [row for row in packages() if row.get("关联主题") == topic_code]
    return sorted(matched, key=lambda row: int(row.get("id", 0)))


def latest_packages(limit: int = 20) -> list[dict[str, Any]]:
    rows = sorted(packages(), key=lambda row: int(row.get("id", 0)), reverse=True)
    return rows[:limit]


def latest_package_for_topic(topic_code: str | None) -> dict[str, Any] | None:
    rows = packages_for_topic(topic_code or "")
    return rows[-1] if rows else None


def latest_published_for_topic(topic_code: str | None) -> dict[str, Any] | None:
    rows = [row for row in packages_for_topic(topic_code or "") if row.get("状态") == STATUS_PUBLISHED]
    return rows[-1] if rows else None


def items_for_package(package_code: str) -> list[dict[str, Any]]:
    rows = [row for row in package_items() if row.get("发布包编号") == package_code]
    return sorted(rows, key=lambda row: int(row.get("id", 0)))


def items_for_job(package_code: str, job_id: int) -> list[dict[str, Any]]:
    return [item for item in items_for_package(package_code) if int(item.get("任务id", 0)) == int(job_id)]


def next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def previous_complete_code(package: dict[str, Any]) -> str | None:
    """同一规则下、当前发布包之前最近一份完整（已发布）发布包。

    当前包若已发布，"上一份"仍是它之前那份完整包；
    当前包若中断，上一份完整包必须原样保留，不能被本次中断覆盖。
    """
    current_id = int(package.get("id", 0))
    candidates = [
        row for row in packages_for_rule(int(package.get("规则id", 0)))
        if int(row.get("id", 0)) < current_id and row.get("状态") == STATUS_PUBLISHED
    ]
    return str(candidates[-1].get("发布包编号")) if candidates else None


def current_complete_code(package: dict[str, Any]) -> str | None:
    """当前规则下最近一份完整发布包；当前包已发布时就是它自己。"""
    candidates = [
        row for row in packages_for_rule(int(package.get("规则id", 0)))
        if row.get("状态") == STATUS_PUBLISHED
    ]
    return str(candidates[-1].get("发布包编号")) if candidates else None


def summary(package: dict[str, Any] | None) -> dict[str, Any]:
    """列表、详情与流式计算共用的发布包摘要口径。

    完成/剩余数量每次从任务项现算，保证列表先读、详情后读时返回一致。
    """
    if package is None:
        return {
            "发布包编号": None, "规则id": None, "规则编号": None, "关联主题": None,
            "来源批次": None, "旧读取版本": None, "新映射版本": None,
            "切换策略": None, "当前可读版本": None, "数据版本": None,
            "可见列": [], "状态": "未生成", "已完成项数": 0, "总项数": 0,
            "剩余项数": 0, "已回滚项数": 0, "上一份完整兼容包": None,
            "当前完整发布包": None, "创建时间": None, "完成时间": None,
            "冲突判定": None, "旧版读取结果": None, "新字段映射": None,
            "旧读取可回滚": False,
        }
    items = items_for_package(str(package.get("发布包编号")))
    mapped = sum(1 for item in items if item.get("映射状态") == ITEM_MAPPED)
    pending = sum(1 for item in items if item.get("映射状态") == ITEM_PENDING)
    rolled_back = sum(1 for item in items if item.get("映射状态") == ITEM_ROLLED_BACK)
    old_read = package.get("旧版读取结果")
    return {
        "发布包编号": package.get("发布包编号"),
        "规则id": package.get("规则id"),
        "规则编号": package.get("规则编号"),
        "关联主题": package.get("关联主题"),
        "来源批次": package.get("来源批次"),
        "旧读取版本": package.get("旧读取版本"),
        "新映射版本": package.get("新映射版本"),
        "切换策略": package.get("切换策略"),
        "当前可读版本": package.get("当前可读版本"),
        "数据版本": package.get("数据版本"),
        "可见列": list(package.get("可见列") or []),
        "状态": package.get("状态"),
        "已完成项数": mapped,
        "总项数": len(items),
        "剩余项数": pending,
        "已回滚项数": rolled_back,
        "上一份完整兼容包": previous_complete_code(package),
        "当前完整发布包": current_complete_code(package),
        "创建时间": package.get("创建时间"),
        "完成时间": package.get("完成时间"),
        "冲突判定": package.get("冲突判定"),
        "旧版读取结果": old_read,
        "新字段映射": package.get("新字段映射"),
        "旧读取可回滚": bool(old_read),
    }


def detail(package: dict[str, Any] | None) -> dict[str, Any] | None:
    """详情：摘要字段与列表同一口径，另附任务项和使用登记。"""
    if package is None:
        return None
    data = summary(package)
    code = str(package.get("发布包编号"))
    data["任务项"] = items_for_package(code)
    data["使用登记"] = [row for row in usage_rows() if row.get("发布包编号") == code]
    return data


def version_number(value: str | None) -> int | None:
    match = re.fullmatch(r"v(\d+)", str(value or "").strip(), re.IGNORECASE)
    return int(match.group(1)) if match else None


def next_version(value: str | None) -> str:
    current = str(value or "").strip()
    number = version_number(current)
    if number is not None:
        return f"v{number + 1}"
    return f"{current or 'v1'}+1"


def split_fields(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text or text == "-":
        return []
    return [part for part in re.split(r"[,，、;；\s]+", text) if part and part != "-"]


def visible_columns(topic_code: str, rule: dict[str, Any], *, new_mapping: bool) -> list[str]:
    """发布包声明的任务可见列。

    旧读取：基础列 + 尚未删除的旧列；
    新映射：在旧读取列基础上再暴露本次新增字段。
    """
    base = list(TOPIC_BASE_COLUMNS.get(topic_code, []))
    deleted = split_fields(rule.get("删除字段"))
    old_columns = base + [field for field in deleted if field not in base]
    if not new_mapping:
        return old_columns
    added = split_fields(rule.get("新增字段"))
    return old_columns + [field for field in added if field not in old_columns]


def build_old_read(rule: dict[str, Any], old_version: str, columns: list[str]) -> dict[str, Any]:
    return {
        "读取版本": old_version,
        "读取字段": columns,
        "默认值填充": rule.get("默认值填充") or "-",
        "兼容模式": rule.get("兼容模式") or "-",
        "说明": f"旧任务继续按 {old_version} 读取，新增字段以默认值补齐",
    }


def build_new_mapping(rule: dict[str, Any], new_version: str) -> dict[str, Any]:
    added = split_fields(rule.get("新增字段"))
    default = str(rule.get("默认值填充") or "").strip()
    default_value = ""
    if "=" in default:
        default_value = default.split("=", 1)[1].strip()
    return {
        "映射版本": new_version,
        "新增字段": added,
        "删除字段": split_fields(rule.get("删除字段")),
        "字段映射": [
            {"源字段": field, "目标字段": field, "默认值": default_value}
            for field in added
        ],
        "类型变更": rule.get("类型变更") or "-",
        "兼容模式": rule.get("兼容模式") or "-",
        "说明": f"新任务按 {new_version} 读取并写入 {', '.join(added) or '无新增字段'}",
    }


def judge_conflict(
    rule: dict[str, Any],
    old_version: str,
    new_version: str,
    old_columns: list[str],
    strategy: str,
) -> dict[str, Any]:
    """旧版读取与新版映射的冲突判定。

    冲突是否存在不随策略变化；冲突的取舍随策略变化，并因此改变发布包的数据版本。
    """
    reasons: list[str] = []
    deleted = split_fields(rule.get("删除字段"))
    for field in deleted:
        reasons.append(f"删除字段 {field} 仍被旧读取引用")
    type_change = str(rule.get("类型变更") or "").strip()
    if type_change and type_change != "-":
        field = type_change.split(":", 1)[0].strip()
        if field in old_columns:
            reasons.append(f"字段 {field} 类型变更（{type_change}）与旧读取不兼容")
    if old_version == new_version:
        reasons.append("旧读取版本与新映射版本相同，无法判定切换边界")

    exists = bool(reasons)
    if not exists:
        conclusion = "旧读取与新版映射无字段冲突，可按策略直接切换"
    elif strategy == STRATEGY_NEW_FIRST:
        conclusion = "以新映射为准：切换前先冻结旧读取，数据版本切到新映射版本"
    else:
        conclusion = "保留旧读取：新映射暂缓生效，数据版本保持旧读取版本"
    return {
        "存在冲突": exists,
        "原因": reasons,
        "取舍": strategy,
        "结论": conclusion,
    }


def task_rows_for_topic(topic_code: str) -> list[dict[str, Any]]:
    """发布包需要登记使用版本的任务：该主题下未停止的流式任务。"""
    return [
        row for row in store.rows("flink_job")
        if row.get("消费主题") == topic_code and row.get("status") != "已停止"
    ]


def is_order_job(job: dict[str, Any]) -> bool:
    output = str(job.get("输出表名") or "")
    return output == ORDER_OUTPUT_TABLE or "order" in output.casefold() or ORDER_KEYWORD in output


def verify_job_package(
    job: dict[str, Any],
    package: dict[str, Any] | None,
    *,
    require_published: bool = True,
) -> tuple[list[str], list[dict[str, Any]]]:
    """流式计算登记使用版本前的核对项：返回 (错误列表, 核对明细)。

    回滚后的发布包允许登记：任务退回旧读取版本，旧任务仍可继续读取。
    """
    errors: list[str] = []
    checks: list[dict[str, Any]] = []

    if package is None:
        errors.append("未找到可用的Schema兼容发布包")
        return errors, checks

    data = summary(package)
    code = data["发布包编号"]
    rolled_back = data["状态"] == STATUS_ROLLED_BACK

    def check(name: str, expected: Any, actual: Any) -> bool:
        passed = expected == actual
        checks.append({"核对项": name, "期望": expected, "实际": actual, "通过": passed})
        if not passed:
            errors.append(f"{name}不一致：期望 {expected}，实际 {actual}")
        return passed

    if require_published:
        allowed = (STATUS_PUBLISHED, STATUS_ROLLED_BACK)
        passed = data["状态"] in allowed
        checks.append({
            "核对项": "发布包状态", "期望": "/".join(allowed),
            "实际": data["状态"], "通过": passed,
        })
        if not passed:
            errors.append(
                f"发布包状态不一致：期望 {'/'.join(allowed)}，实际 {data['状态']}"
                f"（{data['状态']} 说明仍映射，未生效）"
            )
    check("关联主题", job.get("消费主题"), data["关联主题"])

    job_items = items_for_job(code, int(job.get("id", 0)))
    if not job_items:
        errors.append(f"任务 {job.get('任务编号')} 未登记在发布包 {code} 的任务项中")
        checks.append({"核对项": "任务项归属", "期望": "已登记", "实际": "未登记", "通过": False})
        return errors, checks
    checks.append({"核对项": "任务项归属", "期望": "已登记", "实际": "已登记", "通过": True})

    pending = [item for item in job_items if item.get("映射状态") == ITEM_PENDING]
    if pending:
        errors.append(f"任务 {job.get('任务编号')} 还有 {len(pending)} 个未完成映射的任务项")
    checks.append({
        "核对项": "任务项映射状态", "期望": "无待映射",
        "实际": "待映射" if pending else "已映射", "通过": not pending,
    })

    if job_items:
        item_version = job_items[-1].get("读取版本")
        expected = data["旧读取版本"] if rolled_back else data["当前可读版本"]
        check("任务使用版本", expected, item_version)

    if rolled_back:
        checks.append({
            "核对项": "旧读取回滚状态", "期望": "旧读取可用",
            "实际": f"旧读取 {data['旧读取版本']}", "通过": True,
        })
    return errors, checks


def mapping_signature(rule: dict[str, Any], strategy: str) -> tuple[Any, ...]:
    """同一次字段演进的判重口径：字段内容 + 切换策略，不含版本号。

    版本号会在发布后推进；若按版本判重，重复生成会把已切换的任务当成新任务，
    重新走一遍旧字段读取。按内容和策略判重才能把"已切换"的任务留在原发布包。
    """
    return (
        tuple(sorted(split_fields(rule.get("新增字段")))),
        tuple(sorted(split_fields(rule.get("删除字段")))),
        str(rule.get("类型变更") or "-").strip(),
        str(rule.get("默认值填充") or "-").strip(),
        str(rule.get("兼容模式") or "-").strip().upper(),
        strategy,
    )


def package_signature(package: dict[str, Any]) -> tuple[Any, ...]:
    mapping = package.get("新字段映射") or {}
    old_read = package.get("旧版读取结果") or {}
    return (
        tuple(sorted(mapping.get("新增字段") or [])),
        tuple(sorted(mapping.get("删除字段") or [])),
        str(mapping.get("类型变更") or "-").strip(),
        str(old_read.get("默认值填充") or "-").strip(),
        str(mapping.get("兼容模式") or "-").strip().upper(),
        str(package.get("切换策略") or ""),
    )


def published_by_signature(rule_id: int, signature: tuple[Any, ...]) -> dict[str, Any] | None:
    candidates = [
        row for row in packages_for_rule(rule_id)
        if row.get("状态") == STATUS_PUBLISHED and package_signature(row) == signature
    ]
    return candidates[-1] if candidates else None


def now_iso() -> str:
    return datetime.now().isoformat()
