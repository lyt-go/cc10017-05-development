"""Schema演进业务规则：状态流转、字段校验与筛选口径都收在这里。

增强：订单字段演进规则校验 + Schema兼容发布包。

兼容发布包只有这一个写入点。生成、中断、恢复、发布、回滚都经过
``_apply_release``：中断只推进已完成的映射项，未完成的项留在发布包里，
重试时按来源批次恢复同一份发布包并补齐剩余项，不会整包重做。
上一份完整兼容包始终原样保留，供旧任务继续读取与回滚。
"""
from __future__ import annotations

import json
import re
from typing import Any

from app import schema_compat
from app.store import store

MODULE = "schema_evolve"
LIST_FIELDS = ["规则编号", "关联主题", "新增字段", "删除字段", "类型变更", "默认值填充", "兼容模式", "生效版本"]
REQUIRED_FIELDS = ["规则编号", "关联主题", "新增字段"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已回滚"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "执行回滚": "已回滚"}
NEGATIVE_ACTIONS = []

VALID_COMPAT_MODES = ("BACKWARD", "FORWARD", "FULL", "NONE")


class SchemaEvolveService:
    def __init__(self) -> None:
        # 演示用发布包由 seed_compat 写入；这里只保证产物表存在。
        schema_compat.ensure_tables()

    # ───────────────────────────────
    # 规则列表与动作
    # ───────────────────────────────

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("规则编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"演进规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于Schema演进可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        if action == "执行回滚":
            package = self._published_package_for_rule(entry_id)
            if package is not None:
                rolled, message = self.rollback_release(str(package.get("发布包编号")))
                if rolled is None:
                    return None, message

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"演进规则已{action}"

    # ───────────────────────────────
    # 订单字段演进规则校验
    # ───────────────────────────────

    def validate_rule(self, rule_id: int) -> tuple[dict[str, Any] | None, str]:
        """校验一条演进规则能否生成兼容发布包。

        订单主题额外要求：新增字段必须有默认值、运行中的历史任务必须保留旧读取。
        """
        rule = store.find(MODULE, rule_id)
        if rule is None:
            return None, f"演进规则 {rule_id} 不存在或已归档"

        checks: list[dict[str, str]] = []

        def add(item: str, level: str, detail: str) -> None:
            checks.append({"校验项": item, "结果": level, "说明": detail})

        for field in REQUIRED_FIELDS:
            if str(rule.get(field) or "").strip():
                add(field, "通过", "已填写")
            else:
                add(field, "失败", f"{field} 为空")

        topic_code = str(rule.get("关联主题") or "").strip()
        topic = self._find_topic(topic_code)
        if topic:
            add("关联主题存在", "通过", f"{topic_code}（{topic.get('主题名称')}）已注册")
        else:
            add("关联主题存在", "失败", f"关联主题 {topic_code or '-'} 未注册")

        added = schema_compat.split_fields(rule.get("新增字段"))
        deleted = schema_compat.split_fields(rule.get("删除字段"))
        if added:
            add("新增字段", "通过", "、".join(added))
        else:
            add("新增字段", "失败", "新增字段为空")
        duplicated = sorted({field for field in added if added.count(field) > 1})
        if duplicated:
            add("新增字段唯一", "失败", f"重复字段：{'、'.join(duplicated)}")
        else:
            add("新增字段唯一", "通过", "无重复")
        overlap = sorted(set(added) & set(deleted))
        if overlap:
            add("增删字段互斥", "失败", f"同时新增与删除：{'、'.join(overlap)}")
        else:
            add("增删字段互斥", "通过", "无冲突")

        base_columns = schema_compat.TOPIC_BASE_COLUMNS.get(topic_code, [])
        existing = sorted(set(added) & set(base_columns))
        if existing:
            add("新增字段未重复声明", "失败", f"主题基础列已包含：{'、'.join(existing)}")
        else:
            add("新增字段未重复声明", "通过", "均为新列")

        default_text = str(rule.get("默认值填充") or "").strip()
        default_fields = {
            part.split("=", 1)[0].strip()
            for part in re.split(r"[,，;；]", default_text)
            if "=" in part
        }
        missing_default = [field for field in added if field not in default_fields]
        if added and missing_default:
            add("默认值填充覆盖新增字段", "失败",
                f"缺少默认值：{'、'.join(missing_default)}；旧任务读取会拿不到值")
        else:
            add("默认值填充覆盖新增字段", "通过", default_text or "无需默认值")

        mode = str(rule.get("兼容模式") or "").strip().upper()
        if mode in VALID_COMPAT_MODES:
            add("兼容模式", "通过", mode)
        else:
            add("兼容模式", "失败", f"{mode or '-'} 不在 {'/'.join(VALID_COMPAT_MODES)} 范围内")

        version = str(rule.get("生效版本") or "").strip()
        if schema_compat.version_number(version) is not None:
            add("生效版本格式", "通过", version)
        else:
            add("生效版本格式", "失败", f"{version or '-'} 不是 vN 形式")

        type_change = str(rule.get("类型变更") or "").strip()
        if type_change and type_change != "-":
            if ":" not in type_change:
                add("类型变更", "失败", f"{type_change} 缺少 '字段:新类型' 写法")
            else:
                field, target_type = (part.strip() for part in type_change.split(":", 1))
                known = field in base_columns or field in added
                add("类型变更", "通过" if known and target_type else "失败",
                    f"{field} -> {target_type}" if known and target_type else f"字段 {field} 不在主题结构中")
        else:
            add("类型变更", "通过", "无类型变更")

        running_jobs = schema_compat.task_rows_for_topic(topic_code)
        if topic_code == schema_compat.ORDER_TOPIC_CODE:
            if running_jobs:
                add("订单历史任务保留旧读取", "通过",
                    f"{len(running_jobs)} 个运行中任务将保留旧读取版本，可回滚")
            else:
                add("订单历史任务保留旧读取", "告警", "订单主题暂无运行中任务")
            order_jobs = [job for job in running_jobs if schema_compat.is_order_job(job)]
            if order_jobs:
                add("订单聚合任务", "通过",
                    f"{'、'.join(str(job.get('任务编号')) for job in order_jobs)} 需登记使用版本")
            else:
                add("订单聚合任务", "告警", "未找到订单聚合任务，发布包将不包含任务项")

        failures = [item for item in checks if item["结果"] == "失败"]
        warnings = [item for item in checks if item["结果"] == "告警"]
        result = {
            "规则id": rule_id,
            "规则编号": rule.get("规则编号"),
            "关联主题": topic_code,
            "订单字段校验": topic_code == schema_compat.ORDER_TOPIC_CODE,
            "ok": not failures,
            "校验项": checks,
            "失败项": failures,
            "告警项": warnings,
        }
        if failures:
            return result, f"规则校验未通过：{failures[0]['说明']}"
        return result, "订单字段演进规则校验通过" if topic_code == schema_compat.ORDER_TOPIC_CODE else "演进规则校验通过"

    # ───────────────────────────────
    # Schema兼容发布包
    # ───────────────────────────────

    def list_releases(self, limit: int = 50) -> list[dict[str, Any]]:
        return [schema_compat.summary(row) for row in schema_compat.latest_packages(limit)]

    def get_release(self, package_code: str) -> dict[str, Any] | None:
        return schema_compat.detail(schema_compat.find_package(package_code))

    def preview_release(
        self,
        rule_id: int,
        *,
        strategy: str = schema_compat.STRATEGY_NEW_FIRST,
        job_ids: list[int] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """只读预览：不写发布包，但给出冲突判定、可见列与任务项。"""
        rule = store.find(MODULE, rule_id)
        if rule is None:
            return None, f"演进规则 {rule_id} 不存在或已归档"
        validation, message = self.validate_rule(rule_id)
        if validation is None or not validation["ok"]:
            return None, message
        if strategy not in schema_compat.STRATEGIES:
            return None, f"切换策略「{strategy}」不在 {'、'.join(schema_compat.STRATEGIES)} 范围内"

        topic_code = str(rule.get("关联主题"))
        old_version = str(rule.get("生效版本") or "v1")
        new_version = schema_compat.next_version(old_version)
        old_columns = schema_compat.visible_columns(topic_code, rule, new_mapping=False)
        new_columns = schema_compat.visible_columns(topic_code, rule, new_mapping=True)
        jobs = self._select_jobs(topic_code, job_ids)
        if not jobs:
            return None, f"主题 {topic_code} 没有可登记的任务"
        conflict = schema_compat.judge_conflict(rule, old_version, new_version, old_columns, strategy)
        publish_version = (
            new_version if strategy == schema_compat.STRATEGY_NEW_FIRST else old_version
        )
        return {
            "规则id": rule_id,
            "规则编号": rule.get("规则编号"),
            "关联主题": topic_code,
            "来源批次": None,
            "旧读取版本": old_version,
            "新映射版本": new_version,
            "切换策略": strategy,
            "当前可读版本": publish_version,
            "数据版本": publish_version,
            "可见列": new_columns if publish_version == new_version else old_columns,
            "旧读取可见列": old_columns,
            "新映射可见列": new_columns,
            "冲突判定": conflict,
            "任务项": [
                {"任务id": job.get("id"), "任务编号": job.get("任务编号"),
                 "输出表名": job.get("输出表名"), "当前状态": job.get("status")}
                for job in jobs
            ],
            "校验项": validation["校验项"],
        }, "发布包预览已生成（未落库）"

    def create_or_resume_release(
        self,
        rule_id: int,
        *,
        strategy: str | None = None,
        source_batch: str | None = None,
        job_ids: list[int] | None = None,
        apply: bool = True,
        interrupt_after: int | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """生成或恢复兼容发布包。所有写入都落到 _apply_release 这一处。"""
        rule = store.find(MODULE, rule_id)
        if rule is None:
            return None, f"演进规则 {rule_id} 不存在或已归档"

        # 1) 显式按来源批次重试：必须恢复同一份发布包，不能新建
        if source_batch:
            existing = schema_compat.find_package_by_batch(source_batch)
            if existing is not None:
                if int(existing.get("规则id", 0)) != rule_id:
                    return None, f"来源批次 {source_batch} 属于规则 {existing.get('规则编号')}，不能跨规则恢复"
                if existing.get("状态") in (schema_compat.STATUS_RELEASING, schema_compat.STATUS_INTERRUPTED):
                    return self._apply_release(existing, interrupt=False)
                return schema_compat.summary(existing), f"来源批次 {source_batch} 已完成，无需重复发布"

        # 2) 同一规则有未完成发布包：续做剩余项，不整包重做
        incomplete = schema_compat.incomplete_packages_for_rule(rule_id)
        if incomplete:
            package = incomplete[-1]
            if not apply:
                return schema_compat.summary(package), (
                    f"规则 {rule_id} 存在未完成发布包 {package.get('发布包编号')}，可恢复补齐"
                )
            package, message = self._apply_release(package, interrupt=False)
            if package is None:
                return None, message
            return package, f"{message}（沿用原发布包 {package['发布包编号']}）"

        # 3) 校验通过才允许生成
        if rule.get("status") not in ("待校验", "已生效"):
            return None, f"规则「{rule.get('规则编号')}」状态为「{rule.get('status')}」，须先提交校验"
        validation, message = self.validate_rule(rule_id)
        if validation is None or not validation["ok"]:
            return None, message

        strategy = strategy or schema_compat.STRATEGY_NEW_FIRST
        if strategy not in schema_compat.STRATEGIES:
            return None, f"切换策略「{strategy}」不在 {'、'.join(schema_compat.STRATEGIES)} 范围内"

        # 同一字段内容与策略已有完整发布包：视为同一次发布，不重复读取旧字段
        signature = schema_compat.mapping_signature(rule, strategy)
        existing = schema_compat.published_by_signature(rule_id, signature)
        if existing is not None:
            if existing.get("当前可读版本") == existing.get("新映射版本"):
                detail = f"当前可读版本 {existing.get('当前可读版本')}，未重复读取旧字段"
            else:
                detail = (
                    f"当前可读版本仍为 {existing.get('当前可读版本')}（旧读取优先），"
                    "新映射暂缓生效"
                )
            return schema_compat.summary(existing), (
                f"同一字段演进已由发布包 {existing.get('发布包编号')} 发布，{detail}"
            )

        topic_code = str(rule.get("关联主题"))
        old_version = str(rule.get("生效版本") or "v1")
        new_version = schema_compat.next_version(old_version)
        jobs = self._select_jobs(topic_code, job_ids)
        if not jobs:
            return None, f"主题 {topic_code} 没有可登记的任务"

        # 已经切换到同一新映射版本的任务不再重复读取旧字段
        switched: list[dict[str, Any]] = []
        pending_jobs: list[dict[str, Any]] = []
        switched_ids = self._switched_job_ids(rule_id, new_version)
        for job in jobs:
            if int(job.get("id", 0)) in switched_ids:
                switched.append(job)
            else:
                pending_jobs.append(job)
        if not pending_jobs:
            latest = self._published_package_for_rule(rule_id)
            if latest is not None:
                return schema_compat.summary(latest), (
                    f"任务已由发布包 {latest.get('发布包编号')} 切换到 {new_version}，"
                    "未重复读取旧字段"
                )
            return None, "所有任务都已完成映射，无需生成新发布包"

        old_columns = schema_compat.visible_columns(topic_code, rule, new_mapping=False)
        new_columns = schema_compat.visible_columns(topic_code, rule, new_mapping=True)
        # 发布包只声明一个当前可读版本；策略决定它是新映射还是旧读取
        publish_version = (
            new_version if strategy == schema_compat.STRATEGY_NEW_FIRST else old_version
        )
        conflict = schema_compat.judge_conflict(rule, old_version, new_version, old_columns, strategy)

        package_id = schema_compat.next_id(schema_compat.packages())
        package_code = f"SCHE-PKG-{package_id:04d}"
        batch = source_batch or f"BATCH-{rule.get('规则编号')}-{package_id:04d}"
        created_at = schema_compat.now_iso()
        package: dict[str, Any] = {
            "id": package_id,
            "发布包编号": package_code,
            "规则id": rule_id,
            "规则编号": rule.get("规则编号"),
            "关联主题": topic_code,
            "来源批次": batch,
            "旧读取版本": old_version,
            "新映射版本": new_version,
            "切换策略": strategy,
            "当前可读版本": old_version,
            "数据版本": old_version,
            "可见列": old_columns,
            "旧读取可见列": old_columns,
            "新映射可见列": new_columns,
            "预期可读版本": publish_version,
            "状态": schema_compat.STATUS_RELEASING,
            "创建时间": created_at,
            "完成时间": None,
            "旧版读取结果": schema_compat.build_old_read(rule, old_version, old_columns),
            "新字段映射": schema_compat.build_new_mapping(rule, new_version),
            "冲突判定": conflict,
            "发布记录": [{
                "时间": created_at,
                "事件": "创建发布包",
                "说明": f"来源批次 {batch}，切换策略 {strategy}，任务 {len(pending_jobs)} 个"
                + (f"；跳过已切换任务 {len(switched)} 个" if switched else ""),
            }],
        }
        schema_compat.packages().append(package)
        for job in pending_jobs:
            schema_compat.package_items().append({
                "id": schema_compat.next_id(schema_compat.package_items()),
                "发布包编号": package_code,
                "规则id": rule_id,
                "规则编号": rule.get("规则编号"),
                "关联主题": topic_code,
                "任务id": int(job.get("id", 0)),
                "任务编号": job.get("任务编号"),
                "输出表名": job.get("输出表名"),
                "来源批次": batch,
                "映射状态": schema_compat.ITEM_PENDING,
                "读取版本": old_version,
                "任务可见列": old_columns,
                "字段映射": package["新字段映射"].get("字段映射"),
                "更新时间": created_at,
                "应用时间": None,
            })

        if not apply:
            return schema_compat.summary(package), f"发布包 {package_code} 已生成，任务项待发布"

        package, message = self._apply_release(package, interrupt_after=interrupt_after)
        if package is None:
            return None, message
        note = f"；已跳过 {len(switched)} 个已切换任务" if switched else ""
        return package, message + note

    def apply_release(
        self,
        package_code: str,
        *,
        limit: int | None = None,
        interrupt: bool = False,
    ) -> tuple[dict[str, Any] | None, str]:
        package = schema_compat.find_package(package_code)
        if package is None:
            return None, f"发布包 {package_code} 不存在"
        if package.get("状态") == schema_compat.STATUS_ROLLED_BACK:
            return None, f"发布包 {package_code} 已回滚，不能继续发布"
        if interrupt and package.get("状态") != schema_compat.STATUS_RELEASING:
            return schema_compat.summary(package), (
                f"发布包 {package_code} 状态为「{package.get('状态')}」，没有可中断的进行中发布"
            )
        return self._apply_release(package, limit=limit, interrupt=interrupt)

    def resume_release(self, package_code: str) -> tuple[dict[str, Any] | None, str]:
        """中断后恢复同一次发布：沿用原发布包编号、来源批次与任务项。"""
        package = schema_compat.find_package(package_code)
        if package is None:
            return None, f"发布包 {package_code} 不存在"
        if package.get("状态") == schema_compat.STATUS_PUBLISHED:
            return schema_compat.summary(package), f"发布包 {package_code} 已发布，无需恢复"
        if package.get("状态") == schema_compat.STATUS_ROLLED_BACK:
            return None, f"发布包 {package_code} 已回滚，请重新生成发布包"
        return self._apply_release(package, interrupt=False)

    def interrupt_release(self, package_code: str) -> tuple[dict[str, Any] | None, str]:
        package = schema_compat.find_package(package_code)
        if package is None:
            return None, f"发布包 {package_code} 不存在"
        if package.get("状态") not in (schema_compat.STATUS_RELEASING, schema_compat.STATUS_INTERRUPTED):
            return None, f"发布包 {package_code} 状态为「{package.get('状态')}」，无需中断"
        return self._apply_release(package, interrupt=True)

    def rollback_release(self, package_code: str) -> tuple[dict[str, Any] | None, str]:
        """回滚到发布包里保留的旧版读取结果。"""
        package = schema_compat.find_package(package_code)
        if package is None:
            return None, f"发布包 {package_code} 不存在"
        if not package.get("旧版读取结果"):
            return None, f"发布包 {package_code} 未保留旧版读取结果，无法回滚"
        old_version = str(package.get("旧读取版本") or "")
        old_columns = list(package.get("旧读取可见列") or [])
        package["状态"] = schema_compat.STATUS_ROLLED_BACK
        package["当前可读版本"] = old_version
        package["数据版本"] = old_version
        package["可见列"] = old_columns
        package["完成时间"] = schema_compat.now_iso()
        for item in schema_compat.items_for_package(package_code):
            item["映射状态"] = schema_compat.ITEM_ROLLED_BACK
            item["读取版本"] = old_version
            item["任务可见列"] = old_columns
            item["更新时间"] = schema_compat.now_iso()
        for usage in schema_compat.usage_rows():
            if usage.get("发布包编号") == package_code:
                usage["状态"] = schema_compat.USAGE_ROLLED_BACK
                usage["读取版本"] = old_version
                usage["数据版本"] = old_version
                usage["任务可见列"] = old_columns
                usage["更新时间"] = schema_compat.now_iso()
        rule = store.find(MODULE, int(package.get("规则id", 0)))
        if rule is not None:
            rule["生效版本"] = old_version
        self._log(package, "回滚发布包",
                  f"当前可读版本回到 {old_version}，旧读取仍可用于历史任务")
        return schema_compat.summary(package), f"发布包 {package_code} 已回滚到 {old_version}"

    def _apply_release(
        self,
        package: dict[str, Any],
        *,
        limit: int | None = None,
        interrupt_after: int | None = None,
        interrupt: bool = False,
    ) -> tuple[dict[str, Any] | None, str]:
        """唯一写入点：逐个任务项推进映射；中断时保留未生效项。"""
        package_code = str(package.get("发布包编号"))
        rule = store.find(MODULE, int(package.get("规则id", 0)))
        if rule is None:
            return None, f"发布包 {package_code} 关联的演进规则已不存在"
        if package.get("状态") == schema_compat.STATUS_ROLLED_BACK:
            return None, f"发布包 {package_code} 已回滚，不能继续发布"

        if interrupt:
            package["状态"] = schema_compat.STATUS_INTERRUPTED
            self._log(package, "中断发布", "按请求中断，保留已完成映射的任务项")
            return schema_compat.summary(package), f"发布包 {package_code} 已中断"

        budget = interrupt_after if interrupt_after is not None else limit
        applied = 0
        for item in schema_compat.items_for_package(package_code):
            if item.get("映射状态") != schema_compat.ITEM_PENDING:
                continue
            if budget is not None and applied >= budget:
                break
            self._apply_item(package, item)
            applied += 1

        remaining = [
            item for item in schema_compat.items_for_package(package_code)
            if item.get("映射状态") == schema_compat.ITEM_PENDING
        ]
        if remaining:
            interrupted = interrupt_after is not None or interrupt
            package["状态"] = (
                schema_compat.STATUS_INTERRUPTED if interrupted else schema_compat.STATUS_RELEASING
            )
            self._log(
                package,
                "中断发布" if interrupted else "部分应用",
                f"本次完成 {applied} 项，剩余 {len(remaining)} 项待映射；"
                f"上一份完整兼容包 {schema_compat.previous_complete_code(package) or '无'} 保持不变",
            )
            return schema_compat.summary(package), (
                f"发布包 {package_code} 已中断，剩余 {len(remaining)} 项可恢复补齐"
                if interrupted else
                f"发布包 {package_code} 已应用 {applied} 项，剩余 {len(remaining)} 项"
            )

        # 全部任务项完成：发布包只声明一个当前可读版本
        publish_version = str(package.get("预期可读版本") or package.get("新映射版本"))
        package["当前可读版本"] = publish_version
        package["数据版本"] = publish_version
        package["可见列"] = (
            list(package.get("新映射可见列") or [])
            if publish_version == package.get("新映射版本")
            else list(package.get("旧读取可见列") or [])
        )
        package["状态"] = schema_compat.STATUS_PUBLISHED
        package["完成时间"] = schema_compat.now_iso()
        if publish_version == package.get("新映射版本"):
            rule["生效版本"] = publish_version
            topic = self._find_topic(str(package.get("关联主题")))
            if topic is not None:
                topic["Schema版本"] = publish_version
        self._log(
            package,
            "发布完成",
            f"当前可读版本 {publish_version}，数据版本 {package.get('数据版本')}；"
            f"旧版读取 {package.get('旧读取版本')} 仍可回滚",
        )
        return schema_compat.summary(package), (
            f"发布包 {package_code} 已发布，当前可读版本 {publish_version}，"
            f"来源批次 {package.get('来源批次')}"
        )

    def _apply_item(self, package: dict[str, Any], item: dict[str, Any]) -> None:
        current = str(package.get("预期可读版本") or package.get("新映射版本"))
        columns = (
            list(package.get("新映射可见列") or [])
            if current == package.get("新映射版本")
            else list(package.get("旧读取可见列") or [])
        )
        item["映射状态"] = schema_compat.ITEM_MAPPED
        item["读取版本"] = current
        item["任务可见列"] = columns
        item["更新时间"] = schema_compat.now_iso()
        item["应用时间"] = item["更新时间"]

    # ───────────────────────────────
    # 导出兼容包
    # ───────────────────────────────

    def export_release_rows(self, package_code: str) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        package = schema_compat.find_package(package_code)
        if package is None:
            return [], None
        data = schema_compat.summary(package)

        def cell(value: Any) -> str:
            if value is None:
                return ""
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False)
            return str(value)

        rows: list[dict[str, Any]] = []
        for item in schema_compat.items_for_package(package_code):
            rows.append({
                "发布包编号": package_code,
                "来源批次": data["来源批次"],
                "规则编号": data["规则编号"],
                "关联主题": data["关联主题"],
                "任务编号": item.get("任务编号"),
                "输出表名": item.get("输出表名"),
                "映射状态": item.get("映射状态"),
                "读取版本": item.get("读取版本"),
                "任务可见列": ",".join(item.get("任务可见列") or []),
                "旧读取版本": data["旧读取版本"],
                "旧版读取结果": cell(data["旧版读取结果"]),
                "新映射版本": data["新映射版本"],
                "新字段映射": cell(data["新字段映射"]),
                "当前可读版本": data["当前可读版本"],
                "数据版本": data["数据版本"],
                "切换策略": data["切换策略"],
                "上一份完整兼容包": data["上一份完整兼容包"],
                "冲突判定": cell(data["冲突判定"]),
                "发布包状态": data["状态"],
                "更新时间": item.get("更新时间"),
            })
        return rows, data

    def release_export_fields(self) -> list[str]:
        return [
            "发布包编号", "来源批次", "规则编号", "关联主题", "任务编号", "输出表名",
            "映射状态", "读取版本", "任务可见列", "旧读取版本", "旧版读取结果",
            "新映射版本", "新字段映射", "当前可读版本", "数据版本", "切换策略",
            "上一份完整兼容包", "冲突判定", "发布包状态", "更新时间",
        ]

    # ───────────────────────────────
    # 内部辅助
    # ───────────────────────────────

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _select_jobs(self, topic_code: str, job_ids: list[int] | None) -> list[dict[str, Any]]:
        jobs = schema_compat.task_rows_for_topic(topic_code)
        if job_ids:
            wanted = {int(job_id) for job_id in job_ids}
            jobs = [job for job in jobs if int(job.get("id", 0)) in wanted]
        return jobs

    def _switched_job_ids(self, rule_id: int, new_version: str) -> set[int]:
        switched: set[int] = set()
        for package in schema_compat.packages_for_rule(rule_id):
            if package.get("状态") != schema_compat.STATUS_PUBLISHED:
                continue
            if package.get("新映射版本") != new_version:
                continue
            for item in schema_compat.items_for_package(str(package.get("发布包编号"))):
                if item.get("映射状态") == schema_compat.ITEM_MAPPED and item.get("读取版本") == new_version:
                    switched.add(int(item.get("任务id", 0)))
        return switched

    def _published_package_for_rule(self, rule_id: int) -> dict[str, Any] | None:
        rows = [
            row for row in schema_compat.packages_for_rule(rule_id)
            if row.get("状态") == schema_compat.STATUS_PUBLISHED
        ]
        return rows[-1] if rows else None

    def _log(self, package: dict[str, Any], event: str, note: str) -> None:
        package.setdefault("发布记录", []).append({
            "时间": schema_compat.now_iso(),
            "事件": event,
            "说明": note,
        })
