"""流式计算业务规则：作业管理、链路消费与状态推进。

增强：Flink作业启动时校验上游主题状态，运行中消费主题数据生成计算产物。
订单聚合任务启动前还要核对Schema兼容发布包，并登记任务使用版本；登记时保留
发布包的来源批次，跨模块交接不丢批次。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from app import schema_compat
from app.store import store

MODULE = "flink_job"
REQUIRED_FIELDS = ["任务编号", "消费主题", "转换逻辑"]
STATUS_ORDER = ["草稿", "待校验", "运行中", "已停止"]
ACTION_RULES = {"提交校验": "待校验", "启动作业": "运行中", "停止作业": "已停止"}
NEGATIVE_ACTIONS = []


class FlinkJobService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        # 校验消费主题是否存在
        topic_code = values.get("消费主题")
        if topic_code:
            topic = self._find_topic(topic_code)
            if not topic:
                return None, [f"消费主题 {topic_code} 未注册"]

        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        # 补充可选字段
        for f in ["输出表名", "并行度", "检查点间隔", "状态后端", "水位策略"]:
            entry[f] = values.get(f, "")
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流式任务 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式计算可执行范围"
        target = ACTION_RULES[action]

        # 启动作业时校验上游主题必须已上线
        if action == "启动作业":
            topic_code = entry.get("消费主题")
            topic = self._find_topic(topic_code) if topic_code else None
            if not topic:
                return None, f"消费主题 {topic_code} 未注册"
            if topic.get("status") != "已上线":
                return None, f"消费主题 {topic_code} 状态为「{topic.get('status')}」，须先上线"
            # 订单聚合任务必须先核对Schema兼容发布包
            if self.is_order_job(entry):
                usage = self.get_schema_usage(int(entry.get("id", 0)))
                if usage is None:
                    package = schema_compat.latest_published_for_topic(topic_code)
                    hint = (
                        f"可用发布包 {package.get('发布包编号')}（来源批次 {package.get('来源批次')}）"
                        if package else "暂无可用的兼容发布包"
                    )
                    return None, (
                        f"订单聚合任务启动前须核对Schema兼容发布包并登记使用版本；{hint}"
                    )
            # 生成计算产物
            self._generate_compute_artifact(entry, topic)

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"流式任务已{action}"

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _generate_compute_artifact(self, entry: dict[str, Any], topic: dict[str, Any]) -> None:
        """Flink作业运行时生成计算产物记录。"""
        output_table = entry.get("输出表名", "unknown")
        artifact = {
            "job_id": entry.get("id"),
            "job_code": entry.get("任务编号"),
            "topic_code": topic.get("主题编号"),
            "topic_name": topic.get("主题名称"),
            "output_table": output_table,
            "transform": entry.get("转换逻辑"),
            "parallelism": entry.get("并行度"),
            "computed_at": datetime.now().isoformat(),
            "status": "running",
        }
        self._ensure_artifact_table("flink_compute_artifacts")
        store.rows("flink_compute_artifacts").append(artifact)
        entry["_last_artifact"] = artifact

    def get_compute_artifacts(self, entry_id: int) -> list[dict[str, Any]]:
        self._ensure_artifact_table("flink_compute_artifacts")
        return [a for a in store.rows("flink_compute_artifacts") if a.get("job_id") == entry_id]

    # ───────────────────────────────
    # Schema兼容发布包核对与使用版本登记
    # ───────────────────────────────

    def is_order_job(self, job: dict[str, Any]) -> bool:
        return schema_compat.is_order_job(job)

    def get_schema_usage(self, entry_id: int) -> dict[str, Any] | None:
        """返回任务当前有效使用版本；已替换的历史登记不参与核对。"""
        rows = [
            row for row in schema_compat.usage_rows()
            if int(row.get("任务id", 0)) == entry_id and row.get("状态") != schema_compat.USAGE_REPLACED
        ]
        rows.sort(key=lambda row: int(row.get("id", 0)))
        return rows[-1] if rows else None

    def list_schema_usage(self, entry_id: int) -> list[dict[str, Any]]:
        rows = [row for row in schema_compat.usage_rows() if int(row.get("任务id", 0)) == entry_id]
        return sorted(rows, key=lambda row: int(row.get("id", 0)))

    def check_schema_package(
        self, entry_id: int, package_code: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """只读核对：不登记，返回发布包摘要、任务项与核对明细。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流式任务 {entry_id} 不存在或已归档"
        package = self._resolve_package(entry, package_code)
        if package is None:
            topic_code = entry.get("消费主题")
            last = schema_compat.latest_package_for_topic(topic_code)
            if last is not None:
                return None, (
                    f"主题 {topic_code} 最近发布包 {last.get('发布包编号')} 状态为"
                    f"「{last.get('状态')}」，未发布完成不能登记使用版本"
                )
            return None, f"主题 {topic_code} 暂无Schema兼容发布包，请先在Schema演进页生成"
        errors, checks = schema_compat.verify_job_package(entry, package)
        data = schema_compat.summary(package)
        jobs = schema_compat.items_for_package(str(data["发布包编号"]))
        job_items = [item for item in jobs if int(item.get("任务id", 0)) == int(entry_id)]
        result = {
            "任务id": entry_id,
            "任务编号": entry.get("任务编号"),
            "输出表名": entry.get("输出表名"),
            "发布包": data,
            "任务项": job_items,
            "核对项": checks,
            "ok": not errors,
            "已登记": self.get_schema_usage(entry_id) is not None,
        }
        return result, "核对通过" if not errors else "；".join(errors)

    def register_schema_usage(
        self, entry_id: int, package_code: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """核对发布包后登记任务使用版本，登记记录保留来源批次。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"流式任务 {entry_id} 不存在或已归档"
        package = self._resolve_package(entry, package_code)
        if package is None:
            return None, f"任务 {entry_id} 没有可登记的Schema兼容发布包"
        errors, checks = schema_compat.verify_job_package(entry, package)
        if errors:
            return None, "；".join(errors)

        data = schema_compat.summary(package)
        job_items = [
            item for item in schema_compat.items_for_package(str(data["发布包编号"]))
            if int(item.get("任务id", 0)) == int(entry_id)
        ]
        previous = self.get_schema_usage(entry_id)
        usage_id = schema_compat.next_id(schema_compat.usage_rows())
        usage = {
            "id": usage_id,
            "登记编号": f"USAG-{usage_id:04d}",
            "任务id": entry_id,
            "任务编号": entry.get("任务编号"),
            "消费主题": entry.get("消费主题"),
            "输出表名": entry.get("输出表名"),
            "发布包编号": data["发布包编号"],
            "来源批次": data["来源批次"],
            "读取版本": job_items[-1].get("读取版本") if job_items else data["当前可读版本"],
            "数据版本": data["数据版本"],
            "切换策略": data["切换策略"],
            "任务可见列": list(job_items[-1].get("任务可见列") or data["可见列"]) if job_items else data["可见列"],
            "上一份完整兼容包": data["上一份完整兼容包"],
            "旧版读取结果": data["旧版读取结果"],
            "核对项": checks,
            "状态": schema_compat.USAGE_REGISTERED,
            "登记时间": schema_compat.now_iso(),
            "更新时间": schema_compat.now_iso(),
        }
        schema_compat.usage_rows().append(usage)
        if previous is not None:
            # 旧登记保留为历史，最新一条代表当前使用版本
            previous["状态"] = schema_compat.USAGE_REPLACED
            previous["更新时间"] = schema_compat.now_iso()
        entry["_schema_usage"] = {
            "发布包编号": data["发布包编号"],
            "来源批次": data["来源批次"],
            "读取版本": usage["读取版本"],
        }
        return usage, (
            f"任务 {entry.get('任务编号')} 已登记使用版本 {usage['读取版本']}，"
            f"发布包 {data['发布包编号']}，来源批次 {data['来源批次']}"
        )

    def _resolve_package(
        self, entry: dict[str, Any], package_code: str | None,
    ) -> dict[str, Any] | None:
        topic_code = entry.get("消费主题")
        if package_code:
            package = schema_compat.find_package(package_code)
            if package is not None and package.get("关联主题") == topic_code:
                return package
            return None
        # 默认取该主题最近一份已发布包；已回滚的包不会被自动选中
        return schema_compat.latest_published_for_topic(topic_code)

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)
