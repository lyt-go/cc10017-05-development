"""Schema兼容发布包与跨模块登记测试。

覆盖：订单字段演进规则校验、发布包同时保留旧读取与新版映射、发布中断后保留上一份
完整兼容包并补齐剩余项、重复判定边界、回滚、流式计算核对与使用版本登记。
"""
from __future__ import annotations

import copy
import unittest
from typing import Any

from app import schema_compat
from app.services.flink_job import FlinkJobService
from app.services.schema_evolve import SchemaEvolveService
from app.store import store


class CompatTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._snapshot = copy.deepcopy(store._tables)
        self.svc = SchemaEvolveService()
        self.flink = FlinkJobService()

    def tearDown(self) -> None:
        store._tables = self._snapshot

    def make_rule(self, **overrides: Any) -> dict[str, Any]:
        values: dict[str, Any] = {
            "规则编号": "SCHE-9001", "关联主题": "TOPI-0001", "新增字段": "buyer_note",
            "删除字段": "-", "类型变更": "amount:decimal(18,4)", "默认值填充": "buyer_note=''",
            "兼容模式": "FORWARD", "生效版本": "v4",
        }
        values.update(overrides)
        rule, missing = self.svc.create_entry(values)
        self.assertEqual(missing, [], msg=f"规则登记失败：{missing}")
        self.svc.run_action(rule["id"], "提交校验")
        return rule

    def add_order_job(self, code: str = "FLIN-9002") -> dict[str, Any]:
        job, missing = self.flink.create_entry({
            "任务编号": code, "消费主题": "TOPI-0001", "转换逻辑": "订单金额二次聚合",
            "输出表名": "order_agg_detail",
        })
        self.assertEqual(missing, [])
        return job


class TestOrderRuleValidation(CompatTestBase):
    def test_order_rule_requires_default_for_new_field(self) -> None:
        rule = self.make_rule(默认值填充="")
        result, message = self.svc.validate_rule(rule["id"])
        self.assertIsNotNone(result)
        self.assertFalse(result["ok"])
        self.assertIn("默认值", message)
        failed = {item["校验项"] for item in result["失败项"]}
        self.assertIn("默认值填充覆盖新增字段", failed)

    def test_order_rule_rejects_duplicate_and_overlapping_fields(self) -> None:
        rule = self.make_rule(新增字段="buyer_note,buyer_note")
        result, _ = self.svc.validate_rule(rule["id"])
        self.assertFalse(result["ok"])
        failed = {item["校验项"] for item in result["失败项"]}
        self.assertIn("新增字段唯一", failed)

        rule2 = self.make_rule(
            规则编号="SCHE-9002", 新增字段="order_id", 默认值填充="order_id=''")
        result2, _ = self.svc.validate_rule(rule2["id"])
        self.assertFalse(result2["ok"])
        failed2 = {item["校验项"] for item in result2["失败项"]}
        self.assertIn("新增字段未重复声明", failed2)

    def test_order_rule_records_historical_readers(self) -> None:
        rule = self.make_rule()
        result, message = self.svc.validate_rule(rule["id"])
        self.assertTrue(result["ok"], message)
        self.assertTrue(result["订单字段校验"])
        names = {item["校验项"] for item in result["校验项"]}
        self.assertIn("订单历史任务保留旧读取", names)
        self.assertIn("订单聚合任务", names)


class TestReleasePackage(CompatTestBase):
    def test_package_keeps_old_read_and_new_mapping(self) -> None:
        rule = self.make_rule()
        package, message = self.svc.create_or_resume_release(rule["id"])
        self.assertIsNotNone(package, msg=message)
        self.assertEqual(package["状态"], schema_compat.STATUS_PUBLISHED)
        self.assertEqual(package["旧读取版本"], "v4")
        self.assertEqual(package["新映射版本"], "v5")
        self.assertIn("order_id", package["旧版读取结果"]["读取字段"])
        self.assertNotIn("buyer_note", package["旧版读取结果"]["读取字段"])
        self.assertIn("buyer_note", [m["目标字段"] for m in package["新字段映射"]["字段映射"]])
        # 发布包只声明一个当前可读版本
        self.assertEqual(package["当前可读版本"], "v5")
        self.assertEqual(package["数据版本"], "v5")

    def test_strategy_changes_readable_version_and_data_version(self) -> None:
        rule = self.make_rule()
        new_first, _ = self.svc.preview_release(rule["id"], strategy=schema_compat.STRATEGY_NEW_FIRST)
        old_first, _ = self.svc.preview_release(rule["id"], strategy=schema_compat.STRATEGY_OLD_FIRST)
        self.assertEqual(new_first["当前可读版本"], new_first["新映射版本"])
        self.assertEqual(old_first["当前可读版本"], old_first["旧读取版本"])
        self.assertNotEqual(new_first["数据版本"], old_first["数据版本"])
        self.assertNotEqual(new_first["可见列"], old_first["可见列"])
        # 冲突是否存在不随策略变化，取舍随策略变化
        self.assertTrue(new_first["冲突判定"]["存在冲突"])
        self.assertEqual(new_first["冲突判定"]["存在冲突"], old_first["冲突判定"]["存在冲突"])
        self.assertNotEqual(new_first["冲突判定"]["取舍"], old_first["冲突判定"]["取舍"])

    def test_interrupt_keeps_previous_complete_package(self) -> None:
        # 第一份完整包
        rule = self.make_rule()
        first, _ = self.svc.create_or_resume_release(rule["id"])
        first_code = first["发布包编号"]

        # 同一规则再演进一版（新增字段变化），制造多个任务项后中断
        store.find("schema_evolve", rule["id"])["新增字段"] = "buyer_note,buyer_phone"
        store.find("schema_evolve", rule["id"])["默认值填充"] = "buyer_note='',buyer_phone=''"
        self.add_order_job()
        second, message = self.svc.create_or_resume_release(
            rule["id"], source_batch="BATCH-MANUAL-01", interrupt_after=1)
        self.assertIsNotNone(second, msg=message)
        self.assertEqual(second["状态"], schema_compat.STATUS_INTERRUPTED)
        self.assertGreaterEqual(second["剩余项数"], 1)
        self.assertEqual(second["上一份完整兼容包"], first_code)

        # 上一份完整包原样保留，旧任务仍能读取
        old = self.svc.get_release(first_code)
        self.assertEqual(old["状态"], schema_compat.STATUS_PUBLISHED)
        self.assertEqual(old["当前可读版本"], "v5")
        self.assertIn("buyer_note", [m["目标字段"] for m in old["新字段映射"]["字段映射"]])

    def test_resume_fills_remaining_without_redoing_mapped(self) -> None:
        rule = self.make_rule()
        self.add_order_job()
        package, _ = self.svc.create_or_resume_release(
            rule["id"], source_batch="BATCH-MANUAL-02", interrupt_after=1)
        code = package["发布包编号"]
        self.assertEqual(package["状态"], schema_compat.STATUS_INTERRUPTED)
        mapped_before = {
            item["任务id"]: item["应用时间"]
            for item in schema_compat.items_for_package(code)
            if item["映射状态"] == schema_compat.ITEM_MAPPED
        }
        self.assertTrue(mapped_before)

        resumed, message = self.svc.resume_release(code)
        self.assertIsNotNone(resumed, msg=message)
        self.assertEqual(resumed["发布包编号"], code)
        self.assertEqual(resumed["状态"], schema_compat.STATUS_PUBLISHED)
        self.assertEqual(resumed["剩余项数"], 0)
        items = {item["任务id"]: item for item in schema_compat.items_for_package(code)}
        for job_id, applied_at in mapped_before.items():
            self.assertEqual(items[job_id]["应用时间"], applied_at, "已完成映射的任务项不应重复应用")
        self.assertEqual(len(items), 2)

    def test_retry_with_same_batch_resumes_same_package(self) -> None:
        rule = self.make_rule()
        self.add_order_job()
        first, _ = self.svc.create_or_resume_release(
            rule["id"], source_batch="BATCH-MANUAL-03", interrupt_after=1)
        count = len(schema_compat.packages())

        retried, message = self.svc.create_or_resume_release(
            rule["id"], source_batch="BATCH-MANUAL-03")
        self.assertIsNotNone(retried, msg=message)
        self.assertEqual(retried["发布包编号"], first["发布包编号"])
        self.assertEqual(retried["状态"], schema_compat.STATUS_PUBLISHED)
        self.assertEqual(len(schema_compat.packages()), count, "重试不应新建发布包")

    def test_retry_without_batch_picks_up_incomplete_package(self) -> None:
        rule = self.make_rule()
        self.add_order_job()
        first, _ = self.svc.create_or_resume_release(
            rule["id"], source_batch="BATCH-MANUAL-04", interrupt_after=1)
        retried, message = self.svc.create_or_resume_release(rule["id"])
        self.assertIsNotNone(retried, msg=message)
        self.assertEqual(retried["发布包编号"], first["发布包编号"])
        self.assertEqual(retried["状态"], schema_compat.STATUS_PUBLISHED)

    def test_already_switched_jobs_do_not_read_old_fields_again(self) -> None:
        rule = self.make_rule()
        first, _ = self.svc.create_or_resume_release(rule["id"])
        self.assertEqual(first["状态"], schema_compat.STATUS_PUBLISHED)
        count = len(schema_compat.packages())

        again, message = self.svc.create_or_resume_release(rule["id"])
        self.assertIsNotNone(again, msg=message)
        self.assertEqual(again["发布包编号"], first["发布包编号"])
        self.assertIn("未重复读取旧字段", message)
        self.assertEqual(len(schema_compat.packages()), count)

    def test_rollback_restores_old_read(self) -> None:
        rule = self.make_rule()
        package, _ = self.svc.create_or_resume_release(rule["id"])
        code = package["发布包编号"]
        self.flink.register_schema_usage(1, code)

        rolled, message = self.svc.rollback_release(code)
        self.assertIsNotNone(rolled, msg=message)
        self.assertEqual(rolled["状态"], schema_compat.STATUS_ROLLED_BACK)
        self.assertEqual(rolled["当前可读版本"], package["旧读取版本"])
        for item in schema_compat.items_for_package(code):
            self.assertEqual(item["映射状态"], schema_compat.ITEM_ROLLED_BACK)
            self.assertEqual(item["读取版本"], package["旧读取版本"])
        usage = self.flink.get_schema_usage(1)
        self.assertEqual(usage["状态"], schema_compat.USAGE_ROLLED_BACK)
        self.assertEqual(usage["读取版本"], package["旧读取版本"])
        self.assertEqual(usage["数据版本"], package["旧读取版本"])
        # 回滚后旧读取仍可用于历史任务：核对可通过、任务仍可启动
        check, message = self.flink.check_schema_package(1, code)
        self.assertTrue(check["ok"], message)
        entry, message = self.flink.run_action(1, "启动作业")
        self.assertIsNotNone(entry, msg=message)

    def test_reregister_replaces_previous_usage(self) -> None:
        rule = self.make_rule()
        package, _ = self.svc.create_or_resume_release(rule["id"])
        code = package["发布包编号"]
        before = len(self.flink.list_schema_usage(1))
        self.flink.register_schema_usage(1, code)
        self.flink.register_schema_usage(1, code)
        active = self.flink.get_schema_usage(1)
        self.assertEqual(active["状态"], schema_compat.USAGE_REGISTERED)
        history = self.flink.list_schema_usage(1)
        self.assertEqual(len(history), before + 2)
        self.assertEqual(history[-1]["状态"], schema_compat.USAGE_REGISTERED)
        self.assertTrue(all(row["状态"] == schema_compat.USAGE_REPLACED for row in history[:-1]))

    def test_list_and_detail_summary_consistent(self) -> None:
        rule = self.make_rule()
        self.add_order_job()
        package, _ = self.svc.create_or_resume_release(
            rule["id"], source_batch="BATCH-MANUAL-05", interrupt_after=1)
        code = package["发布包编号"]
        listed = next(item for item in self.svc.list_releases() if item["发布包编号"] == code)
        detail = self.svc.get_release(code)
        for key in ("状态", "已完成项数", "总项数", "剩余项数", "当前可读版本",
                    "数据版本", "来源批次", "上一份完整兼容包"):
            self.assertEqual(listed[key], detail[key], f"{key} 在列表与详情间不一致")


class TestStreamingRegistration(CompatTestBase):
    def test_usage_registration_keeps_source_batch(self) -> None:
        rule = self.make_rule()
        package, _ = self.svc.create_or_resume_release(rule["id"])
        usage, message = self.flink.register_schema_usage(1, package["发布包编号"])
        self.assertIsNotNone(usage, msg=message)
        self.assertEqual(usage["发布包编号"], package["发布包编号"])
        self.assertEqual(usage["来源批次"], package["来源批次"])
        self.assertEqual(usage["读取版本"], package["当前可读版本"])
        self.assertEqual(usage["数据版本"], package["数据版本"])
        self.assertEqual(usage["任务可见列"], package["可见列"])

    def test_registration_runs_checks_and_exposes_result(self) -> None:
        rule = self.make_rule()
        package, _ = self.svc.create_or_resume_release(rule["id"])
        check, message = self.flink.check_schema_package(1, package["发布包编号"])
        self.assertIsNotNone(check, msg=message)
        self.assertTrue(check["ok"])
        names = {item["核对项"] for item in check["核对项"]}
        self.assertIn("发布包状态", names)
        self.assertIn("任务项映射状态", names)

    def test_registration_rejects_unpublished_package(self) -> None:
        rule = self.make_rule()
        self.add_order_job()
        package, _ = self.svc.create_or_resume_release(
            rule["id"], source_batch="BATCH-MANUAL-06", interrupt_after=1)
        usage, message = self.flink.register_schema_usage(1, package["发布包编号"])
        self.assertIsNone(usage)
        self.assertIn("发布包状态", message)
        self.assertIn("已中断", message)

    def test_order_job_start_requires_registration(self) -> None:
        # 新建订单任务：未登记使用版本不能启动
        job = self.add_order_job("FLIN-9009")
        entry, message = self.flink.run_action(job["id"], "启动作业")
        self.assertIsNone(entry)
        self.assertIn("核对Schema兼容发布包", message)

    def test_order_job_start_after_registration(self) -> None:
        job = self.add_order_job("FLIN-9010")
        rule = self.make_rule()
        package, _ = self.svc.create_or_resume_release(
            rule["id"], job_ids=[job["id"]])
        self.assertIsNotNone(package)
        usage, message = self.flink.register_schema_usage(job["id"], package["发布包编号"])
        self.assertIsNotNone(usage, msg=message)
        entry, message = self.flink.run_action(job["id"], "启动作业")
        self.assertIsNotNone(entry, msg=message)
        self.assertEqual(entry["status"], "运行中")


if __name__ == "__main__":
    unittest.main()
