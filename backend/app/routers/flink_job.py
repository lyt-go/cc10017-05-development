"""流式计算 时间线接口：返回作业时间序列结构。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.flink_job import FlinkJobService
from app.store import store

router = APIRouter(prefix="/api/flink_job", tags=["流式计算"])
service = FlinkJobService()
LIST_FIELDS = ["任务编号", "消费主题", "转换逻辑", "输出表名", "并行度", "检查点间隔", "状态后端", "水位策略"]
STATUSES = ["草稿", "待校验", "运行中", "已停止"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=flink_job.csv"})


@router.get("/schema-usage")
def list_schema_usage() -> dict[str, Any]:
    """流式计算页的Schema兼容发布包核对区：逐个任务给出使用版本。"""
    from app import schema_compat

    items: list[dict[str, Any]] = []
    for job in store.rows("flink_job"):
        job_id = int(job.get("id", 0))
        usage = service.get_schema_usage(job_id)
        topic_packages = [
            schema_compat.summary(row)
            for row in reversed(schema_compat.packages_for_topic(job.get("消费主题")))
        ]
        candidate = next(
            (row for row in topic_packages if row["状态"] == schema_compat.STATUS_PUBLISHED),
            None,
        )
        items.append({
            "id": job_id,
            "任务编号": job.get("任务编号"),
            "消费主题": job.get("消费主题"),
            "输出表名": job.get("输出表名"),
            "status": job.get("status"),
            "订单任务": service.is_order_job(job),
            "usage": usage,
            "可用发布包": candidate,
            "可选发布包": [
                {"发布包编号": row["发布包编号"], "状态": row["状态"],
                 "来源批次": row["来源批次"], "当前可读版本": row["当前可读版本"]}
                for row in topic_packages
            ],
            "需登记": service.is_order_job(job) and usage is None and job.get("status") == "运行中",
        })
    return {"items": items}


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流式任务 {entry_id} 不存在")
    return entry


@router.get("/{entry_id}/schema-usage")
def get_schema_usage(entry_id: int) -> dict[str, Any]:
    """查看任务登记的Schema兼容发布包使用版本。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"流式任务 {entry_id} 不存在")
    return {
        "task": {"id": entry_id, "任务编号": entry.get("任务编号"),
                 "消费主题": entry.get("消费主题"), "输出表名": entry.get("输出表名")},
        "usage": service.get_schema_usage(entry_id),
        "history": service.list_schema_usage(entry_id),
    }


@router.post("/{entry_id}/schema-check")
def check_schema_package(entry_id: int, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """只读核对发布包：不写使用登记。"""
    payload = payload or {}
    result, message = service.check_schema_package(entry_id, payload.get("package"))
    return {"ok": result is not None and result.get("ok", False),
            "message": message, "check": result}


@router.post("/{entry_id}/schema-usage")
def register_schema_usage(entry_id: int, payload: dict[str, Any] | None = None) -> ActionResult:
    """核对发布包后登记任务使用版本，登记记录保留来源批次。"""
    payload = payload or {}
    usage, message = service.register_schema_usage(entry_id, payload.get("package"))
    if usage is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=usage)


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="流式任务已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
