"""Schema演进接口：维护演进规则，覆盖提交校验、标记生效、执行回滚等动作。

另有Schema兼容发布包接口：规则校验、预览、生成/恢复、中断、恢复、发布、回滚、
详情、列表与导出。发布包写入只在服务层发生，路由只做参数适配。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app import schema_compat
from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.schema_evolve import SchemaEvolveService

router = APIRouter(prefix="/api/schema_evolve", tags=["Schema演进"])

service = SchemaEvolveService()

LIST_FIELDS = ["规则编号", "关联主题", "新增字段", "删除字段", "类型变更", "默认值填充", "兼容模式", "生效版本"]
STATUSES = ["草稿", "待校验", "已生效", "已回滚"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


def _job_ids(payload: dict[str, Any]) -> list[int] | None:
    raw = payload.get("job_ids")
    if not raw:
        return None
    return [int(item) for item in raw]


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按规则编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已生效、已回滚"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、规则编号关键字与状态过滤Schema演进列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/releases")
def list_releases(limit: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
    """兼容发布包列表：与详情共用同一摘要口径。"""
    return {"items": service.list_releases(limit), "total": len(service.list_releases(limit))}


@router.get("/releases/export")
def export_releases(package: str | None = Query(default=None)) -> StreamingResponse:
    """导出兼容包 CSV；指定 package 时只导出该发布包，否则导出全部已完成/进行中的包。"""
    fields = service.release_export_fields()
    packages: list[dict[str, Any]]
    if package:
        target = schema_compat.find_package(package)
        if target is None:
            raise HTTPException(status_code=404, detail=f"发布包 {package} 不存在")
        packages = [target]
    else:
        packages = list(reversed(schema_compat.latest_packages(200)))

    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(fields)
    for row_package in packages:
        rows, _ = service.export_release_rows(str(row_package.get("发布包编号")))
        for row in rows:
            cells = []
            for field in fields:
                value = row.get(field)
                text = "" if value is None else str(value)
                cells.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
            writer.writerow(cells)
    filename = f"{package}.csv" if package else "schema_compat_releases.csv"
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.post("/releases/preview")
def preview_release(payload: dict[str, Any]) -> dict[str, Any]:
    """只读预览发布包：冲突判定、任务可见列与待登记任务。"""
    try:
        rule_id = int(payload.get("rule_id"))
    except (TypeError, ValueError):
        return {"ok": False, "message": "缺少 rule_id", "preview": None}
    preview, message = service.preview_release(
        rule_id,
        strategy=str(payload.get("strategy") or schema_compat.STRATEGY_NEW_FIRST),
        job_ids=_job_ids(payload),
    )
    return {"ok": preview is not None, "message": message, "preview": preview}


@router.post("/releases")
def create_release(payload: dict[str, Any]) -> ActionResult:
    """生成或恢复兼容发布包；同一规则有未完成发布包时续做剩余项。"""
    try:
        rule_id = int(payload.get("rule_id"))
    except (TypeError, ValueError):
        return ActionResult(ok=False, message="缺少 rule_id")
    package, message = service.create_or_resume_release(
        rule_id,
        strategy=str(payload.get("strategy") or schema_compat.STRATEGY_NEW_FIRST),
        source_batch=payload.get("source_batch"),
        job_ids=_job_ids(payload),
        apply=bool(payload.get("apply", True)),
        interrupt_after=payload.get("interrupt_after"),
    )
    if package is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=package)


@router.post("/releases/{package_code}/apply")
def apply_release(package_code: str, payload: dict[str, Any] | None = None) -> ActionResult:
    """继续应用剩余任务项。"""
    payload = payload or {}
    package, message = service.apply_release(
        package_code,
        limit=payload.get("limit"),
        interrupt=bool(payload.get("interrupt", False)),
    )
    if package is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=package)


@router.post("/releases/{package_code}/interrupt")
def interrupt_release(package_code: str) -> ActionResult:
    """中断一次发布：保留已完成映射项和上一份完整兼容包。"""
    package, message = service.interrupt_release(package_code)
    if package is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=package)


@router.post("/releases/{package_code}/resume")
def resume_release(package_code: str) -> ActionResult:
    """中断后恢复同一次发布并补齐剩余项。"""
    package, message = service.resume_release(package_code)
    if package is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=package)


@router.post("/releases/{package_code}/rollback")
def rollback_release(package_code: str) -> ActionResult:
    """回滚到发布包保留的旧版读取结果。"""
    package, message = service.rollback_release(package_code)
    if package is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=package)


@router.get("/releases/{package_code}")
def get_release(package_code: str) -> dict:
    """发布包详情：摘要、任务项、使用登记与发布记录。"""
    detail = service.get_release(package_code)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"发布包 {package_code} 不存在")
    package = schema_compat.find_package(package_code)
    detail["发布记录"] = list((package or {}).get("发布记录") or [])
    return detail


@router.get("/{entry_id}/releases")
def list_rule_releases(entry_id: int) -> dict[str, Any]:
    """按演进规则查看其发布包历史。"""
    rule = service.get_entry(entry_id)
    if rule is None:
        raise HTTPException(status_code=404, detail=f"演进规则 {entry_id} 不存在或已归档")
    items = [schema_compat.summary(row) for row in schema_compat.packages_for_rule(entry_id)]
    return {"items": items, "total": len(items)}


@router.post("/{entry_id}/validate")
def validate_rule(entry_id: int) -> dict[str, Any]:
    """校验订单字段演进规则；不通过时逐项说明原因。"""
    result, message = service.validate_rule(entry_id)
    if result is None:
        return {"ok": False, "message": message, "validation": None}
    return {"ok": result["ok"], "message": message, "validation": result}


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="schema_evolve.csv"'})


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条演进规则明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"演进规则 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条演进规则，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="演进规则已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条演进规则执行提交校验、标记生效、执行回滚；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
