"""检查点保留/恢复数据接口：详情页下载、跨页面校验导入、状态页保留原因。

这些路径比通用资源路由更具体，必须在 ``resource`` 之前注册，否则
``/api/checkpoint_mgr/{entry_id}`` 会抢先匹配。
"""
from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.services import service_for

router = APIRouter(prefix="/api/checkpoint-retention", tags=["检查点保留"])


class ReplayPayload(BaseModel):
    checkpoint_id: str | None = None
    topology: str = "任务拓扑样例1"


class RetentionWindowPayload(BaseModel):
    keep: int | None = Field(default=None, ge=0)
    mode: str = "immediate"
    replay_task_id: int | None = None


class RecoveryBundlePayload(BaseModel):
    bundle: dict[str, Any]
    dry_run: bool = False


def _checkpoint_service():
    service = service_for("checkpoint_mgr")
    if service is None:
        raise HTTPException(status_code=503, detail="检查点服务不可用")
    return service


def _state_service():
    service = service_for("state_store")
    if service is None:
        raise HTTPException(status_code=503, detail="状态存储服务不可用")
    return service


@router.get("/configs")
def list_configs() -> dict[str, Any]:
    """可供页面选择的检查点配置。"""
    service = _checkpoint_service()
    configs = []
    for name in service._config_names():
        item = service._config_row(name) or {}
        configs.append({
            "config": name,
            "status": item.get("status", ""),
            "keep": item.get("保留数量", ""),
            "state_config": service.linked_state_config(name),
            "available_snapshots": len(service._available_rows(name)),
        })
    return {"configs": configs}


@router.get("/replays")
def list_replays(config: str | None = None) -> dict[str, Any]:
    """回放历史：页面用它确认所有可用快照都能恢复。"""
    replay_service = service_for("replay_engine")
    if replay_service is None:
        raise HTTPException(status_code=503, detail="回放引擎服务不可用")
    return {
        "items": replay_service.list_replay_history(),
        "coverage": replay_service.restore_coverage(config),
    }


@router.get("/ledger")
def export_ledger() -> dict[str, Any]:
    """导出台账：跨页面接收同源导出文件时用它核对版本与覆盖区间。"""
    ledger = _checkpoint_service().export_ledger()
    return {"total": len(ledger), "items": ledger}


@router.get("/state-consumption")
def state_consumption(config: str | None = None) -> dict[str, Any]:
    """状态存储调用链读取的可用快照视图。"""
    return _checkpoint_service().state_store_consumes_snapshots(config)


@router.get("/{config_name}/report")
def retention_report(config_name: str) -> dict[str, Any]:
    result = _checkpoint_service().retention_report(config_name)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error"))
    return result


@router.get("/{config_name}/coverage")
def coverage(config_name: str) -> dict[str, Any]:
    result = _checkpoint_service().coverage_report(config_name)
    return result


@router.get("/{config_name}/export")
def export_bundle(
    config_name: str,
    boundary: str = Query(default="checkpoint", pattern="^(checkpoint|state)$"),
) -> Response:
    result = _checkpoint_service().export_recovery_bundle(config_name, boundary)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    body = json.dumps(result["bundle"], ensure_ascii=False, indent=2)
    return Response(
        content=body.encode("utf-8"),
        media_type="application/json; charset=utf-8",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{result["file_name"]}"; '
                f"filename*=UTF-8''{quote(result['file_name'])}"
            ),
            "X-Recovery-Version": result["version"],
        },
    )


@router.post("/verify")
def verify_bundle(payload: RecoveryBundlePayload) -> dict[str, Any]:
    """校验恢复数据文件：同源导出再次上传、跨页面接收时都走这里。"""
    return _checkpoint_service().verify_recovery_bundle(payload.bundle)


@router.post("/import")
def import_bundle(payload: RecoveryBundlePayload) -> dict[str, Any]:
    """导入状态恢复数据；同版本文件只入库一次。"""
    return _checkpoint_service().import_recovery_bundle(
        payload.bundle, dry_run=payload.dry_run)


@router.post("/{config_name}/window")
def apply_window(
    config_name: str, payload: RetentionWindowPayload,
) -> dict[str, Any]:
    result = _checkpoint_service().apply_retention_window(
        config_name, keep=payload.keep, mode=payload.mode,
        replay_task_id=payload.replay_task_id)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    return result


@router.post("/{config_name}/replay")
def run_replay(config_name: str, payload: ReplayPayload) -> dict[str, Any]:
    """用可用快照演练一次回放；成功后会兑现延迟裁剪。

    延迟模式下旧快照在成功回放前一直可用，回放通过后才释放并压缩状态，
    和立即裁剪模式最终收敛到同一个连续可恢复区间。
    """
    checkpoint_service = _checkpoint_service()
    replay_service = service_for("replay_engine")
    if replay_service is None:
        raise HTTPException(status_code=503, detail="回放引擎服务不可用")

    available = checkpoint_service.available_snapshots(config_name)
    if not available:
        raise HTTPException(status_code=400, detail="该检查点配置下没有可用快照")

    checkpoint_id = payload.checkpoint_id or available[-1]["checkpoint_id"]
    job = replay_service.create_replay_job(
        f"保留边界演练_{config_name}_{checkpoint_id}",
        checkpoint_id,
        payload.topology,
    )
    if not job.get("ok"):
        raise HTTPException(status_code=400, detail=job.get("error"))
    execution = replay_service.execute_replay(job["task_id"])
    report = checkpoint_service.retention_report(config_name)
    return {
        "ok": execution.get("ok", False),
        "task_id": job["task_id"],
        "checkpoint_id": checkpoint_id,
        "execution": execution,
        "retention": execution.get("retention"),
        "report": {
            "available_snapshots": report.get("available_snapshots"),
            "contiguous": report.get("contiguous"),
            "interval": report.get("interval"),
        },
    }


@router.get("/{config_name}/state-retention")
def state_retention(config_name: str) -> dict[str, Any]:
    """状态页面读取：每行状态的保留原因及被哪些快照引用。"""
    return {
        "config": config_name,
        "items": _state_service().retention_reasons(config_name),
        "active_state_keys": _state_service().list_active_state_keys(),
    }
