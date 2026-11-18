"""回放引擎领域服务：消费检查点数据执行数据回放与恢复验证。

数据流: checkpoint_mgr（检查点管理）→ replay_engine（回放引擎）→ restore_plan（故障恢复）
核心逻辑: 检查点快照回放、状态恢复验证、位点对齐、回放进度管理。

回放边界:
  - 可回放的源只能是 checkpoint_mgr ``available_snapshots()`` 给出的快照；
  - 回放读取恢复数据时，快照键集合与状态集合必须逐键一致，否则回放失败；
  - 延迟裁剪模式下，只有新回放验证通过才释放旧快照。
"""
from __future__ import annotations

import copy
import time
from typing import Any

from app.services import retention
from app.services.base import BaseModuleService
from app.store import store


class ReplayEngineService(BaseModuleService):
    MODULE = "replay_engine"
    ENTITY = "回放任务"
    LIST_FIELDS = ["任务名称", "源检查点", "目标拓扑", "回放模式", "起始位点", "结果验证", "进度"]
    REQUIRED_FIELDS = ["任务名称", "源检查点", "回放模式"]
    STATUS_ORDER = ["草稿", "待回放", "回放中", "已完成", "验证失败"]
    ACTION_RULES = {"启动回放": "待回放", "暂停回放": "回放中", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = ["暂停回放"]

    def _checkpoint_service(self):
        from app.services import service_for

        return service_for("checkpoint_mgr")

    def _find_available_snapshot(self, checkpoint_id: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        service = self._checkpoint_service()
        if service is None:
            return None, []
        available = service.available_snapshots()
        for item in available:
            if item["checkpoint_id"] == str(checkpoint_id):
                return item, available
        return None, available

    def create_replay_job(
        self,
        job_name: str,
        checkpoint_id: str,
        topology_name: str,
        mode: str = "full_replay",
    ) -> dict[str, Any]:
        """基于检查点创建数据回放任务。

        消费 checkpoint_mgr 中的检查点快照，结合 task_topology 的拓扑配置，
        创建回放任务并写入恢复方案的引用。只接受可用快照集合内的检查点。
        """
        # 1. 验证检查点在可用快照集合内
        snapshot, available = self._find_available_snapshot(checkpoint_id)
        if snapshot is None:
            if available:
                return {
                    "ok": False,
                    "error": f"检查点「{checkpoint_id}」不在可用快照集合内（可能已裁剪）",
                    "available_checkpoints": [s["checkpoint_id"] for s in available],
                }
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在或没有可用快照"}

        # 2. 验证拓扑配置
        topo_found = any(
            r.get("拓扑名称") == topology_name
            for r in store.rows("task_topology")
        )
        if not topo_found and topology_name:
            return {"ok": False, "error": f"拓扑配置「{topology_name}」不存在"}

        # 3. 创建回放任务
        task_id = max(
            (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
        ) + 1
        replay_task = {
            "id": task_id,
            "任务名称": job_name,
            "源检查点": str(checkpoint_id),
            "目标拓扑": topology_name,
            "回放模式": mode,
            "起始位点": snapshot.get("snapshot_ts", time.time() - 3600),
            "结果验证": "待验证",
            "进度": "0%",
            "status": "待回放",
            "pending": True,
            "abnormal": False,
            "_checkpoint_snapshot": copy.deepcopy(snapshot),
            "_checkpoint_state_keys": list(snapshot.get("state_keys", [])),
            "_checkpoint_state_versions": dict(snapshot.get("state_versions", {})),
            "_created_at": time.time(),
        }
        store.rows(self.MODULE).append(replay_task)

        return {
            "ok": True,
            "task_id": task_id,
            "task_name": job_name,
            "checkpoint_id": checkpoint_id,
            "topology": topology_name,
            "mode": mode,
            "state_keys": replay_task["_checkpoint_state_keys"],
        }

    def execute_replay(self, task_id: int) -> dict[str, Any]:
        """执行回放任务：检查点状态快照回放 + 结果验证。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "error": "回放任务不存在"}

        if task.get("status") not in ("待回放", "回放中"):
            return {"ok": False, "error": f"任务状态为「{task.get('status')}」, 不可回放"}

        source_id = str(task.get("源检查点", ""))
        snapshot, available = self._find_available_snapshot(source_id)
        if snapshot is None:
            task["结果验证"] = "源快照不可用"
            task["status"] = "验证失败"
            task["abnormal"] = True
            return {
                "ok": False,
                "error": f"源检查点「{source_id}」已不在可用快照集合内，回放拒绝执行",
                "available_checkpoints": [s["checkpoint_id"] for s in available],
            }

        task["status"] = "回放中"

        state_data = self._load_recovery_states(
            source_id, snapshot, task.get("_checkpoint_state_keys") or [])
        consistency = self._verify_state_key_consistency(
            snapshot.get("state_keys", []), state_data)

        if not consistency["consistent"]:
            task["结果验证"] = "状态集合不一致"
            task["status"] = "验证失败"
            task["abnormal"] = True
            task["_replay_consistency"] = consistency
            return {
                "ok": False,
                "error": "快照引用的键集合与状态恢复数据不一致，回放终止",
                "consistency": consistency,
            }

        replay_results = self._restore_states_to_store(
            task_id, source_id, state_data, snapshot)
        verification = self._verify_replay_results(task_id, replay_results)
        verification["consistency"] = consistency

        task["结果验证"] = verification["status"]
        task["进度"] = "100%"
        task["status"] = "已完成" if verification["all_passed"] else "验证失败"
        task["_replay_results"] = replay_results
        task["_replay_consistency"] = consistency

        retention_result = None
        if task["status"] == "已完成":
            retention_result = self._complete_retention_after_replay(task, snapshot)

        return {
            "ok": True,
            "task_id": task_id,
            "restored_operators": len(replay_results),
            "replay_results": replay_results,
            "verification": verification,
            "retention": retention_result,
        }

    def _load_recovery_states(
        self,
        checkpoint_id: str,
        snapshot: dict[str, Any],
        expected_keys: list[str],
    ) -> dict[str, Any]:
        """读取状态恢复数据：按快照引用的版本从状态存储精确取值。

        取不到时回落到快照正文，但只回落到与快照键集合一致的部分；两者都缺键
        就交由一致性校验拒绝本次回放。
        """
        state_service = self._state_service()
        state_config = snapshot.get("state_config") or snapshot.get("config", "")
        versions = snapshot.get("state_versions") or {}
        recovered: dict[str, Any] = {}
        if state_service is not None:
            for key in expected_keys or snapshot.get("state_keys", []):
                version = versions.get(key, "")
                value = (
                    state_service.state_value_for_version(state_config, key, version)
                    if version else state_service.state_value(state_config, key)
                )
                if value is not None:
                    recovered[key] = value

        if len(recovered) >= len(expected_keys or snapshot.get("state_keys", [])):
            return copy.deepcopy(recovered)

        checkpoint_service = self._checkpoint_service()
        restored = (
            checkpoint_service.restore_state(checkpoint_id)
            if checkpoint_service is not None else {"ok": False}
        )
        snapshot_states = restored.get("states") if restored.get("ok") else None
        if isinstance(snapshot_states, dict):
            for key, value in retention.snapshot_state_values(snapshot_states).items():
                recovered.setdefault(key, value)
        return copy.deepcopy(recovered)

    @staticmethod
    def _verify_state_key_consistency(
        snapshot_keys: list[str], state_data: dict[str, Any],
    ) -> dict[str, Any]:
        expected = sorted(str(key) for key in snapshot_keys)
        actual = sorted(str(key) for key in state_data.keys())
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        return {
            "consistent": not missing and not extra,
            "expected_count": len(expected),
            "actual_count": len(actual),
            "missing_keys": missing,
            "extra_keys": extra,
        }

    def _restore_states_to_store(
        self,
        task_id: int,
        checkpoint_id: str,
        state_data: dict[str, Any],
        snapshot: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """把恢复状态按算子聚合后写入 state_store。"""
        by_operator: dict[str, dict[str, Any]] = {}
        for key, value in state_data.items():
            operator, _, field = str(key).partition("::")
            by_operator.setdefault(operator, {})[field or "__scalar__"] = value

        replay_results = []
        for op_name, op_state in by_operator.items():
            state_entry = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("state_store")
                     if str(r.get("id", "")).isdigit()), default=0
                ) + 1,
                "配置名称": f"replay_{op_name}_{task_id}",
                "后端类型": "memory",
                "状态键模式": f"replay://{op_name}",
                "TTL时长": "3600",
                "检查点间隔": "30",
                "增量模式": "full",
                "清理策略": "after_replay",
                "status": "已生效",
                "pending": True,
                "abnormal": False,
                "_replay_task_id": task_id,
                "_replay_operator": op_name,
                "_replay_from_checkpoint": checkpoint_id,
                "_replay_state": copy.deepcopy(op_state),
                "_state_config": snapshot.get("config", ""),
                "_state_source": "replay",
                "_state_versions": {
                    key: retention.digest_value(value)
                    for key, value in state_data.items()
                    if str(key).split("::", 1)[0] == op_name
                },
                "_state_keys": sorted(
                    key for key in state_data
                    if str(key).split("::", 1)[0] == op_name
                ),
                "_created_at": time.time(),
            }
            store.rows("state_store").append(state_entry)
            replay_results.append({
                "operator": op_name,
                "state_keys": len(op_state),
                "restored_to": "state_store",
            })
        return replay_results

    def _complete_retention_after_replay(
        self, task: dict[str, Any], snapshot: dict[str, Any],
    ) -> dict[str, Any] | None:
        """延迟裁剪收敛：新回放成功后才释放旧快照并压缩被释放状态。"""
        checkpoint_service = self._checkpoint_service()
        if checkpoint_service is None:
            return None
        config_name = snapshot.get("config", "")
        if not config_name:
            return None
        result = checkpoint_service.complete_deferred_retention(
            config_name, int(task.get("id", 0)), replay_ok=True)
        if result.get("pruned") or result.get("compacted"):
            return result
        return None

    def _state_service(self):
        from app.services import service_for

        return service_for("state_store")

    def _verify_replay_results(
        self, task_id: int, results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """验证回放结果的完整性：检查 state_store 中是否正确落入了回放状态。"""
        restored_ops = set(r["operator"] for r in results)
        found_in_store = set()
        for row in store.rows("state_store"):
            if row.get("_replay_task_id") == task_id:
                found_in_store.add(row.get("_replay_operator", ""))

        missing = restored_ops - found_in_store
        all_passed = len(missing) == 0
        return {
            "status": "passed" if all_passed else "partial",
            "all_passed": all_passed,
            "expected": len(restored_ops),
            "found": len(found_in_store),
            "missing_operators": list(missing),
        }

    def list_replay_history(self) -> list[dict[str, Any]]:
        """列出所有回放任务历史记录及恢复方案关联。"""
        checkpoint_service = self._checkpoint_service()
        available_ids = {
            item["checkpoint_id"]
            for item in (checkpoint_service.available_snapshots()
                         if checkpoint_service is not None else [])
        }
        history = []
        for task in store.rows(self.MODULE):
            source_id = str(task.get("源检查点", ""))
            entry = {
                "task_id": task.get("id"),
                "任务名称": task.get("任务名称", ""),
                "源检查点": source_id,
                "回放模式": task.get("回放模式", ""),
                "结果验证": task.get("结果验证", "待验证"),
                "进度": task.get("进度", "0%"),
                "status": task.get("status", ""),
                "source_available": source_id in available_ids,
                "restorable": (
                    task.get("结果验证") == "passed"
                    and task.get("_replay_consistency", {}).get("consistent", True)
                ),
            }
            if task.get("_replay_consistency"):
                entry["state_key_consistency"] = task["_replay_consistency"]
            # 查找关联的恢复方案
            for rp in store.rows("restore_plan"):
                if task.get("目标拓扑", "") in rp.get("任务范围", ""):
                    entry["linked_restore_plan"] = rp.get("方案名称", "")
                    break
            history.append(entry)
        return history

    def restore_coverage(self, config_name: str | None = None) -> dict[str, Any]:
        """回放历史视角的覆盖报告：每个可用快照是否都有成功回放。"""
        checkpoint_service = self._checkpoint_service()
        available = (
            checkpoint_service.available_snapshots(config_name)
            if checkpoint_service is not None else []
        )
        restored: dict[str, dict[str, Any]] = {}
        for task in store.rows(self.MODULE):
            if task.get("结果验证") != "passed":
                continue
            source = str(task.get("源检查点", ""))
            consistency = task.get("_replay_consistency") or {}
            if not consistency.get("consistent", True):
                continue
            restored.setdefault(source, {
                "task_id": task.get("id"),
                "任务名称": task.get("任务名称", ""),
                "progress": task.get("进度", ""),
            })

        items = []
        for snapshot in available:
            cp_id = snapshot["checkpoint_id"]
            items.append({
                "checkpoint_id": cp_id,
                "config": snapshot.get("config", ""),
                "state_keys": snapshot.get("state_keys", []),
                "restorable": cp_id in restored,
                "replay_task_id": restored.get(cp_id, {}).get("task_id"),
            })
        missing = [item["checkpoint_id"] for item in items if not item["restorable"]]
        return {
            "available_checkpoints": len(items),
            "restored_checkpoints": len(items) - len(missing),
            "all_available_restorable": not missing,
            "missing_checkpoints": missing,
            "items": items,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，附加回放成功率。"""
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        completed = sum(1 for t in rows if t.get("status") == "已完成")
        failed = sum(1 for t in rows if t.get("status") == "验证失败")
        total_done = completed + failed
        inconsistent = sum(
            1 for t in rows
            if t.get("_replay_consistency")
            and not t["_replay_consistency"].get("consistent", True)
        )
        base["replay_success_rate"] = completed / max(total_done, 1)
        base["completed_replays"] = completed
        base["failed_replays"] = failed
        base["inconsistent_replays"] = inconsistent
        coverage = self.restore_coverage()
        base["available_checkpoints"] = coverage["available_checkpoints"]
        base["restorable_checkpoints"] = coverage["restored_checkpoints"]
        return base
