"""检查点领域服务：管理流式任务的检查点策略。

数据流角色: replay_engine → checkpoint_mgr（消费者+生产者）→ restore_plan（消费者）
增强能力: 检查点创建/快照/恢复/对齐、状态校验、外部化存储路径管理、多检查点一致性验证、
          保留窗口裁剪、可用快照集合、恢复数据导出与幂等导入。

保留边界约定:
  1. 快照裁剪只影响快照本身，不会把状态存储里的行标成停用；
  2. 状态压缩必须同时读取 ``available_snapshots()``，至少保留每个可用快照
     引用的状态键/版本；
  3. 保留数量缩短时支持「立即裁剪」和「延迟到新回放成功后裁剪」两种模式，
     两种模式的最终结果都收敛到同一个连续可恢复区间。
"""
from __future__ import annotations

import copy
import hashlib
import time
from typing import Any

from app.services import retention
from app.services.base import BaseModuleService
from app.store import store


class CheckpointMgrService(BaseModuleService):
    MODULE = "checkpoint_mgr"
    ENTITY = "检查点配置"
    LIST_FIELDS = ["配置名称", "间隔时间", "超时时长", "最大并发", "对齐策略", "保留数量", "外部化路径"]
    REQUIRED_FIELDS = ["配置名称", "间隔时间", "超时时长"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用检查点": "草稿", "停用检查点": "已启用"}
    NEGATIVE_ACTIONS = ["停用检查点"]

    RETENTION_MODES = {"immediate", "deferred"}
    RECOVERY_FORMAT = "state-recovery/v1"
    BOUNDARIES = {"checkpoint", "state"}

    # ---- 通用列表：快照行不混进配置清单 ----

    def list_entries(self, **kwargs: Any):
        items, total = super().list_entries(**kwargs)
        visible = [row for row in items if not retention.is_snapshot_row(row)]
        return visible, total - (len(items) - len(visible))

    # ---- 跨模块数据管理 ----

    def create_checkpoint(
        self,
        config_name: str,
        operator_states: dict[str, Any],
        state_row_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """创建算子状态检查点快照。

        为指定的检查点配置生成带有状态快照的检查点记录，
        存储到 store 中供 replay_engine 消费。

        ``state_row_ids`` 用于把快照显式绑定到状态存储里的状态行；不传时按
        operator_states 的 ``算子::字段`` 键与状态行做摘要匹配。
        """
        config = self._config_row(config_name)
        if config is None:
            return {"ok": False, "error": f"检查点配置「{config_name}」不存在"}
        if config.get("status") != "已启用":
            return {"ok": False, "error": f"配置「{config_name}」未启用"}

        try:
            interval = float(config.get("间隔时间", "30"))
        except (ValueError, TypeError):
            interval = 30.0

        cp_id = hashlib.md5(
            f"{config_name}_{time.time()}".encode()
        ).hexdigest()[:12]

        state_versions = retention.snapshot_state_versions(operator_states)
        state_row_ids = list(state_row_ids or [])
        if not state_row_ids:
            state_row_ids = self._materialize_state_versions(
                config_name, operator_states, cp_id)
        snapshot = {
            "id": cp_id,
            "配置名称": config_name,
            "间隔时间": str(interval),
            "超时时长": config.get("超时时长", "60"),
            "最大并发": config.get("最大并发", "3"),
            "对齐策略": config.get("对齐策略", "exactly_once"),
            "保留数量": int(config.get("保留数量", 5)),
            "外部化路径": config.get("外部化路径", "/data/checkpoints"),
            "status": "已启用",
            "pending": False,
            "abnormal": False,
            "checkpoint_id": cp_id,
            "operator_states": copy.deepcopy(operator_states),
            "state_keys": retention.snapshot_state_keys(operator_states),
            "state_versions": state_versions,
            "state_row_ids": list(state_row_ids or []),
            "snapshot_ts": time.time(),
            "retain_reason": "保留窗口内最新快照",
            "_created_at": time.time(),
        }
        store.rows(self.MODULE).append(snapshot)

        # 清理旧检查点（保留最近 N 个），只裁剪快照，不触碰状态存储
        self._prune_old_checkpoints(config_name)

        return {
            "ok": True,
            "checkpoint_id": cp_id,
            "config": config_name,
            "operators": list(operator_states.keys()),
            "state_keys": snapshot["state_keys"],
            "interval": interval,
        }

    def _materialize_state_versions(
        self,
        config_name: str,
        operator_states: dict[str, Any],
        checkpoint_id: str,
    ) -> list[int]:
        """把快照里的算子状态登记成状态存储版本。

        检查点本身带有状态正文，但状态存储才是恢复时读取的地方：只有把版本
        写进去，压缩/清理才会把「被快照引用的版本」保护下来。
        """
        from app.services import service_for

        state_service = service_for("state_store")
        if state_service is None:
            return []
        state_config = self.linked_state_config(config_name)
        row_ids: list[int] = []
        for operator, operator_state in operator_states.items():
            if not isinstance(operator_state, dict):
                operator_state = {"__scalar__": operator_state}
            result = state_service.record_state_version(
                state_config,
                operator,
                operator_state,
                checkpoint_ref=checkpoint_id,
                source="checkpoint",
            )
            if result.get("ok"):
                row_ids.append(int(result["state_row_id"]))
        return row_ids

    def _config_row(self, config_name: str) -> dict[str, Any] | None:
        for r in store.rows(self.MODULE):
            if not retention.is_snapshot_row(r) and r.get("配置名称") == config_name:
                return r
        return None

    def linked_state_config(self, config_name: str) -> str:
        """检查点配置绑定的状态存储配置：优先取显式「状态配置」，否则同名。"""
        config = self._config_row(config_name)
        linked = str((config or {}).get("状态配置") or "").strip()
        return linked or config_name

    def _config_names(self) -> list[str]:
        names = []
        for r in store.rows(self.MODULE):
            name = r.get("配置名称")
            if name and not retention.is_snapshot_row(r) and name not in names:
                names.append(name)
        return names

    def _snapshot_rows(self, config_name: str | None = None) -> list[dict[str, Any]]:
        rows = [r for r in store.rows(self.MODULE) if retention.is_snapshot_row(r)]
        if config_name:
            rows = [r for r in rows if r.get("配置名称") == config_name]
        return rows

    def _snapshot_by_id(self, checkpoint_id: str) -> dict[str, Any] | None:
        for row in self._snapshot_rows():
            if retention.snapshot_id(row) == str(checkpoint_id):
                return row
        return None

    def _keep_count(self, config_name: str, keep: int | None = None) -> int:
        if keep is not None:
            return max(int(keep), 0)
        config = self._config_row(config_name)
        raw = (config or {}).get("保留数量", 5)
        try:
            return max(int(raw), 0)
        except (ValueError, TypeError):
            return 5

    def _prune_old_checkpoints(self, config_name: str) -> None:
        """清理超过保留数量的旧检查点（只改快照状态，不触碰状态行）。"""
        max_keep = self._keep_count(config_name)
        snapshots = sorted(
            (r for r in self._snapshot_rows(config_name) if not r.get("_pruned")),
            key=retention.snapshot_timestamp,
            reverse=True,
        )
        for s in snapshots[max_keep:]:
            self._mark_pruned(s, "超过保留数量")

    @staticmethod
    def _mark_pruned(snapshot: dict[str, Any], reason: str) -> None:
        snapshot["status"] = "已停用"
        snapshot["_pruned"] = True
        snapshot["_pruned_at"] = time.time()
        snapshot["pruned_reason"] = reason
        snapshot.pop("_deferred_prune_after_replay", None)
        snapshot.pop("_deferred_for_task", None)

    def apply_retention_window(
        self,
        config_name: str,
        keep: int | None = None,
        mode: str = "immediate",
        replay_task_id: int | None = None,
    ) -> dict[str, Any]:
        """按保留数量裁剪检查点窗口。

        ``mode=immediate``：立即停用超出窗口的快照，并允许立刻压缩被释放的状态；
        ``mode=deferred``：先登记待裁剪快照，等新回放成功后由
        ``complete_deferred_retention`` 统一切换，最终收敛到同一区间。
        """
        config = self._config_row(config_name)
        if config is None:
            return {"ok": False, "error": f"检查点配置「{config_name}」不存在"}
        if mode not in self.RETENTION_MODES:
            return {"ok": False, "error": f"未知的保留模式「{mode}」"}

        max_keep = self._keep_count(config_name, keep)
        if keep is not None:
            config["保留数量"] = max_keep

        snapshots = sorted(
            (r for r in self._snapshot_rows(config_name)
             if not r.get("_pruned") and not r.get("_compacted")),
            key=retention.snapshot_timestamp,
            reverse=True,
        )
        overflow = snapshots[max_keep:]
        deferred_ids = []
        pruned_ids = []
        for snapshot in overflow:
            cp_id = retention.snapshot_id(snapshot)
            if mode == "immediate":
                self._mark_pruned(snapshot, "保留窗口缩短")
                pruned_ids.append(cp_id)
            else:
                # 延迟模式：快照在释放前仍然可用，回放可以继续引用它
                snapshot["_deferred_prune_after_replay"] = True
                snapshot["_deferred_for_task"] = replay_task_id
                snapshot["deferred_reason"] = "保留窗口缩短，待新回放成功后裁剪"
                deferred_ids.append(cp_id)

        compacted = 0
        if mode == "immediate" and pruned_ids:
            # 立即模式：快照一旦释放，被释放的状态版本可以立刻压缩
            compaction = self._compact_released_states(config_name)
            compacted = int(compaction.get("compacted", 0))

        return {
            "ok": True,
            "config": config_name,
            "mode": mode,
            "keep": max_keep,
            "total_snapshots": len(snapshots),
            "pruned": pruned_ids,
            "deferred": deferred_ids,
            "compacted": compacted,
            "replay_task_id": replay_task_id,
            "available_after": len(self._available_rows(config_name)),
        }

    def complete_deferred_retention(
        self,
        config_name: str,
        replay_task_id: int,
        replay_ok: bool,
    ) -> dict[str, Any]:
        """延迟裁剪的收敛点：只有新回放成功才真正释放旧快照。"""
        pending = [
            r for r in self._snapshot_rows(config_name)
            if r.get("_deferred_prune_after_replay") and not r.get("_pruned")
        ]
        # 只兑现本次回放之前登记的延迟裁剪
        if replay_task_id is not None:
            pending = [
                r for r in pending
                if r.get("_deferred_for_task") in (None, replay_task_id)
            ]
        if not replay_ok:
            return {
                "ok": False,
                "config": config_name,
                "replay_task_id": replay_task_id,
                "pruned": [],
                "deferred": [retention.snapshot_id(r) for r in pending],
                "error": "新回放未成功，延迟裁剪保持原状",
            }

        pruned = []
        for snapshot in pending:
            self._mark_pruned(snapshot, "延迟裁剪：新回放成功")
            pruned.append(retention.snapshot_id(snapshot))

        compacted = 0
        # 延迟裁剪成功才允许压缩被释放的状态版本
        result = self._compact_released_states(config_name)
        compacted = int(result.get("compacted", 0))
        return {
            "ok": True,
            "config": config_name,
            "replay_task_id": replay_task_id,
            "pruned": pruned,
            "compacted": compacted,
            "available_after": len(self._available_rows(config_name)),
        }

    def _compact_released_states(self, config_name: str) -> dict[str, Any]:
        """延迟裁剪成功后才允许状态存储压缩被释放的旧版本。"""
        from app.services import service_for  # 延迟导入，避免模块级循环依赖

        state_service = service_for("state_store")
        if state_service is None:
            return {"ok": False, "compacted": 0}
        return state_service.compact_duplicate_states(
            self.linked_state_config(config_name))

    def _available_rows(self, config_name: str | None = None) -> list[dict[str, Any]]:
        return [
            r for r in self._snapshot_rows(config_name)
            if r.get("status") == "已启用"
            and not r.get("_pruned")
            and not r.get("_compacted")
        ]

    def available_snapshots(self, config_name: str | None = None) -> list[dict[str, Any]]:
        """向状态存储/回放引擎暴露的可用快照集合（含键与版本摘要）。"""
        result = []
        for snapshot in sorted(self._available_rows(config_name),
                               key=retention.snapshot_timestamp):
            cp_id = retention.snapshot_id(snapshot)
            states = retention.snapshot_states(snapshot)
            keys = snapshot.get("state_keys") or retention.snapshot_state_keys(states)
            versions = snapshot.get("state_versions") or retention.snapshot_state_versions(states)
            config_name = snapshot.get("配置名称", "")
            result.append({
                "checkpoint_id": cp_id,
                "config": config_name,
                "state_config": self.linked_state_config(config_name),
                "snapshot_ts": retention.snapshot_timestamp(snapshot),
                "state_keys": list(keys),
                "state_versions": dict(versions),
                "state_row_ids": list(snapshot.get("state_row_ids") or []),
                "external_path": snapshot.get("外部化路径", ""),
                "retain_reason": snapshot.get(
                    "retain_reason", "保留窗口内最新快照"),
            })
        return result

    def available_snapshot_set(self, config_name: str | None = None) -> dict[str, dict[str, Any]]:
        return {item["checkpoint_id"]: item for item in self.available_snapshots(config_name)}

    def _snapshots_for_state_config(
        self, state_config: str | None,
    ) -> list[dict[str, Any]]:
        snapshots = self._available_rows()
        if state_config is None:
            return snapshots
        return [
            item for item in snapshots
            if self.linked_state_config(item.get("配置名称", "")) == state_config
        ]

    def state_retention_guard(
        self, config_name: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        """状态压缩的保留护栏：``状态键`` → 引用的版本集合/快照列表。

        ``config_name`` 是状态存储配置名；检查点配置通过「状态配置」字段或
        同名约定绑定到它。
        """
        guard: dict[str, dict[str, Any]] = {}
        for row in self._snapshots_for_state_config(config_name):
            cp_id = retention.snapshot_id(row)
            states = retention.snapshot_states(row)
            versions = row.get("state_versions") or retention.snapshot_state_versions(states)
            keys = row.get("state_keys") or retention.snapshot_state_keys(states)
            for key in keys:
                entry = guard.setdefault(key, {"versions": set(), "checkpoint_ids": []})
                version = versions.get(key)
                if version:
                    entry["versions"].add(version)
                if cp_id not in entry["checkpoint_ids"]:
                    entry["checkpoint_ids"].append(cp_id)
        return guard

    def referenced_state_keys(self, config_name: str | None = None) -> dict[str, list[str]]:
        """可用检查点引用的状态键及版本，供状态存储压缩和页面展示。"""
        return {
            key: sorted(entry["versions"])
            for key, entry in self.state_retention_guard(config_name).items()
        }

    def restore_state(self, checkpoint_id: str) -> dict[str, Any]:
        """从检查点恢复算子状态，验证完整性后返回。"""
        snapshot = self._snapshot_by_id(checkpoint_id)
        if snapshot is None:
            return {"ok": False, "error": f"检查点「{checkpoint_id}」不存在"}
        if snapshot.get("_pruned") or snapshot.get("status") != "已启用":
            return {
                "ok": False,
                "checkpoint_id": checkpoint_id,
                "error": "检查点已超出保留窗口，不可用于恢复",
                "retain_reason": snapshot.get("pruned_reason", "已裁剪"),
            }

        states = retention.snapshot_states(snapshot)
        # 验证状态完整性
        validation = self._validate_state_structure(states)

        return {
            "ok": validation["valid"],
            "checkpoint_id": checkpoint_id,
            "config": snapshot.get("配置名称", ""),
            "snapshot_ts": retention.snapshot_timestamp(snapshot),
            "operator_count": len(states),
            "operators": list(states.keys()),
            "state_keys": retention.snapshot_state_keys(states),
            "states": states,
            "validation": validation,
        }

    @staticmethod
    def _validate_state_structure(states: dict[str, Any]) -> dict[str, Any]:
        """验证状态数据结构完整性。"""
        issues = []
        if not isinstance(states, dict):
            return {"valid": False, "issues": ["状态结构不是 dict 类型"]}
        for op_name, op_state in states.items():
            if op_state is None:
                issues.append(f"算子「{op_name}」状态为 None")
            elif isinstance(op_state, dict) and len(op_state) == 0:
                issues.append(f"算子「{op_name}」状态为空字典")
        return {"valid": len(issues) == 0, "issues": issues}

    def verify_multi_checkpoint_consistency(
        self, config_name: str,
    ) -> dict[str, Any]:
        """验证同一配置下多个检查点之间的状态一致性。"""
        snapshots = sorted(
            self._snapshot_rows(config_name),
            key=retention.snapshot_timestamp,
        )

        if len(snapshots) < 2:
            return {"ok": True, "consistent": True, "message": "检查点不足2个，无需比较"}

        baseline = retention.snapshot_states(snapshots[0])
        inconsistencies = []
        for i, s in enumerate(snapshots[1:], 1):
            current = retention.snapshot_states(s)
            for op, state in current.items():
                if op not in baseline:
                    inconsistencies.append(f"检查点{i}: 新增算子「{op}」")
                elif state != baseline.get(op):
                    inconsistencies.append(f"检查点{i}: 算子「{op}」状态变更")

        return {
            "ok": True,
            "config": config_name,
            "snapshot_count": len(snapshots),
            "consistent": len(inconsistencies) == 0,
            "inconsistencies": inconsistencies,
        }

    def list_checkpoints_for_replay(self) -> list[dict[str, Any]]:
        """列出可用于回放引擎消费的检查点。"""
        return [
            {
                "checkpoint_id": item["checkpoint_id"],
                "config": item["config"],
                "operator_count": len({
                    key.split("::", 1)[0] for key in item["state_keys"]
                }),
                "operators": sorted({
                    key.split("::", 1)[0] for key in item["state_keys"]
                }),
                "state_keys": item["state_keys"],
                "state_versions": item["state_versions"],
                "snapshot_ts": item["snapshot_ts"],
                "external_path": item["external_path"],
                "retain_reason": item["retain_reason"],
            }
            for item in self.available_snapshots()
        ]

    # ---- 恢复数据导出 ----

    def coverage_report(self, config_name: str) -> dict[str, Any]:
        """检查可用快照的引用是否都能在状态存储中找到匹配版本。"""
        available = self.available_snapshots(config_name)
        state_service = self._state_service()
        state_index = (
            state_service.state_version_index(self.linked_state_config(config_name))
            if state_service is not None else {}
        )

        snapshot_reports = []
        for item in available:
            missing = []
            mismatched = []
            for key in item["state_keys"]:
                expected = item["state_versions"].get(key)
                versions = set(state_index.get(key, []))
                if not versions:
                    missing.append(key)
                elif expected and expected not in versions:
                    mismatched.append(key)
            snapshot_reports.append({
                "checkpoint_id": item["checkpoint_id"],
                "snapshot_ts": item["snapshot_ts"],
                "state_keys": item["state_keys"],
                "missing_keys": missing,
                "version_mismatched_keys": mismatched,
                "restorable": not missing and not mismatched,
                "retain_reason": item["retain_reason"],
                "exported_versions": sorted({
                    ledger["version"] for ledger in store._checkpoint_exports
                    if item["checkpoint_id"] in ledger["checkpoint_ids"]
                }),
            })

        pruned_ts = sorted(
            retention.snapshot_timestamp(row)
            for row in self._snapshot_rows(config_name)
            if row.get("_pruned")
        )
        interval = self._recoverable_interval(snapshot_reports, pruned_ts)
        return {
            "ok": True,
            "config": config_name,
            "available_snapshots": len(available),
            "interval": interval,
            "contiguous": interval["contiguous"],
            "all_restorable": all(s["restorable"] for s in snapshot_reports)
            if snapshot_reports else True,
            "snapshots": snapshot_reports,
        }

    @staticmethod
    def _recoverable_interval(
        snapshot_reports: list[dict[str, Any]],
        pruned_ts: list[float] | None = None,
    ) -> dict[str, Any]:
        """按快照时间排序，返回连续可恢复区间。

        连续的含义：区间内每个快照的状态键都能在状态存储里找到对应版本，且
        没有已被裁剪的快照夹在两个可用快照之间（那会在时间轴上留下缺口）。
        """
        usable = [s for s in snapshot_reports if s["restorable"]]
        if not snapshot_reports:
            return {
                "checkpoint_ids": [], "start_ts": None, "end_ts": None,
                "contiguous": True, "length": 0, "gap_ts": [],
            }
        start = snapshot_reports[0]
        end = snapshot_reports[-1]
        gaps = sorted(
            ts for ts in (pruned_ts or [])
            if start["snapshot_ts"] < ts < end["snapshot_ts"]
        )
        contiguous = len(usable) == len(snapshot_reports) and not gaps
        return {
            "checkpoint_ids": [s["checkpoint_id"] for s in usable],
            "start_ts": start["snapshot_ts"],
            "end_ts": end["snapshot_ts"],
            "contiguous": contiguous,
            "length": len(usable),
            "gap_ts": gaps,
            "broken_at": None if contiguous else [
                s["checkpoint_id"] for s in snapshot_reports if not s["restorable"]
            ] + [f"pruned@{int(ts)}" for ts in gaps],
        }

    def export_recovery_bundle(
        self,
        config_name: str,
        boundary: str = "checkpoint",
    ) -> dict[str, Any]:
        """生成状态恢复数据文件。

        两种文件边界:
          - ``checkpoint``：按「状态键 + 版本摘要」完整导出所有可用检查点引用的
            版本，文件覆盖整个连续可恢复区间；
          - ``state``：每个状态键只导出去重后的最新版本，文件更小，但只保证最新
            快照可恢复。
        两种边界写出的条目集合不同，文件版本摘要也不同。
        """
        if boundary not in self.BOUNDARIES:
            return {"ok": False, "error": f"未知文件边界「{boundary}」"}
        config = self._config_row(config_name)
        if config is None:
            return {"ok": False, "error": f"检查点配置「{config_name}」不存在"}

        coverage = self.coverage_report(config_name)
        available = self.available_snapshots(config_name)
        if not available:
            return {"ok": False, "error": "检查点配置下没有可用快照，无法生成恢复数据"}

        state_service = self._state_service()
        state_config = self.linked_state_config(config_name)

        entries: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for item in available:
            for key in item["state_keys"]:
                version = item["state_versions"].get(key, "")
                if not version or (key, version) in seen:
                    continue
                seen.add((key, version))
                value = (
                    state_service.state_value_for_version(state_config, key, version)
                    if state_service is not None else None
                )
                if value is None:
                    continue
                entries.append({"state_key": key, "version": version, "value": value})

        if boundary == "state":
            latest: dict[str, dict[str, Any]] = {}
            for item in sorted(available, key=lambda s: s["snapshot_ts"]):
                for key in item["state_keys"]:
                    version = item["state_versions"].get(key, "")
                    for entry in entries:
                        if entry["state_key"] == key and entry["version"] == version:
                            latest[key] = entry
            entries = list(latest.values())

        entries.sort(key=lambda e: (e["state_key"], e["version"]))

        checkpoints = [
            {
                "checkpoint_id": item["checkpoint_id"],
                "snapshot_ts": item["snapshot_ts"],
                "state_keys": item["state_keys"],
                "state_versions": item["state_versions"],
                "external_path": item["external_path"],
            }
            for item in available
        ]
        core = {
            "format": self.RECOVERY_FORMAT,
            "boundary": boundary,
            "config": config_name,
            "state_config": state_config,
            "interval": coverage["interval"],
            "checkpoints": checkpoints,
            "states": entries,
        }
        version = retention.content_digest(core)
        bundle = dict(core, version=version, generated_at=time.time())
        covered = self._covered_checkpoints(bundle)
        ledger_entry = {
            "version": version,
            "config": config_name,
            "state_config": state_config,
            "boundary": boundary,
            "file_name": (
                f"recovery_{retention.file_token(config_name)}_{boundary}_"
                f"{version[:8]}.json"
            ),
            "checkpoint_ids": [c["checkpoint_id"] for c in checkpoints],
            "state_count": len(entries),
            "covers_all_checkpoints": covered["fully_covered"],
            "exported_at": bundle["generated_at"],
        }
        store._checkpoint_exports.append(ledger_entry)
        return {
            "ok": True,
            "config": config_name,
            "boundary": boundary,
            "version": version,
            "file_name": ledger_entry["file_name"],
            "bundle": bundle,
            "coverage": coverage,
            "state_count": len(entries),
            "covers_all_checkpoints": covered["fully_covered"],
        }

    @staticmethod
    def _bundle_entries(bundle: dict[str, Any]) -> list[dict[str, Any]]:
        states = bundle.get("states")
        if not isinstance(states, list):
            return []
        return [
            entry for entry in states
            if isinstance(entry, dict) and entry.get("state_key") and entry.get("version")
        ]

    def _covered_checkpoints(self, bundle: dict[str, Any]) -> dict[str, Any]:
        """统计文件里的条目覆盖了哪些可用检查点。"""
        present = {
            (entry["state_key"], entry["version"])
            for entry in self._bundle_entries(bundle)
        }
        covered = []
        uncovered = []
        for checkpoint in bundle.get("checkpoints") or []:
            cp_id = checkpoint.get("checkpoint_id")
            versions = checkpoint.get("state_versions") or {}
            missing = [
                key for key in checkpoint.get("state_keys", [])
                if (key, versions.get(key, "")) not in present
            ]
            if missing:
                uncovered.append({"checkpoint_id": cp_id, "missing_keys": missing})
            else:
                covered.append(cp_id)
        return {
            "fully_covered": not uncovered,
            "covered_checkpoints": covered,
            "uncovered_checkpoints": uncovered,
        }

    def verify_recovery_bundle(self, bundle: dict[str, Any]) -> dict[str, Any]:
        """校验恢复数据文件：格式、版本摘要与检查点引用覆盖。"""
        issues: list[str] = []
        if not isinstance(bundle, dict):
            return {"valid": False, "issues": ["恢复数据不是 JSON 对象"]}
        if bundle.get("format") != self.RECOVERY_FORMAT:
            issues.append("恢复数据格式标识不匹配")
        boundary = bundle.get("boundary")
        if boundary not in self.BOUNDARIES:
            issues.append(f"未知文件边界「{boundary}」")
        if not isinstance(bundle.get("states"), list):
            issues.append("恢复数据缺少 states 条目列表")
        checkpoints = bundle.get("checkpoints")
        if not isinstance(checkpoints, list):
            issues.append("恢复数据缺少 checkpoints 列表")
            checkpoints = []

        entries = self._bundle_entries(bundle)
        if not entries:
            issues.append("恢复数据不含任何状态条目")
        duplicated = set()
        seen = set()
        for entry in entries:
            token = (entry["state_key"], entry["version"])
            if token in seen:
                duplicated.add(entry["state_key"])
            seen.add(token)
            if retention.digest_value(entry.get("value")) != entry["version"]:
                issues.append(
                    f"状态键「{entry['state_key']}」内容与版本摘要不一致")

        coverage = self._covered_checkpoints(bundle)
        if boundary == "checkpoint" and not coverage["fully_covered"]:
            issues.append("文件未覆盖全部可用检查点的键集合")
        if not checkpoints:
            issues.append("恢复数据没有可恢复的检查点区间")

        core = {
            "format": bundle.get("format"),
            "boundary": boundary,
            "config": bundle.get("config"),
            "state_config": bundle.get("state_config"),
            "interval": bundle.get("interval"),
            "checkpoints": checkpoints,
            "states": bundle.get("states") if isinstance(bundle.get("states"), list) else [],
        }
        expected_version = retention.content_digest(core)
        if bundle.get("version") != expected_version:
            issues.append("恢复数据版本摘要校验失败，文件可能被篡改或截断")

        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "version": bundle.get("version"),
            "expected_version": expected_version,
            "boundary": boundary,
            "checkpoint_count": len(checkpoints),
            "state_count": len(entries),
            "duplicated_state_keys": sorted(duplicated),
            "coverage": coverage,
            "interval": bundle.get("interval"),
        }

    def import_recovery_bundle(
        self,
        bundle: dict[str, Any],
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """幂等导入状态恢复数据。

        同一份文件（版本摘要相同）只入库一次；校验失败时不写入任何状态行。
        """
        validation = self.verify_recovery_bundle(bundle)
        if not validation["valid"]:
            return {
                "ok": False,
                "error": "恢复数据校验失败，已退回原文件",
                "validation": validation,
            }

        version = validation["version"]
        config_name = bundle.get("config", "")
        state_service = self._state_service()
        if state_service is None:
            return {"ok": False, "error": "状态存储服务不可用", "validation": validation}

        if dry_run:
            return {
                "ok": True,
                "dry_run": True,
                "would_import": validation["state_count"],
                "duplicated": state_service.has_import_batch(version),
                "validation": validation,
            }

        result = state_service.import_recovery_states(
            config_name=self.linked_state_config(config_name),
            entries=self._bundle_entries(bundle),
            version=version,
            boundary=bundle.get("boundary", "checkpoint"),
            checkpoints=bundle.get("checkpoints") or [],
        )
        if result.get("error"):
            return {
                "ok": False,
                "error": result["error"],
                "rolled_back": result.get("rolled_back", False),
                "validation": validation,
            }
        return {
            "ok": True,
            "imported": result["imported"],
            "versions_imported": result.get("versions_imported", result["imported"]),
            "duplicated": result["duplicated"],
            "batch_id": result["batch_id"],
            "version": version,
            "config": config_name,
            "covers_all_checkpoints": validation["coverage"]["fully_covered"],
            "validation": validation,
        }

    def state_store_consumes_snapshots(
        self, config_name: str | None = None,
    ) -> dict[str, Any]:
        """状态存储消费链路的可用快照视图：只暴露键集合与版本，不给整份正文。"""
        items = self.available_snapshots(config_name)
        return {
            "count": len(items),
            "checkpoint_ids": [item["checkpoint_id"] for item in items],
            "state_keys": sorted({key for item in items for key in item["state_keys"]}),
            "snapshots": [
                {
                    "checkpoint_id": item["checkpoint_id"],
                    "config": item["config"],
                    "state_keys": item["state_keys"],
                    "state_versions": item["state_versions"],
                    "retain_reason": item["retain_reason"],
                }
                for item in items
            ],
        }

    def export_ledger(self) -> list[dict[str, Any]]:
        """导出台账：其他页面据此核对接收到的文件与本地导出记录一致。"""
        return list(store._checkpoint_exports)

    def _state_service(self):
        from app.services import service_for

        return service_for("state_store")

    # ---- 页面/接口输出 ----

    def retention_report(self, config_name: str) -> dict[str, Any]:
        """状态页面读取的保留原因报告：快照为什么被保留或裁剪。"""
        config = self._config_row(config_name)
        if config is None:
            return {"ok": False, "error": f"检查点配置「{config_name}」不存在"}
        coverage = self.coverage_report(config_name)
        snapshots = []
        for row in sorted(self._snapshot_rows(config_name),
                          key=retention.snapshot_timestamp, reverse=True):
            cp_id = retention.snapshot_id(row)
            snapshots.append({
                "checkpoint_id": cp_id,
                "snapshot_ts": retention.snapshot_timestamp(row),
                "status": row.get("status", ""),
                "available": row in self._available_rows(config_name),
                "retain_reason": (
                    row.get("pruned_reason")
                    or row.get("deferred_reason")
                    or row.get("retain_reason", "保留窗口内最新快照")
                ),
                "state_keys": list(row.get("state_keys") or []),
            })
        state_service = self._state_service()
        state_reasons = (
            state_service.retention_reasons(self.linked_state_config(config_name))
            if state_service is not None else []
        )
        return {
            "ok": True,
            "config": config_name,
            "keep": self._keep_count(config_name),
            "available_snapshots": coverage["available_snapshots"],
            "interval": coverage["interval"],
            "contiguous": coverage["contiguous"],
            "all_restorable": coverage["all_restorable"],
            "referenced_state_keys": self.referenced_state_keys(
                self.linked_state_config(config_name)),
            "snapshots": snapshots,
            "state_retention": state_reasons,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        available = self._available_rows()
        base["total_snapshots"] = sum(1 for r in rows if retention.is_snapshot_row(r))
        base["available_snapshots"] = len(available)
        base["pruned_snapshots"] = sum(1 for r in rows if r.get("_pruned"))
        base["deferred_prunes"] = sum(
            1 for r in rows
            if r.get("_deferred_prune_after_replay") and not r.get("_pruned")
        )
        base["referenced_state_keys"] = len(self.referenced_state_keys())
        base["available_recovery_interval"] = self.coverage_report(
            available[0].get("配置名称", ""))["interval"] if available else None
        return base
