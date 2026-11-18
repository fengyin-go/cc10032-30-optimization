"""状态存储领域服务：管理流式任务的有状态存储配置。

数据流角色: calc_operator → state_store（消费者+生产者）→ 下游多模块
增强能力: 状态TTL清理、增量/全量模式切换、状态键去重、状态查询、跨模块状态索引、
          检查点保留窗口感知的压缩、恢复数据幂等导入。

压缩约定:
  状态行先按「状态键 + 版本摘要」归组，再按更新时间倒序排列。仅保留：
    - 每组最新版本（在线状态）；
    - 任何可用检查点快照引用的版本（恢复必需）。
  其余版本才允许标成已停用；因此压缩不会再提前停用恢复所需状态。
"""
from __future__ import annotations

import copy
import time
from typing import Any

from app.services import retention
from app.services.base import BaseModuleService
from app.store import store


class StateStoreService(BaseModuleService):
    MODULE = "state_store"
    ENTITY = "状态配置"
    LIST_FIELDS = ["配置名称", "后端类型", "状态键模式", "TTL时长", "检查点间隔", "增量模式", "清理策略"]
    REQUIRED_FIELDS = ["配置名称", "后端类型", "状态键模式"]
    STATUS_ORDER = ["草稿", "已生效", "已停用"]
    ACTION_RULES = {"启用配置": "草稿", "停用配置": "已生效"}
    NEGATIVE_ACTIONS = ["停用配置"]

    # ---- 通用列表：内部状态条目不混进配置清单 ----

    def list_entries(self, **kwargs: Any):
        items, total = super().list_entries(**kwargs)
        visible = [row for row in items if not retention.is_state_entry(row)]
        return visible, total - (len(items) - len(visible))

    # ---- 跨模块数据管理 ----

    def _config_row(self, config_name: str) -> dict[str, Any] | None:
        for r in store.rows(self.MODULE):
            if r.get("配置名称") == config_name and not retention.is_state_entry(r):
                return r
        return None

    def _state_rows(self, config_name: str | None = None) -> list[dict[str, Any]]:
        rows = [r for r in store.rows(self.MODULE) if retention.is_state_entry(r)]
        if config_name:
            rows = [
                r for r in rows
                if r.get("_state_config") == config_name
                or r.get("_imported_from") == config_name
            ]
        return rows

    def _active_state_rows(self, config_name: str | None = None) -> list[dict[str, Any]]:
        return [
            r for r in self._state_rows(config_name)
            if r.get("status") == "已生效"
            and not r.get("_compacted")
            and not r.get("_cleaned")
            and not r.get("_rolled_back")
        ]

    # ---- 状态版本写入 ----

    def record_state_version(
        self,
        config_name: str,
        operator: str,
        state: dict[str, Any],
        *,
        checkpoint_ref: str | None = None,
        source: str = "runtime",
    ) -> dict[str, Any]:
        """写入一份算子状态版本，返回行号与版本摘要。"""
        config = self._config_row(config_name)
        if config is None:
            return {"ok": False, "error": f"状态配置「{config_name}」不存在"}

        values = {
            f"{operator}::{field}": value
            for field, value in state.items()
        } if isinstance(state, dict) else {f"{operator}::__scalar__": state}
        row = {
            "id": self._next_state_id(),
            "配置名称": f"{config_name}::{operator}",
            "后端类型": config.get("后端类型", "memory"),
            "状态键模式": f"{config.get('状态键模式', 'state')}://{operator}",
            "TTL时长": config.get("TTL时长", "3600"),
            "检查点间隔": config.get("检查点间隔", "30"),
            "增量模式": config.get("增量模式", "incremental"),
            "清理策略": config.get("清理策略", "ttl"),
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "_state_config": config_name,
            "_state_source": source,
            "_state_keys": sorted(values.keys()),
            "_state_values": copy.deepcopy(values),
            "_state_versions": {
                key: retention.digest_value(value) for key, value in values.items()
            },
            "_operator": operator,
            "_timestamp": time.time(),
            "_created_at": time.time(),
        }
        if checkpoint_ref:
            row["_checkpoint_ref"] = checkpoint_ref
        store.rows(self.MODULE).append(row)
        return {
            "ok": True,
            "state_row_id": row["id"],
            "state_keys": row["_state_keys"],
            "state_versions": row["_state_versions"],
        }

    def _next_state_id(self) -> int:
        return max(
            (int(r.get("id", 0)) for r in store.rows(self.MODULE)
             if isinstance(r.get("id"), (int, float)) or str(r.get("id", "")).isdigit()),
            default=0,
        ) + 1

    # ---- 压缩 ----

    def compact_duplicate_states(self, config_name: str) -> dict[str, Any]:
        """压缩重复状态：按状态键去重，保留最新版本及检查点引用的版本。"""
        config = self._config_row(config_name)
        if config is None:
            return {"ok": False, "error": f"状态配置「{config_name}」不存在"}

        guard = self._checkpoint_guard(config_name)
        referenced_versions = {
            key: set(info.get("versions", [])) for key, info in guard.items()
        }

        states_grouped: dict[str, list[dict[str, Any]]] = {}
        for row in self._state_rows(config_name):
            if row.get("_compacted") or row.get("_cleaned") or row.get("_rolled_back"):
                continue
            states_grouped.setdefault(
                retention.row_compaction_key(row), []).append(row)

        compacted = 0
        retained_for_checkpoints = 0
        details = []
        for group, entries in states_grouped.items():
            entries.sort(key=retention.row_updated_at, reverse=True)
            keep_ids = set()
            for index, entry in enumerate(entries):
                versions = retention.row_state_versions(entry)
                if index == 0:
                    # 最新版本始终保留，保证在线状态可读
                    keep_ids.add(entry.get("id"))
                    continue
                if self._row_referenced_by_checkpoint(entry, referenced_versions):
                    keep_ids.add(entry.get("id"))
                    retained_for_checkpoints += 1
                    continue
                entry["status"] = "已停用"
                entry["_compacted"] = True
                entry["_compacted_at"] = time.time()
                entry["compacted_reason"] = "同键旧版本，未被任何可用检查点引用"
                compacted += 1
            details.append({
                "group": group,
                "versions": len(entries),
                "retained": len(keep_ids),
            })

        return {
            "ok": True,
            "config": config_name,
            "groups_found": len(states_grouped),
            "compacted": compacted,
            "retained_for_checkpoints": retained_for_checkpoints,
            "remaining_active": len(self._active_state_rows(config_name)),
            "referenced_state_keys": len(referenced_versions),
            "details": details,
        }

    @staticmethod
    def _row_referenced_by_checkpoint(
        row: dict[str, Any], referenced_versions: dict[str, set[str]],
    ) -> bool:
        """行里只要有任一状态键被可用快照引用，整行就不能压缩/清理。

        状态行是多键打包写入的，删除整行会连带丢掉被引用的那个键。
        """
        versions = retention.row_state_versions(row)
        if not versions:
            return False
        return any(
            version in referenced_versions.get(key, set())
            for key, version in versions.items()
        )

    def _checkpoint_guard(self, config_name: str | None = None):
        from app.services import service_for

        checkpoint_service = service_for("checkpoint_mgr")
        if checkpoint_service is None:
            return {}
        return checkpoint_service.state_retention_guard(config_name)

    def referenced_keys(self, config_name: str | None = None) -> dict[str, list[str]]:
        guard = self._checkpoint_guard(config_name)
        return {key: sorted(info["versions"]) for key, info in guard.items()}

    def state_version_index(self, config_name: str | None = None) -> dict[str, list[str]]:
        """活跃状态版本索引：``状态键`` → 可用版本摘要列表。"""
        index: dict[str, list[str]] = {}
        for row in self._active_state_rows(config_name):
            for key, version in retention.row_state_versions(row).items():
                index.setdefault(key, [])
                if version not in index[key]:
                    index[key].append(version)
        return index

    def state_value_for_version(
        self, config_name: str, key: str, version: str,
    ) -> Any:
        """按检查点引用的版本摘要精确取状态值，取不到返回 None。"""
        for row in self._active_state_rows(config_name):
            versions = retention.row_state_versions(row)
            if versions.get(key) != version:
                continue
            values = retention.row_state_values(row)
            if key in values:
                return copy.deepcopy(values[key])
        return None

    def state_value(self, config_name: str, key: str) -> Any:
        """取状态键的值：优先匹配检查点引用的版本，否则取最新活跃版本。"""
        guard = self._checkpoint_guard(config_name).get(key, {})
        wanted = set(guard.get("versions", []))
        rows = [
            row for row in self._active_state_rows(config_name)
            if key in retention.row_state_values(row)
        ]
        if not rows:
            return None
        rows.sort(key=retention.row_updated_at, reverse=True)
        for row in rows:
            version = retention.row_state_versions(row).get(key)
            if version and version in wanted:
                return copy.deepcopy(retention.row_state_values(row)[key])
        return copy.deepcopy(retention.row_state_values(rows[0])[key])

    # ---- TTL 清理 ----

    def apply_ttl_cleanup(self, config_name: str) -> dict[str, Any]:
        """根据TTL策略清理过期状态条目；检查点引用的版本不参与清理。"""
        config = self._config_row(config_name)
        if config is None:
            return {"ok": False, "error": f"状态配置「{config_name}」不存在"}

        try:
            ttl = int(config.get("TTL时长", "3600"))
        except (ValueError, TypeError):
            ttl = 3600

        guard = self._checkpoint_guard(config_name)
        referenced_versions = {
            key: set(info.get("versions", [])) for key, info in guard.items()
        }

        now = time.time()
        cleaned = 0
        protected = 0
        for r in self._state_rows(config_name):
            if r.get("_compacted") or r.get("_cleaned"):
                continue
            ts = r.get("_timestamp", 0)
            if not isinstance(ts, (int, float)) or now - ts <= ttl:
                continue
            if self._row_referenced_by_checkpoint(r, referenced_versions):
                protected += 1
                continue
            r["status"] = "已停用"
            r["_cleaned"] = True
            r["_cleaned_at"] = now
            cleaned += 1

        return {
            "ok": True,
            "config": config_name,
            "ttl": ttl,
            "cleaned": cleaned,
            "protected_by_checkpoints": protected,
            "timestamp": now,
        }

    # ---- 恢复数据导入（幂等 + 失败回退） ----

    def has_import_batch(self, version: str) -> bool:
        return any(
            row.get("_import_version") == version and not row.get("_rolled_back")
            for row in store.rows(self.MODULE)
        )

    def import_recovery_states(
        self,
        config_name: str,
        entries: list[dict[str, Any]],
        version: str,
        boundary: str = "checkpoint",
        checkpoints: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """幂等写入一份恢复数据：同版本文件只入库一次，失败整体回退。"""
        if self.has_import_batch(version):
            existing = [
                row for row in store.rows(self.MODULE)
                if row.get("_import_version") == version and not row.get("_rolled_back")
            ]
            return {
                "imported": 0,
                "duplicated": True,
                "batch_id": existing[0].get("_import_batch") if existing else None,
                "state_row_ids": [row.get("id") for row in existing],
            }

        user_config = self._config_row(config_name)
        target_name = config_name
        if user_config is None:
            # 配置不在本环境时，落到一个以版本命名的独立配置上，保持文件可追溯
            target_name = f"恢复导入_{config_name or version[:8]}"
            user_config = {
                "配置名称": target_name,
                "后端类型": "memory",
                "状态键模式": f"import://{version[:8]}",
                "TTL时长": "3600",
                "检查点间隔": "30",
                "增量模式": "full",
                "清理策略": "after_replay",
            }

        batch_id = f"imp_{version[:12]}"
        rows_before = len(store.rows(self.MODULE))
        imported_rows: list[dict[str, Any]] = []
        try:
            if not entries:
                raise ValueError("恢复数据中没有状态条目")
            if not any(
                not retention.is_state_entry(r) and r.get("配置名称") == target_name
                for r in store.rows(self.MODULE)
            ):
                store.rows(self.MODULE).append({
                    "id": self._next_state_id(),
                    "配置名称": target_name,
                    "后端类型": user_config.get("后端类型", "memory"),
                    "状态键模式": user_config.get("状态键模式", "import"),
                    "TTL时长": user_config.get("TTL时长", "3600"),
                    "检查点间隔": user_config.get("检查点间隔", "30"),
                    "增量模式": user_config.get("增量模式", "full"),
                    "清理策略": user_config.get("清理策略", "after_replay"),
                    "status": "已生效",
                    "pending": False,
                    "abnormal": False,
                    "_created_at": time.time(),
                })

            row = {
                "id": self._next_state_id(),
                "配置名称": f"{target_name}::恢复导入",
                "后端类型": user_config.get("后端类型", "memory"),
                "状态键模式": f"import://{target_name}",
                "TTL时长": user_config.get("TTL时长", "3600"),
                "检查点间隔": user_config.get("检查点间隔", "30"),
                "增量模式": "full",
                "清理策略": "after_replay",
                "status": "已生效",
                "pending": False,
                "abnormal": False,
                "_state_config": target_name,
                "_state_source": "import",
                "_state_keys": sorted({e["state_key"] for e in entries}),
                "_imported_entries": copy.deepcopy(entries),
                "_state_versions": {
                    e["state_key"]: e["version"] for e in entries
                },
                "_state_values": {
                    e["state_key"]: copy.deepcopy(e["value"]) for e in entries
                },
                "_imported_from": config_name,
                "_import_version": version,
                "_import_batch": batch_id,
                "_import_boundary": boundary,
                "_import_checkpoints": [
                    c.get("checkpoint_id") for c in (checkpoints or [])
                ],
                "_timestamp": time.time(),
                "_created_at": time.time(),
            }
            store.rows(self.MODULE).append(row)
            imported_rows.append(row)
        except Exception as exc:  # 事务失败：退回原文件，不留下半份导入
            for imported in imported_rows:
                imported["_rolled_back"] = True
                imported["status"] = "已停用"
            del store.rows(self.MODULE)[rows_before:]
            return {
                "imported": 0,
                "duplicated": False,
                "batch_id": None,
                "error": f"导入失败，已退回原文件：{exc}",
                "rolled_back": True,
            }

        return {
            "imported": len({e["state_key"] for e in entries}),
            "versions_imported": len(entries),
            "duplicated": False,
            "batch_id": batch_id,
            "state_row_ids": [r.get("id") for r in imported_rows],
            "target_config": target_name,
        }

    # ---- 查询 ----

    def query_state_by_operator(
        self, operator_name: str, group_key: str | None = None,
    ) -> dict[str, Any]:
        """按算子名称查询关联状态，支持分组过滤。"""
        matches = []
        for r in store.rows(self.MODULE):
            if r.get("_operator") == operator_name:
                if group_key and r.get("_group") != group_key:
                    continue
                matches.append({
                    "state_id": r.get("id"),
                    "config": r.get("配置名称", ""),
                    "operator": r.get("_operator", ""),
                    "group": r.get("_group", ""),
                    "aggregates": r.get("_aggregates", {}),
                    "timestamp": r.get("_timestamp", 0),
                    "status": r.get("status", ""),
                })
        return {
            "ok": True,
            "operator": operator_name,
            "group_filter": group_key,
            "count": len(matches),
            "states": matches,
        }

    def list_active_state_keys(self) -> list[dict[str, Any]]:
        """列出所有活跃的状态键及其数据量。"""
        key_stats: dict[str, dict[str, Any]] = {}
        for r in self._active_state_rows():
            for key in retention.row_state_keys(r):
                entry = key_stats.setdefault(
                    key, {"pattern": key, "count": 0, "latest_ts": 0})
                entry["count"] += 1
                ts = retention.row_updated_at(r)
                if ts > entry["latest_ts"]:
                    entry["latest_ts"] = ts
        return sorted(key_stats.values(), key=lambda x: x["count"], reverse=True)

    def retention_reasons(self, config_name: str) -> list[dict[str, Any]]:
        """状态页面展示的保留原因：每行状态为什么保留/被压缩。"""
        guard = self._checkpoint_guard(config_name)
        result = []
        for row in self._state_rows(config_name):
            versions = retention.row_state_versions(row)
            referenced_by = sorted({
                cp_id
                for key, version in versions.items()
                for cp_id in guard.get(key, {}).get("checkpoint_ids", [])
                if version in guard.get(key, {}).get("versions", set())
            })
            if row.get("_compacted"):
                reason = row.get("compacted_reason", "已压缩的旧版本")
            elif row.get("_cleaned"):
                reason = "TTL 清理，且未被可用检查点引用"
            elif referenced_by:
                reason = f"被可用检查点引用：{', '.join(referenced_by)}"
            elif row.get("_state_source") == "import":
                reason = f"恢复数据导入（{row.get('_import_version', '')[:8]}）"
            else:
                reason = "最新在线版本"
            result.append({
                "state_row_id": row.get("id"),
                "config": row.get("_state_config", ""),
                "operator": row.get("_operator", ""),
                "state_keys": sorted(versions.keys()),
                "state_versions": versions,
                "status": row.get("status", ""),
                "source": row.get("_state_source", ""),
                "retain_reason": reason,
                "referenced_by": referenced_by,
            })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active_states"] = sum(1 for r in rows if r.get("status") == "已生效")
        base["compacted_states"] = sum(1 for r in rows if r.get("_compacted"))
        base["cleaned_states"] = sum(1 for r in rows if r.get("_cleaned"))
        base["state_entries"] = len(self._state_rows())
        base["imported_batches"] = len({
            r.get("_import_version") for r in rows
            if r.get("_import_version") and not r.get("_rolled_back")
        })
        base["unique_key_patterns"] = len(set(
            r.get("状态键模式", "") for r in rows
        ))
        return base
