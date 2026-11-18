"""保留边界共享工具：统一状态条目、检查点快照和恢复文件的键/版本表示。

检查点快照里的 ``operator_states`` 是一棵「算子 → 状态字段」的字典树；状态存储
里的行既可能是配置行，也可能是回放/导入产生的状态条目。压缩、恢复和导出都必
须用同一套「状态键 + 版本摘要」语言描述两边，否则检查点引用的旧版本会在按更
新时间排序时被误停用。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

# 一行状态数据至少带有下面任一标记，才会被当作「状态条目」参与压缩/保留判断。
_STATE_ENTRY_MARKERS = (
    "_state_key",
    "_state_keys",
    "_state_value",
    "_state_values",
    "_state_versions",
    "_replay_state",
    "_imported_states",
    "_imported_from",
    "_state_config",
)

_SNAPSHOT_STATE_FIELDS = ("operator_states", "state")


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def digest_value(value: Any) -> str:
    """状态字段值的稳定摘要，用于判断两个状态版本是否同一份数据。"""
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()[:16]


def file_token(name: str) -> str:
    """文件名安全令牌：中文配置名转成稳定 ASCII，避免 Content-Disposition 非 latin-1。"""
    cleaned = "".join(ch for ch in name if ch.isascii() and (ch.isalnum() or ch in "-_."))
    if cleaned == name and cleaned:
        return cleaned
    return f"{cleaned or 'cfg'}_{digest_value(name)[:8]}"


def content_digest(payload: Any) -> str:
    """整份文件/载荷的摘要，用于导入幂等与导出台账核对。"""
    return hashlib.sha256(stable_json(payload).encode("utf-8")).hexdigest()


# ---- 检查点快照 ----


def snapshot_states(snapshot: dict[str, Any]) -> dict[str, Any]:
    for field in _SNAPSHOT_STATE_FIELDS:
        value = snapshot.get(field)
        if isinstance(value, dict):
            return value
    return {}


def snapshot_state_values(states: dict[str, Any]) -> dict[str, Any]:
    """把算子状态字典树摊平成 ``算子::字段`` → 值。"""
    values: dict[str, Any] = {}
    if not isinstance(states, dict):
        return values
    for operator, operator_state in states.items():
        if isinstance(operator_state, dict):
            if not operator_state:
                values[f"{operator}::__empty__"] = {}
                continue
            for field, value in operator_state.items():
                values[f"{operator}::{field}"] = value
        else:
            values[f"{operator}::__scalar__"] = operator_state
    return values


def snapshot_state_versions(states: dict[str, Any]) -> dict[str, str]:
    return {key: digest_value(value) for key, value in snapshot_state_values(states).items()}


def snapshot_state_keys(states: dict[str, Any]) -> list[str]:
    return sorted(snapshot_state_values(states).keys())


def snapshot_timestamp(row: dict[str, Any]) -> float:
    for field in ("snapshot_ts", "timestamp", "_created_at", "_timestamp"):
        value = row.get(field)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                continue
    return 0.0


def is_snapshot_row(row: dict[str, Any]) -> bool:
    """检查点快照行：服务创建的带 checkpoint_id，旧管道创建的 id 为摘要串。"""
    if row.get("checkpoint_id"):
        return True
    legacy_id = row.get("id")
    return isinstance(legacy_id, str) and (
        "state" in row or "operator_states" in row
    )


def snapshot_id(row: dict[str, Any]) -> str:
    return str(row.get("checkpoint_id") or row.get("id") or "")


# ---- 状态存储行 ----


def is_state_entry(row: dict[str, Any]) -> bool:
    if any(marker in row for marker in _STATE_ENTRY_MARKERS):
        return True
    pattern = str(row.get("状态键模式", ""))
    return pattern.startswith("replay://") or pattern.startswith("import://")


def row_state_values(row: dict[str, Any]) -> dict[str, Any]:
    """从状态存储行还原 ``状态键`` → 值。"""
    # 显式导入的状态集合
    imported = row.get("_imported_states")
    if isinstance(imported, dict) and imported:
        return {str(key): value for key, value in imported.items()}

    # 显式单键状态
    key = row.get("_state_key")
    if key:
        value = row.get("_state_value")
        if value is None and isinstance(row.get("_state_values"), dict):
            value = row["_state_values"].get(key)
        return {str(key): value}

    values = row.get("_state_values")
    keys = row.get("_state_keys")
    if isinstance(values, dict) and values:
        return {str(k): v for k, v in values.items()}
    if isinstance(keys, list) and keys:
        return {
            str(k): (values.get(k) if isinstance(values, dict) else row.get(f"_state_{k}"))
            for k in keys
        }

    # 回放引擎写入的整算子状态
    replay_state = row.get("_replay_state")
    if isinstance(replay_state, dict):
        operator = row.get("_replay_operator") or _operator_from_pattern(
            str(row.get("状态键模式", ""))
        )
        if operator:
            return {
                f"{operator}::{field}": value
                for field, value in replay_state.items()
            }

    return {}


def row_state_versions(row: dict[str, Any]) -> dict[str, str]:
    explicit = row.get("_state_versions")
    if isinstance(explicit, dict) and explicit:
        return {str(k): str(v) for k, v in explicit.items()}
    return {key: digest_value(value) for key, value in row_state_values(row).items()}


def row_state_keys(row: dict[str, Any]) -> list[str]:
    explicit = row.get("_state_key")
    if explicit:
        return [str(explicit)]
    keys = row.get("_state_keys")
    if isinstance(keys, list) and keys:
        return [str(k) for k in keys]
    return sorted(row_state_versions(row).keys())


def row_compaction_key(row: dict[str, Any]) -> str:
    """压缩分组键：单键状态按状态键，多键状态按整组键序，保证同一份状态一起比较。"""
    explicit = row.get("_state_key")
    if explicit:
        return str(explicit)
    versions = row_state_versions(row)
    if versions:
        return "|".join(sorted(versions))
    pattern = str(row.get("状态键模式", ""))
    if pattern:
        return f"pattern::{pattern}"
    return f"row::{row.get('id', '')}"


def row_updated_at(row: dict[str, Any]) -> float:
    for field in ("_timestamp", "_created_at", "timestamp", "updated_at"):
        value = row.get(field)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                continue
    return 0.0


def _operator_from_pattern(pattern: str) -> str:
    if "://" not in pattern:
        return ""
    tail = pattern.split("://", 1)[1]
    # 回放写入的模式是 replay://算子名，导入是 import://状态键；去掉任务后缀
    return tail.split("_replay_", 1)[0]
