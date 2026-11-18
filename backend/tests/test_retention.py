"""保留边界测试：检查点引用、状态压缩、回放可用快照与恢复数据导入。"""
from __future__ import annotations

import copy
import time

import pytest

from app.services import retention, service_for
from app.store import store


@pytest.fixture()
def clean_store():
    """每个用例在干净的种子数据上跑，并移除演示用的历史快照/状态版本。"""
    saved = copy.deepcopy(store._tables)
    try:
        store._tables["checkpoint_mgr"] = [
            row for row in store._tables.get("checkpoint_mgr", [])
            if row.get("checkpoint_id") not in {"cp_demo_001", "cp_demo_002", "cp_demo_000"}
        ]
        store._tables["state_store"] = [
            row for row in store._tables.get("state_store", [])
            if row.get("_state_source") is None
        ]
        yield
    finally:
        store._tables.clear()
        store._tables.update(copy.deepcopy(saved))


def _checkpoint():
    return service_for("checkpoint_mgr")


def _state():
    return service_for("state_store")


CHECKPOINT_CONFIG = "检查点样例2"
STATE_CONFIG = "状态存储样例2"


def _enable_config(name: str = CHECKPOINT_CONFIG) -> dict:
    for row in store.rows("checkpoint_mgr"):
        if row.get("配置名称") == name and not retention.is_snapshot_row(row):
            row["status"] = "已启用"
            row["保留数量"] = 2
            return row
    raise AssertionError("seed config missing")


def _build_versions(config_name: str, operator: str, values: list[int]) -> list[dict]:
    rows = []
    for offset, value in enumerate(values):
        rows.append(_state().record_state_version(
            config_name, operator, {"offset": value, "window": {"count": value * 2}}))
        time.sleep(0.002)
    return rows


class TestCompactionRespectsCheckpoints:
    def test_checkpoint_referenced_version_survives_compaction(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)
        _build_versions(STATE_CONFIG, "agg_op", [1, 2, 3])

        # 第二个版本仍被检查点引用（模拟快照落在旧版本上）
        older_row = [r for r in store.rows("state_store")
                     if r.get("_state_config") == STATE_CONFIG][1]
        older_states = {
            "agg_op": {
                key.split("::", 1)[1]: value
                for key, value in retention.row_state_values(older_row).items()
            }
        }
        created = _checkpoint().create_checkpoint(
            name, older_states, state_row_ids=[older_row["id"]])
        assert created["ok"]

        result = _state().compact_duplicate_states(STATE_CONFIG)
        assert result["ok"]
        assert result["retained_for_checkpoints"] >= 1
        assert older_row["status"] == "已生效"
        assert older_row.get("_compacted") is not True

        # 最旧且未被任何检查点引用的版本才被压缩
        newest_row = [r for r in store.rows("state_store")
                      if r.get("_state_config") == STATE_CONFIG][-1]
        assert newest_row["status"] == "已生效"

    def test_ttl_cleanup_protects_checkpoint_versions(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)
        state_config = next(
            r for r in store.rows("state_store")
            if r.get("配置名称") == STATE_CONFIG)
        state_config["TTL时长"] = "0"
        _build_versions(STATE_CONFIG, "agg_op", [1, 2])
        referenced = [r for r in store.rows("state_store")
                      if r.get("_state_config") == STATE_CONFIG][0]
        _checkpoint().create_checkpoint(
            name,
            {"agg_op": {"offset": 1, "window": {"count": 2}}},
            state_row_ids=[referenced["id"]],
        )

        time.sleep(0.01)
        result = _state().apply_ttl_cleanup(STATE_CONFIG)
        assert result["cleaned"] >= 1
        assert result["protected_by_checkpoints"] >= 1
        assert referenced["status"] == "已生效"


class TestRetentionWindow:
    def test_snapshot_prune_keeps_states_referenced_by_remaining_snapshots(
        self, clean_store,
    ):
        name = CHECKPOINT_CONFIG
        _enable_config(name)["保留数量"] = 5
        _build_versions(STATE_CONFIG, "agg_op", [1])
        first = _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 1}})
        time.sleep(0.005)
        _build_versions(STATE_CONFIG, "agg_op", [2])
        second = _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 2}})
        assert first["ok"] and second["ok"]

        state_rows = [r for r in store.rows("state_store")
                      if r.get("_state_config") == STATE_CONFIG]
        # 两个版本分别被两个快照引用，窗口未缩短前都应在启用状态
        assert {r["status"] for r in state_rows} == {"已生效"}

        result = _checkpoint().apply_retention_window(name, keep=1, mode="immediate")
        assert result["ok"]
        assert result["pruned"] == [first["checkpoint_id"]]

        # 剩余可用快照引用的版本必须保持启用，被释放的旧版本允许压缩
        remaining = _checkpoint().available_snapshots(name)[0]
        guard = _checkpoint().state_retention_guard(STATE_CONFIG)
        for key, version in remaining["state_versions"].items():
            assert version in guard[key]["versions"]
            matching = [
                r for r in state_rows
                if retention.row_state_versions(r).get(key) == version
                and not r.get("_compacted")
            ]
            assert matching, f"{key} 的剩余快照版本被误压缩"
        assert all(r["status"] == "已生效" for r in matching)

    def test_immediate_and_deferred_converge(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)["保留数量"] = 5
        created = []
        for value in range(4):
            created.append(_checkpoint().create_checkpoint(
                name, {"agg_op": {"offset": value}}))
            time.sleep(0.004)

        immediate = _checkpoint().apply_retention_window(name, keep=1, mode="immediate")
        assert immediate["available_after"] == 1
        immediate_ids = [
            item["checkpoint_id"]
            for item in _checkpoint().available_snapshots(name)
        ]

        # 复原所有快照，再用延迟模式走一遍
        for row in store.rows("checkpoint_mgr"):
            if retention.is_snapshot_row(row):
                row.pop("_pruned", None)
                row.pop("_deferred_prune_after_replay", None)
                row["status"] = "已启用"

        deferred = _checkpoint().apply_retention_window(name, keep=1, mode="deferred")
        assert deferred["pruned"] == []
        assert len(deferred["deferred"]) == 3
        assert len(_checkpoint().available_snapshots(name)) == 4

        done = _checkpoint().complete_deferred_retention(name, 1, replay_ok=True)
        assert done["ok"]
        deferred_ids = [
            item["checkpoint_id"]
            for item in _checkpoint().available_snapshots(name)
        ]
        assert deferred_ids == immediate_ids

    def test_failed_replay_keeps_deferred_snapshots(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)["保留数量"] = 5
        for value in range(3):
            _checkpoint().create_checkpoint(name, {"agg_op": {"offset": value}})
            time.sleep(0.004)
        _checkpoint().apply_retention_window(name, keep=1, mode="deferred")
        result = _checkpoint().complete_deferred_retention(name, 9, replay_ok=False)
        assert result["ok"] is False
        assert len(_checkpoint().available_snapshots(name)) == 3


class TestReplayBoundaries:
    def test_replay_rejects_pruned_snapshot(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)
        first = _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 1}})
        time.sleep(0.005)
        _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 2}})
        _checkpoint().apply_retention_window(name, keep=1, mode="immediate")

        replay = service_for("replay_engine")
        result = replay.create_replay_job(
            "回放裁剪快照", first["checkpoint_id"], "任务拓扑样例1")
        assert result["ok"] is False
        assert "可用快照集合" in result["error"]

    def test_replay_uses_matching_state_versions(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)
        _build_versions(STATE_CONFIG, "agg_op", [5, 6])
        snapshot_row = [r for r in store.rows("state_store")
                        if r.get("_state_config") == STATE_CONFIG][0]
        created = _checkpoint().create_checkpoint(
            name,
            {"agg_op": {"offset": 5, "window": {"count": 10}}},
            state_row_ids=[snapshot_row["id"]],
        )
        replay = service_for("replay_engine")
        job = replay.create_replay_job(
            "回放旧版本", created["checkpoint_id"], "任务拓扑样例1")
        assert job["ok"]
        executed = replay.execute_replay(job["task_id"])
        assert executed["ok"]
        assert executed["verification"]["all_passed"]
        assert executed["verification"]["consistency"]["consistent"] is True

        restored = [
            r for r in store.rows("state_store")
            if r.get("_replay_task_id") == job["task_id"]
        ]
        assert restored and restored[0]["_replay_state"]["offset"] == 5

    def test_replay_history_marks_availability(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)
        first = _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 1}})
        time.sleep(0.005)
        _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 2}})
        replay = service_for("replay_engine")
        job = replay.create_replay_job(
            "历史回放", first["checkpoint_id"], "任务拓扑样例1")
        assert job["ok"]
        replay.execute_replay(job["task_id"])
        _checkpoint().apply_retention_window(name, keep=1, mode="immediate")

        history = replay.list_replay_history()
        entry = next(item for item in history if item["task_id"] == job["task_id"])
        assert entry["source_available"] is False
        assert entry["restorable"] is True


class TestRecoveryBundle:
    def _prepared(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)
        _build_versions(STATE_CONFIG, "agg_op", [10, 11])
        _checkpoint().create_checkpoint(
            name, {"agg_op": {"offset": 10, "window": {"count": 20}}})
        time.sleep(0.005)
        _checkpoint().create_checkpoint(
            name, {"agg_op": {"offset": 11, "window": {"count": 22}}})
        return name

    def test_two_boundaries_produce_different_files(self, clean_store):
        name = self._prepared(clean_store)
        checkpoint_file = _checkpoint().export_recovery_bundle(name, "checkpoint")
        state_file = _checkpoint().export_recovery_bundle(name, "state")
        assert checkpoint_file["ok"] and state_file["ok"]
        assert checkpoint_file["version"] != state_file["version"]
        assert checkpoint_file["file_name"] != state_file["file_name"]
        assert _checkpoint().verify_recovery_bundle(
            checkpoint_file["bundle"])["valid"] is True
        assert _checkpoint().verify_recovery_bundle(
            state_file["bundle"])["valid"] is True

    def test_export_covers_every_available_checkpoint(self, clean_store):
        name = self._prepared(clean_store)
        exported = _checkpoint().export_recovery_bundle(name, "checkpoint")
        bundle = exported["bundle"]
        available_ids = [
            item["checkpoint_id"]
            for item in _checkpoint().available_snapshots(name)
        ]
        assert [c["checkpoint_id"] for c in bundle["checkpoints"]] == available_ids
        exported_tokens = {
            (entry["state_key"], entry["version"]) for entry in bundle["states"]
        }
        for checkpoint_item in bundle["checkpoints"]:
            for key in checkpoint_item["state_keys"]:
                version = checkpoint_item["state_versions"][key]
                assert (key, version) in exported_tokens
        coverage = exported["coverage"]
        assert coverage["contiguous"] is True
        assert coverage["interval"]["length"] == len(available_ids)

    def test_import_is_idempotent(self, clean_store):
        name = self._prepared(clean_store)
        bundle = _checkpoint().export_recovery_bundle(name, "state")["bundle"]
        first = _checkpoint().import_recovery_bundle(bundle)
        assert first["ok"] and first["duplicated"] is False
        second = _checkpoint().import_recovery_bundle(bundle)
        assert second["ok"] and second["duplicated"] is True
        assert second["imported"] == 0

        imported_rows = [
            r for r in store.rows("state_store")
            if r.get("_import_version") == first["version"]
        ]
        assert len(imported_rows) == 1
        assert len(imported_rows[0]["_imported_entries"]) == first["versions_imported"]

    def test_tampered_bundle_is_rejected_without_writes(self, clean_store):
        name = self._prepared(clean_store)
        bundle = _checkpoint().export_recovery_bundle(name, "checkpoint")["bundle"]
        tampered = copy.deepcopy(bundle)
        tampered["states"][0]["value"] = 999
        before = len(store.rows("state_store"))
        result = _checkpoint().import_recovery_bundle(tampered)
        assert result["ok"] is False
        assert len(store.rows("state_store")) == before
        assert any("版本" in issue for issue in result["validation"]["issues"])

    def test_export_declares_which_checkpoints_are_restorable(self, clean_store):
        name = self._prepared(clean_store)
        report = _checkpoint().retention_report(name)
        assert report["ok"]
        assert report["all_restorable"] is True
        assert report["contiguous"] is True
        assert report["referenced_state_keys"]
        assert all(item["retain_reason"] for item in report["snapshots"])

        state_report = _state().retention_reasons(STATE_CONFIG)
        assert state_report
        assert any("检查点引用" in item["retain_reason"] for item in state_report)


class TestReplayCoverageAndLedger:
    def _prepare(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)
        _build_versions(STATE_CONFIG, "agg_op", [21, 22])
        ids = []
        for value in (21, 22):
            created = _checkpoint().create_checkpoint(
                name, {"agg_op": {"offset": value}})
            ids.append(created["checkpoint_id"])
            time.sleep(0.004)
        return name, ids

    def test_replay_coverage_reports_untried_snapshots(self, clean_store):
        name, ids = self._prepare(clean_store)
        replay = service_for("replay_engine")
        before = replay.restore_coverage(name)
        assert before["all_available_restorable"] is False
        assert set(before["missing_checkpoints"]) == set(ids)

        job = replay.create_replay_job("覆盖演练", ids[-1], "任务拓扑样例1")
        replay.execute_replay(job["task_id"])
        after = replay.restore_coverage(name)
        restored = {item["checkpoint_id"] for item in after["items"] if item["restorable"]}
        assert ids[-1] in restored
        assert ids[0] not in restored

    def test_export_ledger_records_each_bundle(self, clean_store):
        name, ids = self._prepare(clean_store)
        first = _checkpoint().export_recovery_bundle(name, "checkpoint")
        second = _checkpoint().export_recovery_bundle(name, "state")
        ledger = _checkpoint().export_ledger()
        versions = {entry["version"] for entry in ledger}
        assert first["version"] in versions
        assert second["version"] in versions
        entry = next(e for e in ledger if e["version"] == first["version"])
        assert entry["boundary"] == "checkpoint"
        assert entry["covers_all_checkpoints"] is True

    def test_state_store_consumes_snapshot_key_sets(self, clean_store):
        name, ids = self._prepare(clean_store)
        view = _checkpoint().state_store_consumes_snapshots(name)
        assert view["count"] == len(ids)
        assert view["state_keys"] == ["agg_op::offset"]
        for snapshot in view["snapshots"]:
            assert "state_versions" in snapshot
            assert "operator_states" not in snapshot

    def test_deferred_replay_compacts_released_states_only(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)["保留数量"] = 5
        _build_versions(STATE_CONFIG, "agg_op", [31])
        old_row = [r for r in store.rows("state_store")
                   if r.get("_state_config") == STATE_CONFIG][-1]
        first_cp = _checkpoint().create_checkpoint(
            name, {"agg_op": {"offset": 31}})
        time.sleep(0.004)
        _build_versions(STATE_CONFIG, "agg_op", [32])
        _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 32}})

        # 压缩前：旧版本仍被两个可用快照引用，必须保留
        before = _state().compact_duplicate_states(STATE_CONFIG)
        assert before["compacted"] == 0
        assert old_row["status"] == "已生效"

        _checkpoint().apply_retention_window(name, keep=1, mode="deferred")
        replay = service_for("replay_engine")
        job = replay.create_replay_job(
            "延迟裁剪回放", first_cp["checkpoint_id"], "任务拓扑样例1")
        result = replay.execute_replay(job["task_id"])
        assert result["ok"] is True

        # 回放成功后旧快照被释放，旧版本才允许压缩，最新版本保持在线
        assert old_row.get("_compacted") is True
        latest = [r for r in store.rows("state_store")
                  if r.get("_state_config") == STATE_CONFIG
                  and not r.get("_compacted") and r.get("_state_source") == "runtime"]
        assert latest


class TestEndToEndBoundary:
    def test_failed_replay_keeps_snapshot_even_after_window_shrink(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)["保留数量"] = 5
        _build_versions(STATE_CONFIG, "agg_op", [41])
        first = _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 41}})
        time.sleep(0.004)
        _build_versions(STATE_CONFIG, "agg_op", [42])
        _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 42}})

        _checkpoint().apply_retention_window(name, keep=1, mode="deferred")
        replay = service_for("replay_engine")
        job = replay.create_replay_job(
            "失败回放", first["checkpoint_id"], "不存在的拓扑")
        assert job["ok"] is False
        assert len(_checkpoint().available_snapshots(name)) == 2

    def test_replay_refuses_snapshot_outside_available_set(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)
        first = _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 51}})
        time.sleep(0.004)
        _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 52}})
        _checkpoint().apply_retention_window(name, keep=1, mode="immediate")

        replay = service_for("replay_engine")
        job = replay.create_replay_job(
            "裁剪后回放", first["checkpoint_id"], "任务拓扑样例1")
        assert job["ok"] is False
        assert any(item["checkpoint_id"] != first["checkpoint_id"]
                   for item in _checkpoint().available_snapshots(name))

    def test_bundle_round_trip_preserves_all_versions(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)["保留数量"] = 5
        _build_versions(STATE_CONFIG, "agg_op", [61])
        _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 61}})
        time.sleep(0.004)
        _build_versions(STATE_CONFIG, "agg_op", [62])
        _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 62}})

        exported = _checkpoint().export_recovery_bundle(name, "checkpoint")
        assert exported["covers_all_checkpoints"] is True
        entries = exported["bundle"]["states"]
        assert len({entry["version"] for entry in entries}) == 2

        imported = _checkpoint().import_recovery_bundle(exported["bundle"])
        assert imported["ok"] and imported["duplicated"] is False
        imported_versions = {
            entry["version"]
            for row in store.rows("state_store")
            if row.get("_import_version") == exported["version"]
            for entry in row["_imported_entries"]
        }
        assert imported_versions == {entry["version"] for entry in entries}

    def test_duplicate_import_does_not_grow_state_table(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)
        _build_versions(STATE_CONFIG, "agg_op", [71])
        _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 71}})
        bundle = _checkpoint().export_recovery_bundle(name, "checkpoint")["bundle"]

        _checkpoint().import_recovery_bundle(bundle)
        after_first = len(store.rows("state_store"))
        for _ in range(3):
            _checkpoint().import_recovery_bundle(bundle)
        assert len(store.rows("state_store")) == after_first


class TestImmediateCompaction:
    def test_immediate_window_compacts_released_versions(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)["保留数量"] = 5
        _build_versions(STATE_CONFIG, "agg_op", [81])
        first = _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 81}})
        time.sleep(0.004)
        _build_versions(STATE_CONFIG, "agg_op", [82])
        _checkpoint().create_checkpoint(name, {"agg_op": {"offset": 82}})

        result = _checkpoint().apply_retention_window(name, keep=1, mode="immediate")
        assert result["pruned"] == [first["checkpoint_id"]]
        assert result["compacted"] >= 1
        # 最新版本仍在，且压缩后仍能恢复到剩余快照
        coverage = _checkpoint().coverage_report(name)
        assert coverage["contiguous"] is True
        assert coverage["all_restorable"] is True


class TestRecoverableInterval:
    def test_pruned_snapshot_between_available_ones_breaks_interval(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)["保留数量"] = 10
        ids = []
        for value in range(1, 5):
            ids.append(_checkpoint().create_checkpoint(
                name, {"agg_op": {"offset": value}})["checkpoint_id"])
            time.sleep(0.004)

        # 手动制造缺口：只裁剪中间那个快照
        middle = next(r for r in store.rows("checkpoint_mgr")
                      if r.get("checkpoint_id") == ids[1])
        _checkpoint()._mark_pruned(middle, "人为缺口")

        coverage = _checkpoint().coverage_report(name)
        assert coverage["contiguous"] is False
        assert coverage["interval"]["gap_ts"]
        assert coverage["all_restorable"] is True  # 缺口是保留问题，不是状态版本问题

    def test_pruned_snapshots_before_window_do_not_break_interval(self, clean_store):
        name = CHECKPOINT_CONFIG
        _enable_config(name)["保留数量"] = 5
        for value in range(1, 5):
            _checkpoint().create_checkpoint(name, {"agg_op": {"offset": value}})
            time.sleep(0.004)
        _checkpoint().apply_retention_window(name, keep=2, mode="immediate")
        coverage = _checkpoint().coverage_report(name)
        assert coverage["contiguous"] is True
        assert coverage["interval"]["length"] == 2
