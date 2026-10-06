import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import DomainError, MaritimeSARService  # noqa: E402


class MaritimeSARFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.incident = self.service.create_incident(
            "coord1", "coordinator", "SAR-001", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset = self.service.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 20, 100, 5
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_complete_assignment_clue_offline_and_close_flow(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-01", "surface", 31.1, 122.1, 8, 1
        )
        assigned = self.service.assign_area("coord1", "coordinator", area["id"], self.asset["id"], self.asset["version"])
        self.assertEqual("assigned", assigned["status"])
        clue = self.service.record_clue(
            "field1", "field", self.incident["id"], "evt-1", 31.1, 122.1, 0.9, "visual", area["id"]
        )
        self.assertEqual("verified", self.service.verify_clue("analyst1", "analyst", clue["id"], "verified")["status"])
        batch = self.service.merge_offline_batch(
            "field1", "field", "batch-1",
            [{"type": "clue", "client_event_id": "off-1", "incident_id": self.incident["id"],
              "latitude": 31.11, "longitude": 122.11, "confidence": 0.7, "source": "radio"}],
        )
        self.assertEqual(1, batch["summary"]["accepted"])
        self.assertTrue(self.service.merge_offline_batch("field1", "field", "batch-1", [])["idempotent"])
        updated_asset = self.service.list_assets()[0]
        self.service.withdraw_asset("coord1", "coordinator", self.asset["id"], "任务移交", updated_asset["version"])
        current_area = self.service.state()["search_areas"][0]
        self.service.complete_area("coord1", "coordinator", area["id"], "abandoned", current_area["version"])
        current_incident = [x for x in self.service.state()["incidents"] if x["id"] == self.incident["id"]][0]
        closed = self.service.close_incident("coord1", "coordinator", self.incident["id"], "resolved", current_incident["version"])
        self.assertEqual("closed", closed["status"])
        self.assertGreaterEqual(len(self.service.incident_timeline(self.incident["id"])), 6)

    def test_duplicate_alarm_and_invalid_position_are_controlled(self):
        duplicate = self.service.create_incident(
            "op1", "operator", "SAR-002", "海燕号", 31.01, 122.01, 5.0, 3, "东海中心"
        )
        self.assertEqual("duplicate", duplicate["status"])
        self.assertEqual(self.incident["id"], duplicate["duplicate_of"])
        invalid = self.service.record_clue(
            "field1", "field", self.incident["id"], "evt-far", 45.0, 130.0, 0.8, "radio"
        )
        self.assertEqual("invalid", invalid["status"])
        with self.assertRaises(DomainError):
            self.service.verify_clue("field1", "field", invalid["id"], "verified")

    def test_assignment_conflict_and_permission(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-02", "surface", 31.1, 122.1, 5
        )
        self.service.assign_area("coord1", "coordinator", area["id"], self.asset["id"], self.asset["version"])
        area2 = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-03", "surface", 31.2, 122.2, 5
        )
        with self.assertRaises(DomainError) as ctx:
            self.service.assign_area("coord1", "coordinator", area2["id"], self.asset["id"], self.asset["version"])
        self.assertEqual(409, ctx.exception.status)
        with self.assertRaises(DomainError) as ctx2:
            self.service.create_search_area("field1", "field", self.incident["id"], "A-04", "surface", 31, 122, 5)
        self.assertEqual(403, ctx2.exception.status)

    def test_offline_replays_by_field_time_not_upload_order(self):
        # 上传顺序：先移交(08:30)、再撤回(08:20)、最后占用(08:10)；重放必须按 08:10→08:20→08:30
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "B-01", "surface", 31.05, 122.05, 20, 1
        )
        events = [
            {"type": "transfer", "client_event_id": "ev-transfer", "incident_id": self.incident["id"],
             "new_org": "南海中心", "expected_version": self.incident["version"],
             "occurred_at": "2026-10-06T08:30:00Z"},
            {"type": "asset.withdraw", "client_event_id": "ev-withdraw", "asset_id": self.asset["id"],
             "reason": "燃料不足", "expected_asset_version": self.asset["version"] + 1,
             "occurred_at": "2026-10-06T08:20:00Z"},
            {"type": "assign", "client_event_id": "ev-assign", "area_id": area["id"],
             "asset_id": self.asset["id"], "expected_asset_version": self.asset["version"],
             "occurred_at": "2026-10-06T08:10:00Z"},
        ]
        batch = self.service.merge_offline_batch("field1", "field", "batch-order", events)
        self.assertEqual("merged", batch["status"])
        self.assertEqual(3, batch["summary"]["accepted"])
        # 最终状态：资源先占用后撤回 => available；事件最终移交到南海中心
        state = self.service.state()
        asset = next(a for a in state["assets"] if a["id"] == self.asset["id"])
        self.assertEqual("available", asset["status"])
        self.assertEqual(3, asset["version"])
        incident = next(i for i in state["incidents"] if i["id"] == self.incident["id"])
        self.assertEqual("南海中心", incident["lead_org"])
        # 时间线按现场时间归位，而不是服务器写入时间
        actions = self.service.incident_timeline(self.incident["id"])
        ordered = [a for a in actions if a.get("occurred_at")]
        self.assertEqual(
            ["area.assigned", "area.unassigned", "incident.transferred"],
            [a["action"] for a in ordered],
        )
        self.assertEqual("2026-10-06T08:10:00+00:00", ordered[0]["occurred_at"])

    def test_offline_conflicts_are_held_until_adjudicated(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "C-01", "surface", 31.05, 122.05, 20, 1
        )
        # 岸端离线期间已把资源派到别的区域（资源版本前移）
        other_area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "C-02", "surface", 31.06, 122.06, 20, 2
        )
        self.service.assign_area("coord1", "coordinator", other_area["id"], self.asset["id"], self.asset["version"])
        batch = self.service.merge_offline_batch(
            "field1", "field", "batch-conflict",
            [{"type": "assign", "client_event_id": "ev-conflict", "area_id": area["id"],
              "asset_id": self.asset["id"], "expected_asset_version": 1,
              "occurred_at": "2026-10-06T09:00:00Z"}],
        )
        # 冲突项保留原操作，批次挂起，不能静默占用资源
        self.assertEqual("pending_review", batch["status"])
        item = batch["summary"]["events"][0]
        self.assertEqual("conflict", item["status"])
        self.assertEqual("version_mismatch", item["conflict_code"])
        state = self.service.state()
        self.assertIsNone(next(a for a in state["search_areas"] if a["id"] == area["id"])["assigned_asset_id"])
        # 未 force 驳回：维持岸端现状
        dismissed = self.service.resolve_offline_conflict(
            "coord1", "coordinator", "ev-conflict", "dismiss", note="岸端安排有效"
        )
        self.assertEqual("rejected", dismissed["status"])
        self.assertEqual("merged", dismissed["batch_status"])
        self.assertIsNone(next(a for a in self.service.state()["search_areas"] if a["id"] == area["id"])["assigned_asset_id"])

    def test_offline_conflict_force_apply_matches_field_intent(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "D-01", "surface", 31.05, 122.05, 20, 1
        )
        other_area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "D-02", "surface", 31.06, 122.06, 20, 2
        )
        self.service.assign_area("coord1", "coordinator", other_area["id"], self.asset["id"], self.asset["version"])
        self.service.merge_offline_batch(
            "field1", "field", "batch-force",
            [{"type": "assign", "client_event_id": "ev-force", "area_id": area["id"],
              "asset_id": self.asset["id"], "expected_asset_version": 1,
              "occurred_at": "2026-10-06T09:10:00Z"}],
        )
        # 硬错误（海况/能力）不会因 force 放松；这里条件合格，force 后以现场为准改派
        resolved = self.service.resolve_offline_conflict(
            "coord1", "coordinator", "ev-force", "apply", force=True, note="现场处置优先"
        )
        self.assertEqual("merged", resolved["status"])
        state = self.service.state()
        self.assertEqual(self.asset["id"], next(a for a in state["search_areas"] if a["id"] == area["id"])["assigned_asset_id"])
        self.assertIsNone(next(a for a in state["search_areas"] if a["id"] == other_area["id"])["assigned_asset_id"])
        self.assertEqual("assigned", next(a for a in state["assets"] if a["id"] == self.asset["id"])["status"])

    def test_offline_retry_is_idempotent_and_does_not_double_occupy(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "E-01", "surface", 31.05, 122.05, 20, 1
        )
        payload = [{"type": "assign", "client_event_id": "ev-once", "area_id": area["id"],
                    "asset_id": self.asset["id"], "expected_asset_version": self.asset["version"],
                    "occurred_at": "2026-10-06T10:00:00Z"}]
        first = self.service.merge_offline_batch("field1", "field", "batch-retry", payload)
        self.assertEqual(1, first["summary"]["accepted"])
        # 同批次号重试：原样返回，不产生第二次占用/审计
        retry = self.service.merge_offline_batch("field1", "field", "batch-retry", payload)
        self.assertTrue(retry["idempotent"])
        # 同一事件换个批次补传：引用既有 ledger，仍然不重复生效
        retry2 = self.service.merge_offline_batch("field1", "field", "batch-retry-2", payload)
        self.assertEqual("merged", retry2["status"])
        self.assertTrue(retry2["summary"]["events"][0]["idempotent"])
        self.assertEqual(2, self.service.list_assets()[0]["version"])
        assigned_actions = [t for t in self.service.incident_timeline(self.incident["id"]) if t["action"] == "area.assigned"]
        self.assertEqual(1, len(assigned_actions))

    def test_clue_after_incident_closed_is_conflict_and_can_be_force_applied(self):
        area = self.service.create_search_area(
            "coord1", "coordinator", self.incident["id"], "F-01", "surface", 31.05, 122.05, 20, 1
        )
        assigned_area = self.service.assign_area("coord1", "coordinator", area["id"], self.asset["id"], self.asset["version"])
        self.service.withdraw_asset("coord1", "coordinator", self.asset["id"], "撤", 2)
        self.service.complete_area("coord1", "coordinator", area["id"], "abandoned", assigned_area["version"] + 1)
        self.service.close_incident("coord1", "coordinator", self.incident["id"], "resolved", 1)
        batch = self.service.merge_offline_batch(
            "field1", "field", "batch-closed",
            [{"type": "clue", "client_event_id": "ev-late-clue", "incident_id": self.incident["id"],
              "latitude": 31.1, "longitude": 122.1, "confidence": 0.6, "source": "visual",
              "occurred_at": "2026-10-06T11:00:00Z"}],
        )
        self.assertEqual("pending_review", batch["status"])
        self.assertEqual("incident_closed", batch["summary"]["events"][0]["conflict_code"])
        with self.assertRaises(DomainError):
            self.service.resolve_offline_conflict("coord1", "coordinator", "ev-late-clue", "apply")
        resolved = self.service.resolve_offline_conflict(
            "coord1", "coordinator", "ev-late-clue", "apply", force=True
        )
        self.assertEqual("merged", resolved["status"])
        self.assertTrue(self.service.state()["clues"][0]["merged_at"])


if __name__ == "__main__":
    unittest.main()
