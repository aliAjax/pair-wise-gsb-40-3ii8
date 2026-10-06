import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import DomainError, MaritimeSARService  # noqa: E402


class OfflineReplayTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = MaritimeSARService(Path(self.tmp.name) / "test.db")
        self.incident = self.svc.create_incident(
            "coord1", "coordinator", "SAR-100", "海燕号", 31.0, 122.0, 10.0, 3, "东海中心"
        )
        self.asset = self.svc.add_asset(
            "coord1", "coordinator", "海巡01", "vessel", ["surface", "night"], 31.0, 122.0, 20, 100, 5
        )
        self.area = self.svc.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-100", "surface", 31.1, 122.1, 8, 1
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _asset(self, name="海巡01"):
        return next(a for a in self.svc.list_assets() if a["name"] == name)

    def _area(self, code="A-100"):
        return next(a for a in self.svc.state()["search_areas"] if a["code"] == code)

    def test_replay_uses_field_time_not_upload_order(self):
        # Upload order is reversed: withdraw (08:30) arrives before assign (08:00).
        events = [
            {"type": "withdraw", "client_event_id": "ev-w", "asset_id": self.asset["id"],
             "reason": "油料告急", "occurred_at": "2026-10-06T08:30:00Z",
             "expected_asset_version": 2},
            {"type": "assignment", "client_event_id": "ev-a", "asset_id": self.asset["id"],
             "area_id": self.area["id"], "occurred_at": "2026-10-06T08:00:00Z",
             "expected_asset_version": 1},
        ]
        result = self.svc.merge_offline_batch("field1", "field", "batch-order", events)
        self.assertEqual("merged", result["status"])
        self.assertEqual(2, result["summary"]["accepted"])
        self.assertEqual(0, result["summary"]["conflicts"])
        # Assignment then withdrawal both applied in field order: asset is free again.
        self.assertEqual("available", self._asset()["status"])
        self.assertIsNone(self._area()["assigned_asset_id"])
        # Timeline entries honour field time.
        tl = self.svc.incident_timeline(self.incident["id"])
        audit = [x for x in tl if x["action"] in {"area.assigned", "area.unassigned"}]
        self.assertEqual(["area.assigned", "area.unassigned"], [x["action"] for x in audit])
        self.assertLessEqual(audit[0]["occurred_at"], audit[1]["occurred_at"])

    def test_conflict_with_shore_modification_is_parked_untouched(self):
        # Shore assigns the asset to another area first.
        area2 = self.svc.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-101", "surface", 31.15, 122.15, 8, 2
        )
        self.svc.assign_area("coord1", "coordinator", area2["id"], self.asset["id"], self.asset["version"])
        events = [{
            "type": "assignment", "client_event_id": "ev-a2", "asset_id": self.asset["id"],
            "area_id": self.area["id"], "occurred_at": "2026-10-06T09:00:00Z",
            "expected_asset_version": 1,
        }]
        result = self.svc.merge_offline_batch("field1", "field", "batch-conflict", events)
        self.assertEqual("conflict", result["status"])
        self.assertEqual(1, result["summary"]["conflicts"])
        parked = result["summary"]["events"][0]
        self.assertEqual("conflict", parked["status"])
        self.assertIn("岸端已修改该资源", parked["error"])
        # Original shore-side occupancy is untouched.
        self.assertEqual("assigned", self._asset()["status"])
        self.assertEqual(self.asset["id"], self._area("A-101")["assigned_asset_id"])
        self.assertIsNone(self._area()["assigned_asset_id"])

    def test_coordinator_can_apply_then_discard_conflicts(self):
        # Conflict: shore withdrew/reassigned state differs; use closed-incident transfer for discard.
        closed = self.svc.create_incident(
            "coord1", "coordinator", "SAR-101", "远星号", 32.0, 123.0, 8.0, 3, "南海中心"
        )
        self.svc.close_incident("coord1", "coordinator", closed["id"], "false_alarm", closed["version"])
        batch = self.svc.merge_offline_batch("field1", "field", "batch-transfer", [
            {"type": "transfer", "client_event_id": "ev-t", "incident_id": closed["id"],
             "new_org": "广州中心", "occurred_at": "2026-10-06T07:00:00Z",
             "expected_version": 1},
        ])
        self.assertEqual(1, batch["summary"]["conflicts"])
        discarded = self.svc.resolve_offline_event("coord1", "coordinator", "ev-t", "discard", "事件已结案")
        self.assertEqual("discarded", discarded["status"])
        incident = next(i for i in self.svc.state()["incidents"] if i["id"] == closed["id"])
        self.assertEqual("南海中心", incident["lead_org"])

        # Apply route on the asset conflict scenario.
        area2 = self.svc.create_search_area(
            "coord1", "coordinator", self.incident["id"], "A-102", "surface", 31.2, 122.2, 8, 2
        )
        self.svc.assign_area("coord1", "coordinator", area2["id"], self.asset["id"], self._asset()["version"])
        self.svc.merge_offline_batch("field1", "field", "batch-apply", [
            {"type": "assignment", "client_event_id": "ev-a3", "asset_id": self.asset["id"],
             "area_id": self.area["id"], "occurred_at": "2026-10-06T10:00:00Z",
             "expected_asset_version": 1},
        ])
        applied = self.svc.resolve_offline_event("coord1", "coordinator", "ev-a3", "apply", "以现场为准")
        self.assertEqual("merged", applied["status"])
        self.assertEqual(self.asset["id"], self._area()["assigned_asset_id"])
        # The shore-side area gets released; the asset stays occupied exactly once.
        self.assertIsNone(self._area("A-102")["assigned_asset_id"])
        self.assertEqual("assigned", self._asset()["status"])
        # Batch flips to merged once no conflicts remain.
        batches = {b["client_batch_id"]: b for b in self.svc.list_offline_batches()}
        self.assertEqual("merged", batches["batch-apply"]["status"])

    def test_retry_is_idempotent_and_never_double_occupies(self):
        events = [{
            "type": "assignment", "client_event_id": "ev-idem", "asset_id": self.asset["id"],
            "area_id": self.area["id"], "occurred_at": "2026-10-06T06:00:00Z",
        }]
        first = self.svc.merge_offline_batch("field1", "field", "batch-retry", events)
        self.assertFalse(first["idempotent"])
        version_after = self._asset()["version"]
        # Simulate a failed network response followed by an identical retry.
        second = self.svc.merge_offline_batch("field1", "field", "batch-retry", events)
        self.assertTrue(second["idempotent"])
        self.assertEqual(1, second["summary"]["accepted"])
        self.assertEqual(version_after, self._asset()["version"])
        self.assertEqual(1, self._asset()["version"] - self.asset["version"])

    def test_same_event_in_another_batch_is_not_replayed(self):
        events = [{
            "type": "clue", "client_event_id": "ev-clue", "incident_id": self.incident["id"],
            "latitude": 31.1, "longitude": 122.1, "confidence": 0.8, "source": "visual",
            "occurred_at": "2026-10-06T05:30:00Z",
        }]
        self.svc.merge_offline_batch("field1", "field", "batch-x", events)
        again = self.svc.merge_offline_batch("field2", "field", "batch-y", events)
        item = again["summary"]["events"][0]
        self.assertTrue(item["idempotent"])
        clues = [c for c in self.svc.state()["clues"] if c["client_event_id"] == "ev-clue"]
        self.assertEqual(1, len(clues))

    def test_batch_list_groups_effective_and_pending_results(self):
        good = {"type": "assignment", "client_event_id": "ev-g", "asset_id": self.asset["id"],
                "area_id": self.area["id"], "occurred_at": "2026-10-06T11:00:00Z"}
        bad = {"type": "unknown", "client_event_id": "ev-b", "occurred_at": "2026-10-06T11:05:00Z"}
        missing_time = {"type": "timeline", "client_event_id": "ev-m",
                        "incident_id": self.incident["id"], "action": "note"}
        res = self.svc.merge_offline_batch("field1", "field", "batch-mix", [bad, good, missing_time])
        self.assertEqual(1, res["summary"]["accepted"])
        self.assertEqual(2, res["summary"]["rejected"])
        batches = self.svc.list_offline_batches()
        self.assertEqual(1, len(batches))
        b = batches[0]
        self.assertEqual(1, b["summary"]["accepted"])
        # Events are presented in field-time order even for display.
        self.assertEqual("ev-g", b["events"][0]["client_event_id"])
        self.assertEqual("merged", b["events"][0]["status"])

    def test_field_role_cannot_adjudicate(self):
        self.svc.merge_offline_batch("field1", "field", "batch-deny", [
            {"type": "transfer", "client_event_id": "ev-deny", "incident_id": self.incident["id"],
             "new_org": "别处", "occurred_at": "2026-10-06T12:00:00Z", "expected_version": 99},
        ])
        with self.assertRaises(DomainError) as ctx:
            self.svc.resolve_offline_event("field1", "field", "ev-deny", "apply")
        self.assertEqual(403, ctx.exception.status)


if __name__ == "__main__":
    unittest.main()
