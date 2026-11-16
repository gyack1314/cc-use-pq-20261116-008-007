from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import json
import unittest

from talent_harvest.analytics import Analytics
from talent_harvest.audit import Auditor
from talent_harvest.cli import main, parser
from talent_harvest.contracts import HarvestError, canonical
from talent_harvest.export import Delivery
from talent_harvest.inventory import Inventory
from talent_harvest.lineage import Lineage
from talent_harvest.ops import Operations
from tests.support import Workspace, manifest, record, source


class OperationsTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.work.register()

    def tearDown(self):
        self.work.close()

    def test_page_retry_targets_only_one_page(self):
        path = manifest(self.work.path, pages=[[record()], [record(record_id="role-2", title="Analyst")]])
        self.work.harvester.ingest(path, only_page=2)
        batch = self.work.ledger.batch("company", "batch-1")
        self.assertEqual(batch["state"], "pending")
        self.assertEqual([page["state"] for page in batch["pages"]], ["pending", "complete"])
        result = self.work.harvester.page_retry(path, 1)
        self.assertEqual(result["state"], "complete")
        self.assertEqual(len(self.work.ledger.observations()), 2)

    def test_page_retry_rejects_out_of_range(self):
        path = manifest(self.work.path)
        for ordinal in (0, 2, -1):
            with self.subTest(ordinal=ordinal), self.assertRaises(HarvestError):
                self.work.harvester.page_retry(path, ordinal)

    def test_stop_on_error_preserves_following_pending_pages(self):
        path = manifest(self.work.path, pages=[[record()], [record(record_id="role-2")],
                                               [record(record_id="role-3")]])
        second = path.parent / "page-2.json"
        second.unlink()
        result = self.work.harvester.ingest(path, stop_on_error=True)
        self.assertEqual([page["ordinal"] for page in result["pages"]], [1, 2])
        self.assertEqual(self.work.ledger.page("company", "batch-1", 3)["state"], "pending")
        self.assertEqual(Inventory(self.work.ledger).recovery_plan()["retryable"], 2)

    def test_bad_record_rolls_back_entire_page(self):
        path = manifest(self.work.path, pages=[[record(), record(record_id="role-2", quantity="bad")]])
        result = self.work.harvester.ingest(path)
        self.assertEqual(result["state"], "failed")
        self.assertEqual(len(self.work.ledger.observations()), 0)
        self.assertEqual(self.work.ledger.page("company", "batch-1", 1)["error_code"], "quantity")

    def test_duplicate_record_ids_roll_back_entire_page(self):
        path = manifest(self.work.path, pages=[[record(), record(quantity=5)]])
        result = self.work.harvester.ingest(path)
        self.assertEqual(result["state"], "failed")
        self.assertEqual(len(self.work.ledger.observations()), 0)

    def test_invalid_json_page_does_not_block_other_pages(self):
        path = manifest(self.work.path, pages=[[record()], [record(record_id="role-2")]])
        broken = b"{not json"
        (path.parent / "page-1.json").write_bytes(broken)
        declaration = json.loads(path.read_text(encoding="utf-8"))
        declaration["pages"][0]["sha256"] = sha256(broken).hexdigest()
        path.write_text(canonical(declaration), encoding="utf-8")
        result = self.work.harvester.ingest(path)
        self.assertEqual(result["state"], "failed")
        self.assertEqual([page["state"] for page in result["pages"]], ["failed", "complete"])
        self.assertEqual(len(self.work.ledger.observations()), 1)

    def test_empty_page_can_complete_batch(self):
        path = manifest(self.work.path, pages=[[]])
        result = self.work.harvester.ingest(path)
        self.assertEqual(result["state"], "complete")
        self.assertEqual(result["pages"][0]["records"], 0)
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_source_revision_updates_name_and_trust(self):
        declaration = source()
        declaration["name"] = "New Mirror Name"
        declaration["trust"] = 95
        result = self.work.harvester.register_source(declaration)
        self.assertEqual(result["revision"], 2)
        self.assertEqual(self.work.ledger.source("company").trust, 95)
        self.assertEqual(self.work.harvester.register_source(declaration)["revision"], 2)

    def test_source_contract_freezes_after_ingest(self):
        self.work.ingest()
        declaration = source()
        declaration["field_map"] = {"title": "position"}
        with self.assertRaises(HarvestError) as caught:
            self.work.harvester.register_source(declaration)
        self.assertEqual(caught.exception.code, "source_contract_frozen")

    def test_cross_format_json_csv_html(self):
        self.work.register(key="campus", fmt="csv", kind="program")
        self.work.register(key="partners", fmt="html", kind="partnership")
        self.work.ingest()
        self.work.ingest(key="campus", fmt="csv", pages=[[record(record_id="cohort-1", title="AI Master's Program")]])
        self.work.ingest(key="partners", fmt="html", pages=[[record(record_id="joint-1", title="Joint Research Lab")]])
        current = Delivery(self.work.ledger).current()
        self.assertEqual(current["counts"]["accepted"], 3)
        kinds = {entity["record"]["kind"] for entity in current["accepted"]}
        self.assertEqual(kinds, {"position", "program", "partnership"})

    def test_field_map_on_csv_mirror(self):
        self.work.register(key="mapped", fmt="csv", field_map={"record_id": "external_id", "title": "role_name"})
        mapped = record()
        mapped["external_id"] = mapped.pop("record_id")
        mapped["role_name"] = mapped.pop("title")
        self.work.ingest(key="mapped", fmt="csv", pages=[[mapped]])
        selected = Delivery(self.work.ledger).current()["accepted"][0]["selected"]
        self.assertEqual(selected["source_record_id"], "role-1")

    def test_quarantined_latest_revision_suppresses_prior(self):
        self.work.ingest()
        self.work.ingest(batch="batch-2", pages=[[record(quantity=0)]])
        self.assertEqual(Delivery(self.work.ledger).current()["counts"]["accepted"], 0)
        quality = Delivery(self.work.ledger).current(include_quarantine=True)
        self.assertEqual(quality["counts"]["quarantined"], 1)
        self.assertEqual(len(Delivery(self.work.ledger).history()["observations"]), 2)

    def test_revised_identity_does_not_retain_old_entity(self):
        self.work.ingest()
        self.work.ingest(batch="batch-2", pages=[[record(title="Robotics Engineer")]])
        current = Delivery(self.work.ledger).current()
        self.assertEqual(current["counts"]["accepted"], 1)
        self.assertEqual(current["accepted"][0]["record"]["title"], "Robotics Engineer")

    def test_multi_source_identity_can_be_supported_by_two_claims(self):
        self.work.register(key="campus", trust=80)
        self.work.ingest()
        self.work.ingest(key="campus", pages=[[record(record_id="role-2")]])
        entity = Delivery(self.work.ledger).current()["accepted"][0]
        self.assertEqual(len(entity["evidence"]), 2)
        self.assertEqual(entity["selected"]["source"], "campus")
        self.assertEqual(Operations(self.work.ledger).consistency_report()["multi_source"], 1)

    def test_conflict_has_complete_provenance(self):
        self.work.register(key="campus")
        self.work.ingest()
        self.work.ingest(key="campus", pages=[[record(record_id="role-2", quantity=9)]])
        conflict = Delivery(self.work.ledger).current()["conflicts"][0]
        self.assertEqual(len(conflict["evidence"]), 2)
        self.assertEqual({item["source"] for item in conflict["evidence"]}, {"company", "campus"})
        self.assertEqual(Operations(self.work.ledger).consistency_report()["quantity_disputes"], 1)

    def test_quarantine_does_not_create_conflict(self):
        self.work.register(key="campus")
        self.work.ingest()
        self.work.ingest(key="campus", pages=[[record(record_id="role-2", quantity=0)]])
        current = Delivery(self.work.ledger).current(include_quarantine=True)
        self.assertEqual(current["counts"], {"accepted": 1, "conflicts": 0, "quarantined": 1})

    def test_trace_includes_source_events(self):
        self.work.ingest()
        identity = Delivery(self.work.ledger).current()["accepted"][0]["identity"]
        traced = Delivery(self.work.ledger).trace(identity)
        self.assertGreaterEqual(len(traced["events"]), 3)
        self.assertEqual(traced["history"][0]["identity"], identity)
        self.assertIsNotNone(traced["current"])

    def test_trace_rejects_unknown_identity(self):
        for identity in ("bad", "0" * 64):
            with self.subTest(identity=identity), self.assertRaises(HarvestError):
                Delivery(self.work.ledger).trace(identity)

    def test_lineage_timeline_shows_withdrawal(self):
        self.work.ingest()
        self.work.harvester.withdraw("company", "obsolete")
        timeline = Lineage(self.work.ledger).source_timeline("company")
        self.assertTrue(timeline["source"]["withdrawn_at"])
        self.assertEqual(timeline["batches"][0]["accepted"], 1)
        self.assertEqual(timeline["events"][-1]["event_type"], "source_withdrawn")

    def test_replay_check_detects_changed_mirror(self):
        path, _ = self.work.ingest()
        payload = canonical({"records": [record(quantity=77)]}).encode("utf-8")
        (path.parent / "page-1.json").write_bytes(payload)
        report = Operations(self.work.ledger).replay_check(path)
        self.assertFalse(report["consistent"])
        self.assertEqual(report["differences"][0]["code"], "page_digest")

    def test_preview_reports_invalid_page_without_state_mutation(self):
        path = manifest(self.work.path)
        (path.parent / "page-1.json").unlink()
        declaration = self.work.path / "source.json"
        declaration.write_text(json.dumps(source()), encoding="utf-8")
        preview = Operations(self.work.ledger).preview(declaration, path)
        self.assertEqual(preview["invalid_pages"], 1)
        self.assertEqual(preview["pages"][0]["code"], "page_missing")
        self.assertEqual(len(self.work.ledger.observations()), 0)

    def test_batch_comparison_requires_completion(self):
        self.work.ingest()
        manifest(self.work.path, batch="batch-2")
        with self.assertRaises(HarvestError):
            Inventory(self.work.ledger).batch_comparison("company", "batch-1", "batch-2")

    def test_quality_report_collects_multiple_reasons(self):
        self.work.register(key="untrusted", trust=10)
        self.work.ingest(key="untrusted", pages=[[record(quantity=0)]])
        report = Inventory(self.work.ledger).quality_report(source="untrusted")
        self.assertEqual(report["by_reason"], {"low_trust_source": 1, "zero_quantity": 1})
        self.assertEqual(len(report["examples"]["zero_quantity"]), 1)

    def test_age_profile_handles_future_and_old_records(self):
        self.work.ingest(pages=[[record(), record(record_id="role-2", title="Scientist",
                                                  published_at="2020-01-01")]])
        buckets = Analytics(self.work.ledger).age_profile("2026-01-01")["by_kind"]["position"]
        self.assertEqual(buckets["future"], 1)
        self.assertEqual(buckets["366+"], 1)

    def test_event_window_cursor_is_monotone(self):
        self.work.ingest()
        first = Operations(self.work.ledger).event_window(limit=1)
        second = Operations(self.work.ledger).event_window(start=first["next_event_id"], limit=1)
        self.assertGreater(second["next_event_id"], first["next_event_id"])

    def test_audit_detects_corrupt_raw_digest(self):
        self.work.ingest()
        self.work.ledger.db.execute("UPDATE observations SET raw_digest=?", ("0" * 64,))
        self.work.ledger.db.commit()
        report = Auditor(self.work.ledger).verify_state()
        self.assertFalse(report["ok"])
        self.assertIn("raw_digest", [item["code"] for item in report["findings"]])

    def test_audit_detects_incorrect_page_count(self):
        self.work.ingest()
        self.work.ledger.db.execute("UPDATE pages SET record_count=100")
        self.work.ledger.db.commit()
        findings = Auditor(self.work.ledger).verify_state()["findings"]
        self.assertIn("page_count", [item["code"] for item in findings])

    def test_analytics_trend_uses_capture_month(self):
        self.work.ingest()
        self.work.ingest(batch="batch-2", captured_at="2026-10-03T08:00:00+00:00",
                         pages=[[record(record_id="role-2", title="Scientist")]])
        trend = Analytics(self.work.ledger).trend()["by_capture_month"]
        self.assertEqual(trend["2026-09"]["observations"], 1)
        self.assertEqual(trend["2026-10"]["observations"], 1)

    def test_checkpoint_changes_when_new_batch_arrives(self):
        before = Operations(self.work.ledger).checkpoint()["sha256"]
        self.work.ingest()
        after = Operations(self.work.ledger).checkpoint()["sha256"]
        self.assertNotEqual(before, after)

    def test_cli_parser_supports_registered_commands(self):
        actions = ["audit", "operations", "sources", "quality", "recovery", "consistency",
                   "database-info", "checkpoint", "distribution", "coverage", "trend",
                   "conflicts", "health", "summary", "current", "history", "events"]
        for action in actions:
            with self.subTest(action=action):
                parsed = parser().parse_args(["--db", str(self.work.path / "state.sqlite3"), action])
                self.assertEqual(parsed.action, action)

    def test_cli_register_and_ingest_exit_success(self):
        declaration = self.work.path / "source.json"
        declaration.write_text(json.dumps(source(key="campus")), encoding="utf-8")
        path = manifest(self.work.path, key="campus")
        database = self.work.path / "cli.sqlite3"
        self.assertEqual(main(["--db", str(database), "register", str(declaration)]), 0)
        self.assertEqual(main(["--db", str(database), "ingest", str(path)]), 0)
        self.assertEqual(main(["--db", str(database), "audit"]), 0)
