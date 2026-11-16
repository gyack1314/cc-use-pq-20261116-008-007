from __future__ import annotations

from pathlib import Path
import json
import unittest

from talent_harvest.audit import Auditor
from talent_harvest.contracts import HarvestError
from talent_harvest.export import Delivery, verify_snapshot
from talent_harvest.inventory import Inventory
from talent_harvest.lineage import Lineage
from talent_harvest.ops import Operations
from talent_harvest.analytics import Analytics
from tests.support import Workspace, manifest, record, source


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.work.register()

    def tearDown(self):
        self.work.close()

    def test_complete_batch_and_current_view(self):
        path, result = self.work.ingest()
        self.assertEqual(result["state"], "complete")
        self.assertEqual(result["completed_pages"], 1)
        self.assertEqual(result["total_pages"], 1)
        view = Delivery(self.work.ledger).current()
        self.assertEqual(view["counts"]["accepted"], 1)
        self.assertEqual(view["accepted"][0]["record"]["quantity"], 4)
        self.assertEqual(view["accepted"][0]["selected"]["version"], 1)

    def test_replaying_same_manifest_is_idempotent(self):
        path, first = self.work.ingest()
        events = len(self.work.ledger.events())
        second = self.work.harvester.ingest(path)
        self.assertEqual(second["state"], "complete")
        self.assertTrue(second["pages"][0]["unchanged"])
        self.assertEqual(len(self.work.ledger.observations()), 1)
        self.assertEqual(len(self.work.ledger.events()), events)

    def test_new_batch_increments_record_version(self):
        self.work.ingest()
        self.work.ingest(batch="batch-2", pages=[[record(quantity=6)]])
        history = Delivery(self.work.ledger).history(source="company")["observations"]
        self.assertEqual([row["version"] for row in history], [1, 2])
        self.assertEqual(Delivery(self.work.ledger).current()["accepted"][0]["record"]["quantity"], 6)
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])

    def test_missing_page_fails_then_recovers_without_duplicate(self):
        path = manifest(self.work.path, pages=[[record()], [record(record_id="role-2", title="Researcher")]])
        page_2 = path.parent / "page-2.json"
        original = page_2.read_bytes()
        page_2.unlink()
        failed = self.work.harvester.ingest(path)
        self.assertEqual(failed["state"], "failed")
        self.assertEqual([page["state"] for page in failed["pages"]], ["complete", "failed"])
        self.assertEqual(len(self.work.ledger.observations()), 1)
        self.assertEqual(Inventory(self.work.ledger).recovery_plan()["retryable"], 1)
        page_2.write_bytes(original)
        recovered = self.work.harvester.resume(path)
        self.assertEqual(recovered["state"], "complete")
        self.assertTrue(recovered["pages"][0]["unchanged"])
        self.assertEqual(len(self.work.ledger.observations()), 2)
        self.assertEqual(self.work.ledger.page("company", "batch-1", 2)["attempts"], 2)

    def test_digest_mismatch_is_quarantined_at_page_boundary(self):
        path = manifest(self.work.path)
        (path.parent / "page-1.json").write_text("altered", encoding="utf-8")
        result = self.work.harvester.ingest(path)
        self.assertEqual(result["state"], "failed")
        self.assertIn("page_digest", result["pages"][0]["error"])
        self.assertEqual(Delivery(self.work.ledger).current()["counts"]["accepted"], 0)
        self.assertEqual(self.work.ledger.page("company", "batch-1", 1)["error_code"], "page_digest")

    def test_immutable_manifest_blocks_same_batch_mutation(self):
        self.work.ingest()
        changed = manifest(self.work.path, pages=[[record(quantity=7)]])
        with self.assertRaises(HarvestError) as caught:
            self.work.harvester.ingest(changed)
        self.assertEqual(caught.exception.code, "batch_immutable")
        self.assertEqual(len(self.work.ledger.observations()), 1)

    def test_zero_quantity_is_quarantined_not_exported(self):
        self.work.ingest(pages=[[record(quantity=0)]])
        delivery = Delivery(self.work.ledger)
        self.assertEqual(delivery.current()["counts"]["accepted"], 0)
        with_quality = delivery.current(include_quarantine=True)
        self.assertEqual(with_quality["counts"]["quarantined"], 1)
        self.assertEqual(with_quality["quarantined"][0]["reasons"], ["zero_quantity"])
        self.assertEqual(Inventory(self.work.ledger).quality_report()["by_reason"]["zero_quantity"], 1)

    def test_future_publication_is_quarantined(self):
        self.work.ingest(pages=[[record(published_at="2027-01-01")]])
        view = Delivery(self.work.ledger).current(include_quarantine=True)
        self.assertEqual(view["quarantined"][0]["reasons"], ["future_publication"])
        self.assertEqual(view["counts"]["accepted"], 0)

    def test_low_trust_source_is_quarantined(self):
        self.work.harvester.register_source(source(key="untrusted", trust=10))
        self.work.ingest(key="untrusted")
        current = Delivery(self.work.ledger).current(include_quarantine=True)
        self.assertEqual(current["counts"]["quarantined"], 1)
        self.assertIn("low_trust_source", current["quarantined"][0]["reasons"])

    def test_partnership_requires_explicit_reference(self):
        self.work.harvester.register_source(source(key="partners", kind="partnership"))
        rows = [record(reference="", title="Joint Lab")]
        self.work.ingest(key="partners", pages=[rows])
        self.assertEqual(Delivery(self.work.ledger).current(include_quarantine=True)["counts"]["quarantined"], 1)

    def test_two_sources_deduplicate_same_identity(self):
        self.work.harvester.register_source(source(key="campus", trust=90))
        self.work.ingest()
        self.work.ingest(key="campus", pages=[[record(record_id="vacancy-2")]])
        current = Delivery(self.work.ledger).current()
        self.assertEqual(current["counts"]["accepted"], 1)
        self.assertEqual(current["accepted"][0]["selected"]["source"], "campus")
        self.assertEqual(len(current["accepted"][0]["evidence"]), 2)

    def test_cross_source_quantity_conflict_is_not_exported(self):
        self.work.harvester.register_source(source(key="campus", trust=90))
        self.work.ingest()
        self.work.ingest(key="campus", pages=[[record(record_id="vacancy-2", quantity=11)]])
        current = Delivery(self.work.ledger).current()
        self.assertEqual(current["counts"]["accepted"], 0)
        self.assertEqual(current["counts"]["conflicts"], 1)
        self.assertEqual(current["conflicts"][0]["reason"], "quantity_mismatch")
        self.assertEqual(Analytics(self.work.ledger).conflict_report()["by_reason"], {"quantity_mismatch": 1})

    def test_source_withdrawal_removes_active_claim(self):
        self.work.ingest()
        original = Delivery(self.work.ledger).current()
        self.assertEqual(original["counts"]["accepted"], 1)
        result = self.work.harvester.withdraw("company", "source retired")
        self.assertFalse(result["unchanged"])
        self.assertEqual(Delivery(self.work.ledger).current()["counts"]["accepted"], 0)
        self.assertEqual(len(Delivery(self.work.ledger).history()["observations"]), 1)
        self.assertTrue(self.work.harvester.withdraw("company", "repeat")["unchanged"])
        with self.assertRaises(HarvestError):
            self.work.ingest(batch="batch-2")

    def test_withdrawal_resolves_cross_source_conflict(self):
        self.work.harvester.register_source(source(key="campus"))
        self.work.ingest()
        self.work.ingest(key="campus", pages=[[record(record_id="vacancy-2", quantity=6)]])
        self.assertEqual(Delivery(self.work.ledger).current()["counts"]["conflicts"], 1)
        self.work.harvester.withdraw("campus", "retracted")
        self.assertEqual(Delivery(self.work.ledger).current()["counts"]["accepted"], 1)

    def test_snapshot_has_stable_digest_and_can_verify(self):
        self.work.ingest()
        path = self.work.path / "out.json"
        snapshot = Delivery(self.work.ledger).write_snapshot(path)
        self.assertEqual(verify_snapshot(path, snapshot["sha256"])["counts"]["accepted"], 1)
        path.write_text("{}", encoding="utf-8")
        with self.assertRaises(HarvestError):
            verify_snapshot(path, snapshot["sha256"])

    def test_auditor_detects_stale_cursor(self):
        self.work.ingest()
        self.assertTrue(Auditor(self.work.ledger).verify_state()["ok"])
        self.work.ledger.db.execute("UPDATE source_records SET version=99")
        self.work.ledger.db.commit()
        report = Auditor(self.work.ledger).verify_state()
        self.assertFalse(report["ok"])
        self.assertIn("cursor_stale", [item["code"] for item in report["findings"]])

    def test_lineage_follows_revisions_and_events(self):
        self.work.ingest()
        self.work.ingest(batch="batch-2", pages=[[record(quantity=8)]])
        lineage = Lineage(self.work.ledger).record("company", "role-1")
        self.assertEqual(len(lineage["versions"]), 2)
        self.assertIn("quantity", lineage["versions"][1]["changed_fields"])
        self.assertEqual(lineage["versions"][1]["version"], 2)
        batch = Lineage(self.work.ledger).batch("company", "batch-2")
        self.assertEqual(batch["pages"][0]["observations"][0]["version"], 2)
        self.assertTrue(Lineage(self.work.ledger).evidence_digest("company", "batch-2")["sha256"])

    def test_preview_and_replay_are_read_only(self):
        path = manifest(self.work.path)
        source_path = self.work.path / "declaration.json"
        source_path.write_text(json.dumps(source()), encoding="utf-8")
        preview = Operations(self.work.ledger).preview(source_path, path)
        self.assertEqual(preview["valid_pages"], 1)
        self.assertEqual(len(self.work.ledger.events()), 1)
        self.work.harvester.ingest(path)
        self.assertTrue(Operations(self.work.ledger).replay_check(path)["consistent"])

    def test_inventory_reports_revision_and_quality(self):
        self.work.ingest(pages=[[record(), record(record_id="role-2", quantity=0)]])
        report = Inventory(self.work.ledger).source_report("company")
        self.assertEqual(report["unique_record_ids"], 2)
        self.assertEqual(report["latest_record_counts"], {"accepted": 1, "quarantined": 1})
        quality = Inventory(self.work.ledger).quality_report()
        self.assertEqual(quality["by_source"]["company"]["quarantined"], 1)

    def test_analytics_distribution_excludes_conflicts(self):
        self.work.ingest(pages=[[record(), record(record_id="role-2", title="Data Scientist", quantity=3)]])
        distribution = Analytics(self.work.ledger).distribution()
        self.assertEqual(distribution["by_kind"]["position"]["quantity"], 7)
        self.assertEqual(distribution["by_region"]["Shanghai"]["entities"], 2)
        self.assertEqual(Analytics(self.work.ledger).age_profile("2026-10-01")["by_kind"]["position"]["0-30"], 2)

    def test_batch_comparison_identifies_changes(self):
        self.work.ingest(pages=[[record(), record(record_id="role-2", title="Analyst")]])
        self.work.ingest(batch="batch-2", pages=[[record(quantity=8), record(record_id="role-3", title="Scientist")]])
        comparison = Inventory(self.work.ledger).batch_comparison("company", "batch-1", "batch-2")
        self.assertEqual(comparison["counts"], {"added": 1, "absent": 1, "changed": 1, "unchanged": 0})
        self.assertEqual(comparison["changed"][0]["after"]["version"], 2)

    def test_cursor_windows_and_checkpoint(self):
        self.work.ingest(pages=[[record(), record(record_id="role-2", title="Analyst")]])
        operations = Operations(self.work.ledger)
        first = operations.record_window(limit=1)
        self.assertEqual(len(first["observations"]), 1)
        self.assertTrue(first["has_more"])
        second = operations.record_window(start=first["next_observation_id"], limit=1)
        self.assertEqual(len(second["observations"]), 1)
        self.assertNotEqual(first["next_observation_id"], second["next_observation_id"])
        self.assertGreater(operations.checkpoint()["state"]["event_cursor"], 0)

    def test_operational_and_health_reports(self):
        self.work.ingest()
        self.assertEqual(Auditor(self.work.ledger).operational()["batches"][0]["complete"], 1)
        self.assertEqual(Analytics(self.work.ledger).delivery_health()["pages"]["complete"], 1)
        self.assertEqual(Analytics(self.work.ledger).coverage()["contributing"], 1)
