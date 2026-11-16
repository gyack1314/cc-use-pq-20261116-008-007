from __future__ import annotations

import contextlib
import io
import json
import threading
import unittest

from talent_harvest.audit import Auditor
from talent_harvest.cli import main, parser
from talent_harvest.contracts import HarvestError
from talent_harvest.export import Delivery
from talent_harvest.ledger import Ledger
from talent_harvest.review import ReviewBoard
from tests.support import Workspace, manifest, record, source


class ReviewTests(unittest.TestCase):
    def setUp(self):
        self.work = Workspace()
        self.work.register()
        self.work.register(key="campus", trust=90)
        self.board = ReviewBoard(self.work.ledger)
        self.delivery = Delivery(self.work.ledger)

    def tearDown(self):
        self.work.close()

    def conflict(self, campus_quantity=11):
        self.work.ingest()
        self.work.ingest(key="campus", pages=[[record(record_id="vacancy-2", quantity=campus_quantity)]])
        current = self.delivery.current()
        self.assertEqual(current["counts"]["conflicts"], 1)
        return current["conflicts"][0]["identity"]

    def basis(self, campus_version=1):
        return ["company/role-1/1", f"campus/vacancy-2/{campus_version}"]

    def audit_ok(self):
        report = Auditor(self.work.ledger).verify_state()
        self.assertTrue(report["ok"], report["findings"])

    def test_dispute_pins_claim_versions_and_quality_status(self):
        identity = self.conflict()
        candidates = self.board.disputes()
        self.assertEqual(candidates["counts"], {"open": 1, "resolved": 0, "closed": 0})
        dispute = candidates["disputes"][0]
        self.assertEqual(dispute["identity"], identity)
        self.assertEqual(dispute["reason"], "quantity_mismatch")
        self.assertEqual(dispute["state"], "open")
        self.assertEqual(dispute["revision"], 1)
        self.assertIsNone(dispute["active_review"])
        claims = {claim["source"]: claim for claim in dispute["claims"]}
        self.assertEqual(set(claims), {"company", "campus"})
        for claim in claims.values():
            self.assertEqual(claim["status"], "accepted")
            self.assertEqual(claim["reasons"], [])
            self.assertEqual(claim["version"], 1)
            self.assertEqual(len(claim["page_digest"]), 64)
        self.assertEqual(claims["company"]["trust"], 70)
        self.assertEqual(claims["campus"]["trust"], 90)
        self.assertEqual(dispute["evidence"],
                         [{"source": "campus", "record_id": "vacancy-2", "version": 1},
                          {"source": "company", "record_id": "role-1", "version": 1}])
        current = self.delivery.current()
        self.assertEqual(current["counts"]["accepted"], 0)
        self.assertEqual(current["conflicts"][0]["dispute_id"], dispute["dispute_id"])
        self.audit_ok()

    def test_review_resolution_delivers_selection_and_excludes_challenged(self):
        identity = self.conflict()
        result = self.board.submit(identity, "alice", "company/role-1/1", "campus/vacancy-2/1",
                                   self.basis(), note="campus 页摘要存疑，名额待补页")
        self.assertEqual(result["state"], "resolved")
        current = self.delivery.current()
        self.assertEqual(current["counts"], {"accepted": 1, "conflicts": 0, "quarantined": 0})
        entity = current["accepted"][0]
        self.assertEqual(entity["record"]["quantity"], 4)
        self.assertEqual(entity["selected"]["source"], "company")
        self.assertEqual([item["source"] for item in entity["evidence"]], ["company"])
        resolution = entity["resolution"]
        self.assertEqual(resolution["reviewer"], "alice")
        self.assertEqual(resolution["challenged"],
                         {"source": "campus", "record_id": "vacancy-2", "version": 1})
        dispute = self.board.disputes()["disputes"][0]
        self.assertEqual(dispute["state"], "resolved")
        self.assertEqual(dispute["active_review"]["reviewer"], "alice")
        history = self.delivery.history()["observations"]
        self.assertEqual(len(history), 2)
        self.audit_ok()

    def test_opposite_review_after_resolution_is_rejected(self):
        identity = self.conflict()
        self.board.submit(identity, "alice", "company/role-1/1", "campus/vacancy-2/1", self.basis())
        with self.assertRaises(HarvestError) as caught:
            self.board.submit(identity, "bob", "campus/vacancy-2/1", "company/role-1/1", self.basis())
        self.assertEqual(caught.exception.code, "review_conflict")
        reviews = self.board.reviews(identity=identity)["reviews"]
        self.assertEqual(len(reviews), 1)
        self.assertEqual(reviews[0]["reviewer"], "alice")
        self.audit_ok()

    def test_concurrent_reviewers_exactly_one_decision_stands(self):
        identity = self.conflict()
        barrier = threading.Barrier(2)
        outcomes = []

        def attempt(reviewer, select, challenge):
            ledger = Ledger(self.work.ledger.path)
            try:
                barrier.wait(timeout=10)
                try:
                    ReviewBoard(ledger).submit(identity, reviewer, select, challenge, self.basis())
                    outcomes.append("ok")
                except HarvestError as exc:
                    outcomes.append(exc.code)
            finally:
                ledger.close()

        threads = [threading.Thread(target=attempt,
                                    args=("alice", "company/role-1/1", "campus/vacancy-2/1")),
                   threading.Thread(target=attempt,
                                    args=("bob", "campus/vacancy-2/1", "company/role-1/1"))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        self.assertEqual(sorted(outcomes), ["ok", "review_conflict"])
        reviews = self.board.reviews(identity=identity)["reviews"]
        self.assertEqual(len(reviews), 1)
        self.assertEqual(reviews[0]["state"], "active")
        self.assertEqual(self.delivery.current()["counts"]["accepted"], 1)
        self.audit_ok()

    def test_page_retry_revision_invalidates_decision_and_stale_basis_fails(self):
        identity = self.conflict()
        self.board.submit(identity, "alice", "company/role-1/1", "campus/vacancy-2/1", self.basis())
        self.assertEqual(self.delivery.current()["accepted"][0]["record"]["quantity"], 4)
        path = manifest(self.work.path, key="campus", batch="batch-2",
                        pages=[[record(record_id="vacancy-2", quantity=7)]],
                        captured_at="2026-10-01T08:00:00+00:00")
        page_file = path.parent / "page-1.json"
        original = page_file.read_bytes()
        page_file.write_bytes(b'{"records":[]}')
        failed = self.work.harvester.ingest(path)
        self.assertEqual(failed["state"], "failed")
        self.assertIn("page_digest", failed["pages"][0]["error"])
        self.assertEqual(self.delivery.current()["counts"]["accepted"], 1)
        self.assertEqual(self.board.reviews(identity=identity)["reviews"][0]["state"], "active")
        page_file.write_bytes(original)
        recovered = self.work.harvester.page_retry(path, 1)
        self.assertEqual(recovered["state"], "complete")
        reviews = self.board.reviews(identity=identity)["reviews"]
        self.assertEqual(reviews[0]["state"], "invalidated")
        self.assertEqual(reviews[0]["invalid_reason"], "evidence_changed")
        self.assertIsNotNone(reviews[0]["invalidated_at"])
        current = self.delivery.current()
        self.assertEqual(current["counts"], {"accepted": 0, "conflicts": 1, "quarantined": 0})
        dispute = self.board.disputes()["disputes"][0]
        self.assertEqual(dispute["state"], "open")
        self.assertEqual(dispute["revision"], 2)
        campus_pin = next(claim for claim in dispute["claims"] if claim["source"] == "campus")
        self.assertEqual(campus_pin["version"], 2)
        self.assertEqual(campus_pin["status"], "accepted")
        with self.assertRaises(HarvestError) as caught:
            self.board.submit(identity, "bob", "campus/vacancy-2/1", "company/role-1/1", self.basis())
        self.assertEqual(caught.exception.code, "review_stale")
        result = self.board.submit(identity, "bob", "campus/vacancy-2/2", "company/role-1/1",
                                   self.basis(campus_version=2))
        self.assertEqual(result["state"], "resolved")
        current = self.delivery.current()
        self.assertEqual(current["counts"]["accepted"], 1)
        entity = current["accepted"][0]
        self.assertEqual(entity["record"]["quantity"], 7)
        self.assertEqual(entity["selected"]["source"], "campus")
        self.assertEqual(entity["resolution"]["reviewer"], "bob")
        self.assertEqual(self.delivery.summary()["by_kind"]["position"]["quantity"], 7)
        self.audit_ok()

    def test_matching_revision_closes_dispute_as_normal_merge(self):
        identity = self.conflict()
        self.board.submit(identity, "alice", "company/role-1/1", "campus/vacancy-2/1", self.basis())
        self.work.ingest(key="campus", batch="batch-2",
                         pages=[[record(record_id="vacancy-2", quantity=4)]],
                         captured_at="2026-10-01T08:00:00+00:00")
        current = self.delivery.current()
        self.assertEqual(current["counts"], {"accepted": 1, "conflicts": 0, "quarantined": 0})
        entity = current["accepted"][0]
        self.assertNotIn("resolution", entity)
        self.assertEqual(entity["selected"]["source"], "campus")
        self.assertEqual(entity["record"]["quantity"], 4)
        self.assertEqual(len(entity["evidence"]), 2)
        dispute = self.board.disputes(include_closed=True)["disputes"][0]
        self.assertEqual(dispute["state"], "closed")
        self.assertEqual(dispute["close_reason"], "merged")
        review = self.board.reviews(identity=identity)["reviews"][0]
        self.assertEqual(review["state"], "invalidated")
        self.assertEqual(review["invalid_reason"], "evidence_changed")
        self.audit_ok()

    def test_withdrawal_of_selected_source_releases_challenged_claim(self):
        identity = self.conflict()
        self.board.submit(identity, "alice", "company/role-1/1", "campus/vacancy-2/1", self.basis())
        self.work.harvester.withdraw("company", "来源撤回")
        current = self.delivery.current()
        self.assertEqual(current["counts"], {"accepted": 1, "conflicts": 0, "quarantined": 0})
        entity = current["accepted"][0]
        self.assertNotIn("resolution", entity)
        self.assertEqual(entity["selected"]["source"], "campus")
        self.assertEqual(entity["record"]["quantity"], 11)
        review = self.board.reviews(identity=identity)["reviews"][0]
        self.assertEqual(review["invalid_reason"], "source_withdrawn")
        dispute = self.board.disputes(include_closed=True)["disputes"][0]
        self.assertEqual(dispute["close_reason"], "source_withdrawn")
        self.audit_ok()

    def test_withdrawal_of_challenged_source_merges_normally(self):
        identity = self.conflict()
        self.board.submit(identity, "alice", "company/role-1/1", "campus/vacancy-2/1", self.basis())
        self.work.harvester.withdraw("campus", "镜像下线")
        current = self.delivery.current()
        self.assertEqual(current["counts"]["accepted"], 1)
        entity = current["accepted"][0]
        self.assertNotIn("resolution", entity)
        self.assertEqual(entity["selected"]["source"], "company")
        self.assertEqual(entity["record"]["quantity"], 4)
        review = self.board.reviews(identity=identity)["reviews"][0]
        self.assertEqual(review["invalid_reason"], "source_withdrawn")
        self.audit_ok()

    def test_review_submission_validates_targets_and_basis(self):
        identity = self.conflict()
        with self.assertRaises(HarvestError) as caught:
            self.board.submit("0" * 64, "alice", "company/role-1/1", "campus/vacancy-2/1", self.basis())
        self.assertEqual(caught.exception.code, "dispute_not_open")
        with self.assertRaises(HarvestError) as caught:
            self.board.submit(identity, "alice", "company/role-1/1", "company/role-1/1", self.basis())
        self.assertEqual(caught.exception.code, "review_target")
        with self.assertRaises(HarvestError) as caught:
            self.board.submit(identity, "alice", "company/role-1/9", "campus/vacancy-2/1", self.basis())
        self.assertEqual(caught.exception.code, "review_target")
        with self.assertRaises(HarvestError) as caught:
            self.board.submit(identity, "alice", "company/role-1/1", "campus/vacancy-2/1",
                              ["company/role-1/1"])
        self.assertEqual(caught.exception.code, "review_stale")
        with self.assertRaises(HarvestError) as caught:
            self.board.submit(identity, "alice", "company/role-1/1", "campus/vacancy-2/1",
                              ["company/role-1/1", "company/role-1/1"])
        self.assertEqual(caught.exception.code, "review_basis")
        self.assertEqual(self.board.disputes()["counts"]["open"], 1)
        self.audit_ok()

    def test_history_and_events_explain_why_decision_failed(self):
        identity = self.conflict()
        self.board.submit(identity, "alice", "company/role-1/1", "campus/vacancy-2/1", self.basis(),
                          note="暂时采用 company")
        self.work.ingest(key="campus", batch="batch-2",
                         pages=[[record(record_id="vacancy-2", quantity=7)]],
                         captured_at="2026-10-01T08:00:00+00:00")
        events = [event["event_type"] for event in self.work.ledger.events()]
        for expected in ("dispute_formed", "review_submitted", "review_invalidated", "dispute_updated"):
            self.assertIn(expected, events)
        invalidated = [event for event in self.work.ledger.events()
                       if event["event_type"] == "review_invalidated"][0]
        self.assertEqual(invalidated["payload"]["reason"], "evidence_changed")
        self.assertEqual(invalidated["payload"]["basis"],
                         [{"source": "campus", "record_id": "vacancy-2", "version": 1},
                          {"source": "company", "record_id": "role-1", "version": 1}])
        review = self.board.reviews(identity=identity)["reviews"][0]
        self.assertEqual(review["note"], "暂时采用 company")
        self.assertEqual(review["invalid_reason"], "evidence_changed")

    def test_cli_review_flow(self):
        database = self.work.path / "cli.sqlite3"
        for key in ("company", "campus"):
            declaration = self.work.path / f"declare-{key}.json"
            declaration.write_text(json.dumps(source(key=key)), encoding="utf-8")
            self.assertEqual(main(["--db", str(database), "register", str(declaration)]), 0)
        company_manifest = manifest(self.work.path, key="company", batch="cli-1")
        campus_manifest = manifest(self.work.path, key="campus", batch="cli-1",
                                   pages=[[record(record_id="vacancy-2", quantity=11)]])
        self.assertEqual(main(["--db", str(database), "ingest", str(company_manifest)]), 0)
        self.assertEqual(main(["--db", str(database), "ingest", str(campus_manifest)]), 0)
        disputes = self._cli_json(database, "disputes")
        self.assertEqual(disputes["counts"]["open"], 1)
        identity = disputes["disputes"][0]["identity"]
        args = ["--db", str(database), "review", identity, "--reviewer", "alice",
                "--select", "company/role-1/1", "--challenge", "campus/vacancy-2/1",
                "--basis", "company/role-1/1", "campus/vacancy-2/1", "--note", "采用 company"]
        self.assertEqual(main(args), 0)
        current = self._cli_json(database, "current")
        self.assertEqual(current["counts"]["accepted"], 1)
        self.assertEqual(current["accepted"][0]["resolution"]["reviewer"], "alice")
        stale = io.StringIO()
        with contextlib.redirect_stderr(stale):
            code = main(["--db", str(database), "review", identity, "--reviewer", "bob",
                         "--select", "campus/vacancy-2/1", "--challenge", "company/role-1/1",
                         "--basis", "company/role-1/1", "campus/vacancy-2/1"])
        self.assertEqual(code, 2)
        self.assertIn("review_conflict", stale.getvalue())
        reviews = self._cli_json(database, "reviews", "--identity", identity)
        self.assertEqual(len(reviews["reviews"]), 1)
        self.assertEqual(reviews["reviews"][0]["state"], "active")
        self.assertEqual(self._cli_json(database, "audit")["ok"], True)

    def _cli_json(self, database, *args):
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream):
            code = main(["--db", str(database), *args])
        self.assertEqual(code, 0)
        return json.loads(stream.getvalue())

    def test_cli_parser_supports_review_commands(self):
        parsed = parser().parse_args(["disputes", "--all"])
        self.assertEqual(parsed.action, "disputes")
        parsed = parser().parse_args(["review", "0" * 64, "--reviewer", "alice",
                                      "--select", "a/b/1", "--challenge", "c/d/2",
                                      "--basis", "a/b/1", "c/d/2"])
        self.assertEqual(parsed.action, "review")
        self.assertEqual(parsed.basis, ["a/b/1", "c/d/2"])
        parsed = parser().parse_args(["reviews"])
        self.assertEqual(parsed.action, "reviews")


if __name__ == "__main__":
    unittest.main()
