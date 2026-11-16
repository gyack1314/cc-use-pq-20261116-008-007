from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from .contracts import HarvestError
from .ledger import Ledger


class Inventory:
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    def source_report(self, source: str) -> dict:
        row = self.ledger.db.execute("SELECT key,name,format,kind,trust,revision,withdrawn_at FROM sources WHERE key=?",
                                     (source,)).fetchone()
        if row is None:
            raise HarvestError("source_unknown", source)
        batches = self.ledger.db.execute("SELECT batch,captured_at,state,started_at,completed_at FROM batches WHERE source=? ORDER BY captured_at,batch",
                                         (source,)).fetchall()
        pages = self.ledger.db.execute("SELECT batch,ordinal,state,attempts,record_count,error_code FROM pages WHERE source=? ORDER BY batch,ordinal",
                                       (source,)).fetchall()
        observations = self.ledger.observations(source=source)
        stats = Counter(item["status"] for item in observations)
        most_recent: dict[str, dict] = {}
        for observation in observations:
            most_recent[observation["record_id"]] = observation
        current = Counter(item["status"] for item in most_recent.values())
        repeated_attempts = [dict(page) for page in pages if page["attempts"] > 1]
        return {"source": dict(row), "batches": [dict(batch) for batch in batches],
                "page_counts": dict(Counter(page["state"] for page in pages)),
                "observation_counts": dict(stats), "latest_record_counts": dict(current),
                "retries": repeated_attempts, "unique_record_ids": len(most_recent),
                "events": len(self.ledger.events(source=source))}

    def quality_report(self, *, source: str | None = None) -> dict:
        rows = self.ledger.observations(source=source)
        by_reason: Counter[str] = Counter()
        by_source: dict[str, Counter[str]] = defaultdict(Counter)
        by_kind: dict[str, Counter[str]] = defaultdict(Counter)
        examples: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            by_source[row["source"]][row["status"]] += 1
            by_kind[row["record"]["kind"]][row["status"]] += 1
            for reason in row["reasons"]:
                by_reason[reason] += 1
                if len(examples[reason]) < 5:
                    examples[reason].append({"source": row["source"], "batch": row["batch"],
                                             "page": row["ordinal"], "record_id": row["record_id"],
                                             "version": row["version"]})
        return {"source": source, "observations": len(rows),
                "by_reason": dict(sorted(by_reason.items())),
                "by_source": {key: dict(value) for key, value in sorted(by_source.items())},
                "by_kind": {key: dict(value) for key, value in sorted(by_kind.items())},
                "examples": dict(sorted(examples.items()))}

    def recovery_plan(self, *, source: str | None = None) -> dict:
        sql = "SELECT p.*,b.manifest_hash,b.state AS batch_state,s.withdrawn_at FROM pages p JOIN batches b ON b.source=p.source AND b.batch=p.batch JOIN sources s ON s.key=p.source WHERE p.state!='complete'"
        params: tuple = ()
        if source is not None:
            sql += " AND p.source=?"
            params = (source,)
        sql += " ORDER BY p.source,p.batch,p.ordinal"
        rows = self.ledger.db.execute(sql, params).fetchall()
        tasks = []
        for row in rows:
            action = "blocked_withdrawn" if row["withdrawn_at"] else "retry"
            tasks.append({"source": row["source"], "batch": row["batch"],
                          "page": row["ordinal"], "path": row["path"],
                          "expected_sha256": row["digest"], "state": row["state"],
                          "attempts": row["attempts"], "last_error": row["error_code"],
                          "manifest_hash": row["manifest_hash"], "action": action})
        return {"tasks": tasks, "retryable": sum(item["action"] == "retry" for item in tasks),
                "blocked": sum(item["action"] != "retry" for item in tasks)}

    def batch_comparison(self, source: str, left: str, right: str) -> dict:
        if left == right:
            raise HarvestError("batch_compare", "choose distinct batches")
        old = self.ledger.batch(source, left)
        new = self.ledger.batch(source, right)
        if old["state"] != "complete" or new["state"] != "complete":
            raise HarvestError("batch_compare", "both batches must be complete")
        rows = self.ledger.observations(source=source)
        before = {row["record_id"]: row for row in rows if row["batch"] == left}
        after = {row["record_id"]: row for row in rows if row["batch"] == right}
        added = sorted(set(after) - set(before))
        absent = sorted(set(before) - set(after))
        changed = []
        unchanged = []
        for record_id in sorted(set(before) & set(after)):
            a, b = before[record_id], after[record_id]
            if a["record"] == b["record"] and a["status"] == b["status"]:
                unchanged.append(record_id)
            else:
                changed.append({"record_id": record_id,
                                "before": {"version": a["version"], "record": a["record"],
                                           "status": a["status"], "reasons": a["reasons"]},
                                "after": {"version": b["version"], "record": b["record"],
                                          "status": b["status"], "reasons": b["reasons"]}})
        return {"source": source, "left": left, "right": right,
                "added": added, "absent": absent, "changed": changed,
                "unchanged": unchanged,
                "counts": {"added": len(added), "absent": len(absent),
                           "changed": len(changed), "unchanged": len(unchanged)}}

    def records_by_source(self) -> dict:
        sources = [row[0] for row in self.ledger.db.execute("SELECT key FROM sources ORDER BY key")]
        return {"sources": [self.source_report(source) for source in sources]}

    def stale_batches(self) -> dict:
        rows = self.ledger.db.execute("SELECT source,batch,captured_at,state FROM batches ORDER BY source,captured_at,batch")
        by_source: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            by_source[row["source"]].append(dict(row))
        result = []
        for source, batches in by_source.items():
            latest = max(batches, key=lambda item: (item["captured_at"], item["batch"]))
            unfinished = [batch for batch in batches if batch["state"] != "complete"]
            result.append({"source": source, "newest_batch": latest,
                           "unfinished": unfinished, "total_batches": len(batches)})
        return {"sources": result}
