from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
import json
import sqlite3

from .contracts import HarvestError, Manifest, Source, canonical
from .intake import read_manifest, read_page
from .ledger import Ledger
from .extractors import extract
from .normalize import normalize_page
from .policy import assess


class Operations:
    def __init__(self, ledger: Ledger):
        self.ledger = ledger

    def preview(self, source_path: str | Path, manifest_path: str | Path) -> dict:
        try:
            source = Source.parse(json.loads(Path(source_path).read_text(encoding="utf-8")))
        except (OSError, UnicodeError, ValueError) as exc:
            if isinstance(exc, HarvestError):
                raise
            raise HarvestError("source_read", str(exc)) from None
        manifest_file = Path(manifest_path)
        manifest = read_manifest(manifest_file)
        if manifest.source != source.key:
            raise HarvestError("source_mismatch", "manifest source differs from declaration")
        result = []
        for page in manifest.pages:
            try:
                mirror = read_page(manifest_file.parent, page)
                records = normalize_page(source, extract(mirror.body, source))
                decisions = [assess(record, source, manifest.captured_at) for record in records]
                result.append({"ordinal": page.ordinal, "state": "valid", "bytes": mirror.byte_count,
                               "records": len(records),
                               "accepted": sum(item.status == "accepted" for item in decisions),
                               "quarantined": sum(item.status == "quarantined" for item in decisions),
                               "reasons": dict(Counter(reason for decision in decisions
                                                       for reason in decision.reasons))})
            except HarvestError as exc:
                result.append({"ordinal": page.ordinal, "state": "invalid", "code": exc.code,
                               "detail": exc.detail})
        return {"source": source.key, "batch": manifest.batch, "pages": result,
                "valid_pages": sum(item["state"] == "valid" for item in result),
                "invalid_pages": sum(item["state"] == "invalid" for item in result)}

    def replay_check(self, manifest_path: str | Path) -> dict:
        manifest_file = Path(manifest_path)
        manifest = read_manifest(manifest_file)
        batch = self.ledger.batch(manifest.source, manifest.batch)
        if batch["state"] != "complete":
            raise HarvestError("replay_incomplete", "batch must be complete")
        source = self.ledger.source(manifest.source)
        differences = []
        for page in manifest.pages:
            location = f"{manifest.source}/{manifest.batch}/{page.ordinal}"
            try:
                mirror = read_page(manifest_file.parent, page)
                raw_rows = extract(mirror.body, source)
                records = normalize_page(source, raw_rows)
            except HarvestError as exc:
                differences.append({"page": page.ordinal, "code": exc.code, "detail": exc.detail})
                continue
            stored = self.ledger.db.execute("SELECT record_id,raw_digest FROM observations WHERE source=? AND batch=? AND ordinal=? ORDER BY id",
                                            (manifest.source, manifest.batch, page.ordinal)).fetchall()
            expected = [(record.source_record_id, record.raw_digest) for record in records]
            actual = [(row["record_id"], row["raw_digest"]) for row in stored]
            if actual != expected:
                differences.append({"page": page.ordinal, "code": "observation_mismatch",
                                    "stored": actual, "mirror": expected})
        return {"source": manifest.source, "batch": manifest.batch,
                "consistent": not differences, "differences": differences}

    def event_window(self, start: int = 0, limit: int = 100) -> dict:
        if start < 0 or limit < 1 or limit > 1000:
            raise HarvestError("event_window", "start must be nonnegative and limit 1-1000")
        rows = self.ledger.db.execute("SELECT * FROM events WHERE id>? ORDER BY id LIMIT ?", (start, limit)).fetchall()
        events = [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]
        return {"events": events, "next_event_id": events[-1]["id"] if events else start,
                "has_more": bool(events and self.ledger.db.execute("SELECT 1 FROM events WHERE id>? LIMIT 1",
                                                                 (events[-1]["id"],)).fetchone())}

    def record_window(self, start: int = 0, limit: int = 100) -> dict:
        if start < 0 or limit < 1 or limit > 1000:
            raise HarvestError("record_window", "start must be nonnegative and limit 1-1000")
        rows = self.ledger.db.execute("SELECT o.*,s.withdrawn_at FROM observations o JOIN sources s ON s.key=o.source WHERE o.id>? ORDER BY o.id LIMIT ?",
                                      (start, limit)).fetchall()
        items = []
        for row in rows:
            items.append({"observation_id": row["id"], "source": row["source"],
                          "batch": row["batch"], "page": row["ordinal"],
                          "record_id": row["record_id"], "identity": row["identity"],
                          "version": row["version"], "status": row["status"],
                          "reasons": json.loads(row["reasons"]),
                          "record": json.loads(row["record_json"]),
                          "withdrawn_at": row["withdrawn_at"]})
        return {"observations": items, "next_observation_id": items[-1]["observation_id"] if items else start,
                "has_more": bool(items and self.ledger.db.execute("SELECT 1 FROM observations WHERE id>? LIMIT 1",
                                                                 (items[-1]["observation_id"],)).fetchone())}

    def consistency_report(self) -> dict:
        rows = self.ledger.observations()
        by_identity: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            by_identity[row["identity"]].append(row)
        histories = []
        for identity, observations in sorted(by_identity.items()):
            sources = sorted({item["source"] for item in observations})
            latest: dict[tuple[str, str], dict] = {}
            for row in observations:
                latest[row["source"], row["record_id"]] = row
            active = [item for item in latest.values() if item["status"] == "accepted"
                      and not item["withdrawn_at"]]
            histories.append({"identity": identity, "sources": sources,
                              "observations": len(observations), "active_claims": len(active),
                              "quantities": sorted({item["record"]["quantity"] for item in active}),
                              "disciplines": sorted({item["record"]["discipline"] for item in active}),
                              "withdrawn_sources": sorted({item["source"] for item in latest.values()
                                                           if item["withdrawn_at"]})})
        return {"entities": histories,
                "multi_source": sum(len(item["sources"]) > 1 for item in histories),
                "quantity_disputes": sum(len(item["quantities"]) > 1 for item in histories)}

    def database_info(self) -> dict:
        page_count = self.ledger.db.execute("PRAGMA page_count").fetchone()[0]
        page_size = self.ledger.db.execute("PRAGMA page_size").fetchone()[0]
        journal_mode = self.ledger.db.execute("PRAGMA journal_mode").fetchone()[0]
        integrity = self.ledger.db.execute("PRAGMA quick_check").fetchone()[0]
        return {"path": str(self.ledger.path), "page_count": page_count,
                "page_size": page_size, "estimated_bytes": page_count * page_size,
                "journal_mode": journal_mode, "sqlite_integrity": integrity}

    def checkpoint(self) -> dict:
        state = self.database_info()
        if state["sqlite_integrity"] != "ok":
            raise HarvestError("database_integrity", state["sqlite_integrity"])
        maximum = self.ledger.db.execute("SELECT coalesce(max(id),0) FROM observations").fetchone()[0]
        event_maximum = self.ledger.db.execute("SELECT coalesce(max(id),0) FROM events").fetchone()[0]
        batches = self.ledger.db.execute("SELECT source,batch,state,manifest_hash FROM batches ORDER BY source,batch").fetchall()
        payload = {"observation_cursor": maximum, "event_cursor": event_maximum,
                   "batches": [dict(batch) for batch in batches]}
        return {"state": payload, "sha256": sha256(canonical(payload).encode("utf-8")).hexdigest()}
