from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable
import json

from .contracts import HarvestError, Record, canonical, identifier, required
from .reconcile import describe_conflict

if TYPE_CHECKING:
    from .ledger import Ledger


REVIEW_SCHEMA = """
CREATE TABLE IF NOT EXISTS disputes (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 identity TEXT NOT NULL, reason TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'open', revision INTEGER NOT NULL DEFAULT 1,
 formed_at TEXT NOT NULL, closed_at TEXT, close_reason TEXT
);
CREATE INDEX IF NOT EXISTS disputes_identity ON disputes(identity,state);
CREATE TABLE IF NOT EXISTS dispute_claims (
 dispute_id INTEGER NOT NULL REFERENCES disputes(id),
 observation_id INTEGER NOT NULL REFERENCES observations(id),
 source TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL,
 batch TEXT NOT NULL, page INTEGER NOT NULL, page_digest TEXT NOT NULL,
 status TEXT NOT NULL, reasons TEXT NOT NULL, trust INTEGER NOT NULL,
 captured_at TEXT NOT NULL, raw_digest TEXT NOT NULL,
 PRIMARY KEY(dispute_id,source,record_id)
);
CREATE TABLE IF NOT EXISTS reviews (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 dispute_id INTEGER NOT NULL REFERENCES disputes(id),
 identity TEXT NOT NULL, reviewer TEXT NOT NULL,
 selected_source TEXT NOT NULL, selected_record_id TEXT NOT NULL, selected_version INTEGER NOT NULL,
 challenged_source TEXT NOT NULL, challenged_record_id TEXT NOT NULL, challenged_version INTEGER NOT NULL,
 basis TEXT NOT NULL, note TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'active',
 invalid_reason TEXT, submitted_at TEXT NOT NULL, invalidated_at TEXT
);
CREATE INDEX IF NOT EXISTS reviews_dispute ON reviews(dispute_id,state);
CREATE TABLE IF NOT EXISTS claim_marks (
 review_id INTEGER NOT NULL REFERENCES reviews(id),
 source TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL,
 PRIMARY KEY(review_id)
);
"""

OPEN_STATES = ("open", "resolved")
INVALID_REASONS = ("evidence_changed", "source_withdrawn")
CLOSE_REASONS = ("merged", "source_withdrawn")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emit(db, kind: str, payload: dict) -> None:
    db.execute("INSERT INTO events(event_type,source,batch,ordinal,payload,occurred_at) VALUES(?,?,?,?,?,?)",
               (kind, None, None, None, canonical(payload), _now()))


def fingerprint(rows: Iterable) -> list[dict]:
    claims = [{"source": row["source"], "record_id": row["record_id"], "version": row["version"]}
              for row in rows]
    return sorted(claims, key=lambda item: (item["source"], item["record_id"]))


def _claim_key(claim: dict) -> tuple[str, str, int]:
    return claim["source"], claim["record_id"], claim["version"]


def _group_rows(db, identity: str) -> list:
    return db.execute(
        "SELECT o.*,s.trust,p.digest AS page_digest FROM observations o"
        " JOIN sources s ON s.key=o.source"
        " JOIN pages p ON p.source=o.source AND p.batch=o.batch AND p.ordinal=o.ordinal"
        " WHERE o.identity=? AND s.withdrawn_at IS NULL"
        " AND o.id=(SELECT max(i.id) FROM observations i"
        "           WHERE i.identity=o.identity AND i.source=o.source AND i.record_id=o.record_id)"
        " ORDER BY o.source,o.record_id", (identity,)).fetchall()


def evidence_fingerprint(db, identity: str) -> list[dict]:
    return fingerprint(_group_rows(db, identity))


def active_marks(db) -> set[tuple[str, str, int]]:
    rows = db.execute(
        "SELECT m.source,m.record_id,m.version FROM claim_marks m"
        " JOIN reviews r ON r.id=m.review_id WHERE r.state='active'").fetchall()
    return {(row["source"], row["record_id"], row["version"]) for row in rows}


def active_resolutions(db) -> dict[str, dict]:
    rows = db.execute("SELECT * FROM reviews WHERE state='active' ORDER BY id").fetchall()
    return {row["identity"]: dict(row) for row in rows}


def _pin(db, dispute_id: int, rows: list) -> None:
    db.execute("DELETE FROM dispute_claims WHERE dispute_id=?", (dispute_id,))
    db.executemany(
        "INSERT INTO dispute_claims(dispute_id,observation_id,source,record_id,version,batch,page,"
        "page_digest,status,reasons,trust,captured_at,raw_digest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(dispute_id, row["id"], row["source"], row["record_id"], row["version"], row["batch"],
          row["ordinal"], row["page_digest"], row["status"], row["reasons"], row["trust"],
          row["captured_at"], row["raw_digest"]) for row in rows])


def sync_identities(db, identities: Iterable[str]) -> None:
    for identity in sorted(set(identities)):
        _sync_identity(db, identity)


def _sync_identity(db, identity: str) -> None:
    rows = _group_rows(db, identity)
    evidence = fingerprint(rows)
    dispute = db.execute(
        "SELECT * FROM disputes WHERE identity=? AND state IN ('open','resolved')"
        " ORDER BY id DESC LIMIT 1", (identity,)).fetchone()
    review = None
    if dispute is not None:
        review = db.execute(
            "SELECT * FROM reviews WHERE dispute_id=? AND state='active'"
            " ORDER BY id DESC LIMIT 1", (dispute["id"],)).fetchone()
    if review is not None:
        basis = json.loads(review["basis"])
        if basis != evidence:
            missing = {item["source"] for item in basis} - {item["source"] for item in evidence}
            reason = "source_withdrawn" if missing else "evidence_changed"
            db.execute("UPDATE reviews SET state='invalidated',invalid_reason=?,invalidated_at=? WHERE id=?",
                       (reason, _now(), review["id"]))
            _emit(db, "review_invalidated",
                  {"review_id": review["id"], "dispute_id": dispute["id"], "identity": identity,
                   "reason": reason, "basis": basis, "evidence": evidence})
        else:
            if dispute["state"] != "resolved":
                db.execute("UPDATE disputes SET state='resolved' WHERE id=?", (dispute["id"],))
            return
    accepted = [row for row in rows if row["status"] == "accepted"]
    conflict = ""
    if len(accepted) >= 2:
        conflict = describe_conflict([Record(**json.loads(row["record_json"])) for row in accepted])
    if conflict:
        if dispute is None:
            cursor = db.execute(
                "INSERT INTO disputes(identity,reason,state,revision,formed_at) VALUES(?,?,?,?,?)",
                (identity, conflict, "open", 1, _now()))
            _pin(db, cursor.lastrowid, rows)
            _emit(db, "dispute_formed",
                  {"dispute_id": cursor.lastrowid, "identity": identity,
                   "reason": conflict, "claims": evidence})
            return
        pins = db.execute("SELECT source,record_id,version FROM dispute_claims WHERE dispute_id=?",
                          (dispute["id"],)).fetchall()
        repinned = fingerprint(pins) != evidence
        if repinned:
            _pin(db, dispute["id"], rows)
        revision = dispute["revision"] + 1 if repinned else dispute["revision"]
        if dispute["state"] != "open" or dispute["reason"] != conflict or repinned:
            db.execute("UPDATE disputes SET state='open',reason=?,revision=? WHERE id=?",
                       (conflict, revision, dispute["id"]))
            _emit(db, "dispute_updated",
                  {"dispute_id": dispute["id"], "identity": identity, "reason": conflict,
                   "state": "open", "revision": revision, "claims": evidence})
        return
    if dispute is not None:
        pinned = {row[0] for row in db.execute(
            "SELECT DISTINCT source FROM dispute_claims WHERE dispute_id=?", (dispute["id"],)).fetchall()}
        current = {item["source"] for item in evidence}
        close_reason = "source_withdrawn" if pinned - current else "merged"
        db.execute("UPDATE disputes SET state='closed',closed_at=?,close_reason=? WHERE id=?",
                   (_now(), close_reason, dispute["id"]))
        _emit(db, "dispute_closed",
              {"dispute_id": dispute["id"], "identity": identity, "reason": close_reason})


def parse_triple(value: Any, field: str) -> dict:
    if isinstance(value, str):
        parts = value.split("/")
        if len(parts) != 3:
            raise HarvestError("review_target", f"{field} must be source/record_id/version")
        value = {"source": parts[0], "record_id": parts[1], "version": parts[2]}
    if not isinstance(value, dict):
        raise HarvestError("review_target", f"{field} must be source/record_id/version")
    version = value.get("version")
    if isinstance(version, str) and version.isdigit():
        version = int(version)
    if type(version) is not int or version < 1:
        raise HarvestError("review_target", f"{field} version must be a positive integer")
    return {"source": identifier(value.get("source"), "source"),
            "record_id": identifier(value.get("record_id"), "record_id"),
            "version": version}


def _basis_of(basis: Any) -> list[dict]:
    if not isinstance(basis, (list, tuple)) or not basis:
        raise HarvestError("review_basis", "basis must list the evidence versions reviewed")
    claims = [parse_triple(item, "basis") for item in basis]
    if len({_claim_key(claim) for claim in claims}) != len(claims):
        raise HarvestError("review_basis", "basis repeats a claim")
    return sorted(claims, key=lambda item: (item["source"], item["record_id"]))


def _review_document(row) -> dict:
    return {"review_id": row["id"], "dispute_id": row["dispute_id"], "identity": row["identity"],
            "reviewer": row["reviewer"],
            "selected": {"source": row["selected_source"], "record_id": row["selected_record_id"],
                         "version": row["selected_version"]},
            "challenged": {"source": row["challenged_source"], "record_id": row["challenged_record_id"],
                           "version": row["challenged_version"]},
            "basis": json.loads(row["basis"]), "note": row["note"], "state": row["state"],
            "invalid_reason": row["invalid_reason"], "submitted_at": row["submitted_at"],
            "invalidated_at": row["invalidated_at"]}


class ReviewBoard:
    def __init__(self, ledger: "Ledger"):
        self.ledger = ledger

    def submit(self, identity: str, reviewer: str, select: Any, challenge: Any,
               basis: Any, note: str = "") -> dict:
        if not isinstance(identity, str) or len(identity) != 64 \
                or any(char not in "0123456789abcdef" for char in identity):
            raise HarvestError("identity", "identity must be sha256 hex")
        reviewer = required(reviewer, "reviewer", 80)
        if not isinstance(note, str) or len(note.strip()) > 500:
            raise HarvestError("review_note", "note must be at most 500 characters")
        chosen = parse_triple(select, "select")
        doubted = parse_triple(challenge, "challenge")
        if chosen == doubted:
            raise HarvestError("review_target", "select and challenge must be different claims")
        evidence = _basis_of(basis)
        with self.ledger.transaction():
            db = self.ledger.db
            dispute = db.execute(
                "SELECT * FROM disputes WHERE identity=? AND state IN ('open','resolved')"
                " ORDER BY id DESC LIMIT 1", (identity,)).fetchone()
            if dispute is None:
                raise HarvestError("dispute_not_open", "no open dispute for identity")
            if dispute["state"] != "open":
                raise HarvestError("review_conflict", "dispute already resolved by an active review")
            pins = db.execute("SELECT * FROM dispute_claims WHERE dispute_id=? ORDER BY source,record_id",
                              (dispute["id"],)).fetchall()
            if fingerprint(pins) != evidence:
                raise HarvestError("review_stale",
                                   "evidence versions changed; reload the dispute and review again")
            accepted = {_claim_key(pin) for pin in pins if pin["status"] == "accepted"}
            if _claim_key(chosen) not in accepted or _claim_key(doubted) not in accepted:
                raise HarvestError("review_target",
                                   "select and challenge must reference accepted pinned claims")
            now = _now()
            cursor = db.execute(
                "INSERT INTO reviews(dispute_id,identity,reviewer,selected_source,selected_record_id,"
                "selected_version,challenged_source,challenged_record_id,challenged_version,basis,note,"
                "state,submitted_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (dispute["id"], identity, reviewer, chosen["source"], chosen["record_id"],
                 chosen["version"], doubted["source"], doubted["record_id"], doubted["version"],
                 canonical(evidence), note.strip(), "active", now))
            review_id = cursor.lastrowid
            db.execute("INSERT INTO claim_marks(review_id,source,record_id,version) VALUES(?,?,?,?)",
                       (review_id, doubted["source"], doubted["record_id"], doubted["version"]))
            db.execute("UPDATE disputes SET state='resolved' WHERE id=?", (dispute["id"],))
            self.ledger.event("review_submitted", None, None, None,
                              {"review_id": review_id, "dispute_id": dispute["id"], "identity": identity,
                               "reviewer": reviewer, "selected": chosen, "challenged": doubted})
        return {"review_id": review_id, "dispute_id": dispute["id"], "identity": identity,
                "state": "resolved", "reviewer": reviewer, "selected": chosen, "challenged": doubted}

    def disputes(self, *, include_closed: bool = False) -> dict:
        db = self.ledger.db
        states = ("open", "resolved", "closed") if include_closed else OPEN_STATES
        marks = ",".join("?" for _ in states)
        rows = db.execute(f"SELECT * FROM disputes WHERE state IN ({marks}) ORDER BY id", states).fetchall()
        totals = {row["state"]: row["total"] for row in
                  db.execute("SELECT state,count(*) AS total FROM disputes GROUP BY state").fetchall()}
        result = []
        for row in rows:
            claims = db.execute("SELECT * FROM dispute_claims WHERE dispute_id=? ORDER BY source,record_id",
                                (row["id"],)).fetchall()
            active = db.execute("SELECT * FROM reviews WHERE dispute_id=? AND state='active'",
                                (row["id"],)).fetchone()
            result.append({"dispute_id": row["id"], "identity": row["identity"], "reason": row["reason"],
                           "state": row["state"], "revision": row["revision"],
                           "formed_at": row["formed_at"], "closed_at": row["closed_at"],
                           "close_reason": row["close_reason"],
                           "claims": [{"source": claim["source"], "record_id": claim["record_id"],
                                       "version": claim["version"], "status": claim["status"],
                                       "reasons": json.loads(claim["reasons"]), "trust": claim["trust"],
                                       "batch": claim["batch"], "page": claim["page"],
                                       "page_digest": claim["page_digest"], "raw_digest": claim["raw_digest"],
                                       "captured_at": claim["captured_at"],
                                       "observation_id": claim["observation_id"]} for claim in claims],
                           "evidence": fingerprint(claims),
                           "active_review": _review_document(active) if active else None})
        return {"disputes": result,
                "counts": {"open": totals.get("open", 0), "resolved": totals.get("resolved", 0),
                           "closed": totals.get("closed", 0)}}

    def reviews(self, *, identity: str | None = None) -> dict:
        if identity is None:
            rows = self.ledger.db.execute("SELECT * FROM reviews ORDER BY id").fetchall()
        else:
            rows = self.ledger.db.execute("SELECT * FROM reviews WHERE identity=? ORDER BY id",
                                          (identity,)).fetchall()
        return {"reviews": [_review_document(row) for row in rows]}
