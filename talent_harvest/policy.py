from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from .contracts import Record, Source


@dataclass(frozen=True)
class Decision:
    status: str
    reasons: tuple[str, ...]

    def document(self) -> dict:
        return {"status": self.status, "reasons": list(self.reasons)}


def assess(record: Record, source: Source, captured_at: str) -> Decision:
    reasons: list[str] = []
    if record.quantity == 0:
        reasons.append("zero_quantity")
    if record.kind == "partnership" and record.reference.startswith(source.key + ":"):
        reasons.append("project_reference_missing")
    if source.trust < 20:
        reasons.append("low_trust_source")
    if date.fromisoformat(record.published_at) > date.fromisoformat(captured_at[:10]):
        reasons.append("future_publication")
    if len(record.description) > 1800:
        reasons.append("description_length")
    return Decision("quarantined" if reasons else "accepted", tuple(reasons))


def batch_state(page_states: list[str]) -> str:
    if not page_states:
        return "empty"
    if all(state == "complete" for state in page_states):
        return "complete"
    if any(state == "failed" for state in page_states):
        return "failed"
    return "pending"


def exposure(status: str, withdrawn: bool) -> bool:
    return status == "accepted" and not withdrawn
