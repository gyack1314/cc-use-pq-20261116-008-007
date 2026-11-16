from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import json

from .contracts import HarvestError, Manifest, Page


MAX_PAGE_BYTES = 2_000_000


@dataclass(frozen=True)
class MirrorPage:
    ordinal: int
    relative_path: str
    body: str
    digest: str
    byte_count: int


def read_manifest(path: Path) -> Manifest:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise HarvestError("manifest_read", str(exc)) from None
    return Manifest.parse(data)


def secure_page_path(base: Path, page: Page) -> Path:
    root = base.resolve()
    candidate = (root / page.path).resolve()
    if not candidate.is_relative_to(root) or candidate == root:
        raise HarvestError("page_path", "page escapes manifest directory")
    if candidate.is_symlink():
        raise HarvestError("page_path", "page must not be a symlink")
    return candidate


def read_page(base: Path, page: Page) -> MirrorPage:
    path = secure_page_path(base, page)
    try:
        size = path.stat().st_size
        if size > MAX_PAGE_BYTES:
            raise HarvestError("page_size", "page exceeds byte limit")
        payload = path.read_bytes()
    except OSError as exc:
        raise HarvestError("page_missing", f"{page.path}: {exc}") from None
    digest = sha256(payload).hexdigest()
    if digest != page.sha256:
        raise HarvestError("page_digest", f"{page.path}: expected {page.sha256}, got {digest}")
    try:
        body = payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HarvestError("page_encoding", f"{page.path} is not UTF-8") from None
    return MirrorPage(page.ordinal, page.path, body, digest, len(payload))


def manifest_digest(manifest: Manifest) -> str:
    return sha256(json.dumps(manifest.document(), sort_keys=True, ensure_ascii=False,
                             separators=(",", ":")).encode("utf-8")).hexdigest()


def find_page(manifest: Manifest, ordinal: int) -> Page:
    if type(ordinal) is not int or ordinal < 1 or ordinal > len(manifest.pages):
        raise HarvestError("page_ordinal", "page number outside manifest")
    return manifest.pages[ordinal - 1]
