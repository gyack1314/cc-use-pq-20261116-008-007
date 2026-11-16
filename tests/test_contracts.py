from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from talent_harvest.contracts import HarvestError, Manifest, Page, Source, content_digest, instant
from talent_harvest.extractors import extract, parse_csv, parse_html, parse_json
from talent_harvest.intake import read_manifest, read_page, secure_page_path
from talent_harvest.normalize import normalize, normalize_page, quantity, published, search_key
from tests.support import manifest, record, source


class ContractTests(unittest.TestCase):
    def test_source_accepts_business_declaration(self):
        parsed = Source.parse(source())
        self.assertEqual(parsed.key, "company")
        self.assertEqual(parsed.kind, "position")
        self.assertEqual(parsed.trust, 70)
        self.assertEqual(parsed.document()["field_map"], {})

    def test_source_rejects_invalid_keys(self):
        for key in ("", "UPPER", "x", "has space", "../bad", 3):
            with self.subTest(key=key), self.assertRaises(HarvestError):
                Source.parse(source(key=key))

    def test_source_rejects_invalid_types(self):
        for field, value in (("format", "xml"), ("kind", "person"), ("trust", True),
                             ("trust", -1), ("trust", 101), ("name", "")):
            declaration = source()
            declaration[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(HarvestError):
                Source.parse(declaration)

    def test_field_map_rejects_unknown_and_duplicate_targets(self):
        for mapping in ({"unknown": "a"}, {"title": "x", "region": "x"},
                        {"title": 3}, {"title": ""}):
            with self.subTest(mapping=mapping), self.assertRaises(HarvestError):
                Source.parse(source(field_map=mapping))

    def test_manifest_rejects_gap_and_wrong_order(self):
        good = {"source": "company", "batch": "batch-1", "captured_at": "2026-09-30T00:00:00Z",
                "pages": [{"ordinal": 1, "path": "a.json", "sha256": "a" * 64},
                          {"ordinal": 2, "path": "b.json", "sha256": "b" * 64}]}
        self.assertEqual(len(Manifest.parse(good).pages), 2)
        for ordinals in ((2, 1), (1, 3), (1, 1)):
            broken = {**good, "pages": [{**page, "ordinal": ordinal}
                                       for page, ordinal in zip(good["pages"], ordinals)]}
            with self.subTest(ordinals=ordinals), self.assertRaises(HarvestError):
                Manifest.parse(broken)

    def test_manifest_rejects_reused_path(self):
        with self.assertRaises(HarvestError) as caught:
            Manifest.parse({"source": "company", "batch": "batch-1",
                            "captured_at": "2026-09-30T00:00:00Z",
                            "pages": [{"ordinal": 1, "path": "a", "sha256": "a" * 64},
                                      {"ordinal": 2, "path": "a", "sha256": "b" * 64}]})
        self.assertEqual(caught.exception.code, "page_path")

    def test_page_path_rejects_escape(self):
        for path in ("../x", "C:\\x", "a\\b", "", "x:y"):
            with self.subTest(path=path), self.assertRaises(HarvestError):
                Page.parse({"ordinal": 1, "path": path, "sha256": "a" * 64})

    def test_timestamp_requires_timezone(self):
        self.assertEqual(instant("2026-09-30T00:00:00Z", "captured_at"),
                         "2026-09-30T00:00:00+00:00")
        for stamp in ("2026-09-30", "2026-09-30T00:00:00", "not a time", None):
            with self.subTest(stamp=stamp), self.assertRaises(HarvestError):
                instant(stamp, "captured_at")

    def test_read_manifest_and_page_digest(self):
        with TemporaryDirectory() as directory:
            path = manifest(Path(directory))
            parsed = read_manifest(path)
            page = read_page(path.parent, parsed.pages[0])
            self.assertEqual(page.ordinal, 1)
            self.assertIn("AI Engineer", page.body)
            self.assertEqual(page.digest, parsed.pages[0].sha256)

    def test_page_digest_detects_tampering(self):
        with TemporaryDirectory() as directory:
            path = manifest(Path(directory))
            (path.parent / "page-1.json").write_text("tampered", encoding="utf-8")
            with self.assertRaises(HarvestError) as caught:
                read_page(path.parent, read_manifest(path).pages[0])
            self.assertEqual(caught.exception.code, "page_digest")

    def test_missing_page_is_a_recoverable_error(self):
        with TemporaryDirectory() as directory:
            path = manifest(Path(directory))
            (path.parent / "page-1.json").unlink()
            with self.assertRaises(HarvestError) as caught:
                read_page(path.parent, read_manifest(path).pages[0])
            self.assertEqual(caught.exception.code, "page_missing")

    def test_json_page_requires_record_objects(self):
        self.assertEqual(parse_json('{"records":[{"x":1}]}'), [{"x": 1}])
        for payload in ("{}", "[1]", "broken", '{"records":{}}'):
            with self.subTest(payload=payload), self.assertRaises(HarvestError):
                parse_json(payload)

    def test_csv_rejects_duplicate_headers_and_extra_columns(self):
        self.assertEqual(parse_csv("x,y\n1,2\n"), [{"x": "1", "y": "2"}])
        for body in ("x,x\n1,2\n", "x,y\n1,2,3\n", "\n"):
            with self.subTest(body=body), self.assertRaises(HarvestError):
                parse_csv(body)

    def test_html_table_extracts_rows(self):
        body = "<table><tr><th>x</th><th>y</th></tr><tr><td>A</td><td>B</td></tr></table>"
        self.assertEqual(parse_html(body), [{"x": "A", "y": "B"}])
        for invalid in ("<div>none</div>", "<table><tr><th>x</th><th>x</th></tr></table>",
                        "<table><tr><th>x</th><th>y</th></tr><tr><td>1</td></tr></table>"):
            with self.subTest(invalid=invalid), self.assertRaises(HarvestError):
                parse_html(invalid)

    def test_normalization_canonicalizes_names_and_count(self):
        raw = record(organization="  Ｎｏｒｔｈ   University  ", quantity="1,234")
        parsed = normalize(Source.parse(source()), raw)
        self.assertEqual(parsed.organization, "North University")
        self.assertEqual(parsed.quantity, 1234)
        self.assertEqual(parsed.published_at, "2026-09-01")
        self.assertEqual(parsed.raw_digest, content_digest(raw))

    def test_quantity_rejects_invalid_values(self):
        self.assertEqual(quantity("0"), 0)
        self.assertEqual(quantity("1,000"), 1000)
        for value in (-1, True, None, "1.0", "x", 1_000_001):
            with self.subTest(value=value), self.assertRaises(HarvestError):
                quantity(value)

    def test_published_date_is_strict(self):
        self.assertEqual(published("2026-09-01"), "2026-09-01")
        for value in ("2026-13-01", "2026-02-30", "09/01/2026", 20260901):
            with self.subTest(value=value), self.assertRaises(HarvestError):
                published(value)

    def test_page_record_id_must_be_unique(self):
        with self.assertRaises(HarvestError) as caught:
            normalize_page(Source.parse(source()), [record(), record(quantity=9)])
        self.assertEqual(caught.exception.code, "duplicate_page_id")

    def test_field_mapping_and_identity(self):
        declaration = Source.parse(source(field_map={"record_id": "id", "organization": "org"}))
        mapped = record()
        mapped["id"] = mapped.pop("record_id")
        mapped["org"] = mapped.pop("organization")
        first = normalize(declaration, mapped)
        second = normalize(Source.parse(source()), record(organization="north university"))
        self.assertEqual(first.identity, second.identity)
        self.assertEqual(first.source_record_id, "role-1")

    def test_identity_distinguishes_kind_and_region(self):
        base = normalize(Source.parse(source()), record())
        other_kind = normalize(Source.parse(source(kind="program")), record())
        other_region = normalize(Source.parse(source()), record(region="Beijing"))
        self.assertNotEqual(base.identity, other_kind.identity)
        self.assertNotEqual(base.identity, other_region.identity)
