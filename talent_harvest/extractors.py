from __future__ import annotations

from csv import DictReader
from html.parser import HTMLParser
from io import StringIO
from typing import Any
import json

from .contracts import HarvestError, Source


class MirrorTable(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._table_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]):
        if tag == "table":
            self._table_depth += 1
        elif self._table_depth == 1 and tag == "tr":
            if self._row is not None:
                raise HarvestError("html_table", "nested rows are unsupported")
            self._row = []
        elif self._row is not None and tag in {"td", "th"}:
            if self._cell is not None:
                raise HarvestError("html_table", "nested cells are unsupported")
            self._cell = []

    def handle_data(self, data: str):
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str):
        if tag in {"td", "th"} and self._cell is not None:
            self._row.append("".join(self._cell).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._cell is not None:
                raise HarvestError("html_table", "cell not closed")
            if self._row:
                self.rows.append(self._row)
            self._row = None
        elif tag == "table" and self._table_depth:
            self._table_depth -= 1


def parse_json(text: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HarvestError("invalid_json", f"page JSON is invalid at {exc.pos}") from None
    if isinstance(payload, dict):
        payload = payload.get("records")
    if not isinstance(payload, list):
        raise HarvestError("json_records", "JSON page requires records array")
    if not all(isinstance(item, dict) for item in payload):
        raise HarvestError("json_records", "every JSON record must be object")
    return payload


def parse_csv(text: str) -> list[dict[str, Any]]:
    try:
        reader = DictReader(StringIO(text, newline=""), strict=True)
        headers = reader.fieldnames
        if not headers or any(not heading for heading in headers) or len(set(headers)) != len(headers):
            raise HarvestError("csv_headers", "CSV needs distinct nonempty headers")
        rows = list(reader)
    except Exception as exc:
        if isinstance(exc, HarvestError):
            raise
        raise HarvestError("csv_parse", "CSV page is malformed") from None
    if any(None in row for row in rows):
        raise HarvestError("csv_columns", "CSV row has more cells than headers")
    return rows


def parse_html(text: str) -> list[dict[str, Any]]:
    parser = MirrorTable()
    try:
        parser.feed(text)
        parser.close()
    except HarvestError:
        raise
    except Exception:
        raise HarvestError("html_parse", "HTML mirror cannot be parsed") from None
    if not parser.rows:
        raise HarvestError("html_table", "HTML mirror needs a table")
    headers, *rows = parser.rows
    if len(set(headers)) != len(headers) or not all(headers):
        raise HarvestError("html_headers", "HTML table headers must be unique")
    if any(len(row) != len(headers) for row in rows):
        raise HarvestError("html_columns", "HTML row width differs from header")
    return [dict(zip(headers, row)) for row in rows]


def extract(text: str, source: Source) -> list[dict[str, Any]]:
    if source.format == "json":
        records = parse_json(text)
    elif source.format == "csv":
        records = parse_csv(text)
    elif source.format == "html":
        records = parse_html(text)
    else:
        raise HarvestError("format", "unsupported source format")
    if len(records) > 10000:
        raise HarvestError("record_limit", "page exceeds record limit")
    return records
