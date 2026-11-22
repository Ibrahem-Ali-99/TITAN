"""Bounded CSV conversion into canonical events. Rows are not written as business facts."""

import csv
import io
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from pydantic import ValidationError

from titan_contracts.events import CommerceEvent
from titan_contracts.money import line_amount, parse_money
from titan_contracts.parsing import CommerceParseError

MAX_IMPORT_ROWS = 500
MAX_IMPORT_BYTES = 1_048_576

CSV_HEADERS = (
    "event_type",
    "occurred_at",
    "order_id",
    "customer_id",
    "currency",
    "product_id",
    "quantity",
    "unit_price",
    "payment_id",
    "amount",
    "payment_method",
    "quantity_on_hand",
    "inventory_delta",
    "warehouse_id",
)


@dataclass(frozen=True)
class ParsedCsvRow:
    row_number: int
    event: CommerceEvent | None
    error: str | None


class CsvFileError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def parse_csv_bytes(raw: bytes, *, source: str, produced_at: datetime) -> list[ParsedCsvRow]:
    if len(raw) > MAX_IMPORT_BYTES:
        raise CsvFileError(413, f"CSV imports are limited to {MAX_IMPORT_BYTES} bytes")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvFileError(422, "CSV must be UTF-8") from exc
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise CsvFileError(422, "CSV header row is missing")
    headers = tuple(name.strip() for name in reader.fieldnames)
    if headers != CSV_HEADERS:
        raise CsvFileError(422, "CSV headers must match the documented column list exactly")

    parsed: list[ParsedCsvRow] = []
    for file_row, raw_row in enumerate(reader, start=2):
        if raw_row is None or all(not (value or "").strip() for value in raw_row.values()):
            continue
        if len(parsed) >= MAX_IMPORT_ROWS:
            raise CsvFileError(413, f"CSV imports are limited to {MAX_IMPORT_ROWS} data rows")
        try:
            event = _convert_row(raw_row, source=source, produced_at=produced_at)
        except (ValidationError, ValueError, CommerceParseError) as exc:
            parsed.append(ParsedCsvRow(file_row, None, _error_text(exc)))
        else:
            parsed.append(ParsedCsvRow(file_row, event, None))
    if not parsed:
        raise CsvFileError(422, "CSV does not contain any data rows")
    return parsed


def _convert_row(
    row: dict[str, str | None], *, source: str, produced_at: datetime
) -> CommerceEvent:
    event_type = _cell(row, "event_type")
    occurred_at = _cell(row, "occurred_at")
    event_id = str(uuid4())
    envelope: dict[str, object] = {
        "event_id": event_id,
        "event_type": event_type,
        "schema_version": 1,
        "occurred_at": occurred_at,
        "produced_at": produced_at.isoformat(),
        "source": source,
    }
    if event_type == "order_created":
        quantity = _int_cell(row, "quantity")
        unit_price = parse_money(_cell(row, "unit_price"))
        envelope["payload"] = {
            "order_id": _cell(row, "order_id"),
            "customer_id": _cell(row, "customer_id"),
            "currency": _cell(row, "currency"),
            "items": [
                {
                    "product_id": _cell(row, "product_id"),
                    "quantity": quantity,
                    "unit_price": format(unit_price, "f"),
                }
            ],
            "total_amount": format(line_amount(quantity, unit_price), "f"),
        }
    elif event_type == "payment_completed":
        envelope["payload"] = {
            "payment_id": _cell(row, "payment_id"),
            "order_id": _cell(row, "order_id"),
            "customer_id": _cell(row, "customer_id"),
            "currency": _cell(row, "currency"),
            "amount": _cell(row, "amount"),
            "method": _cell(row, "payment_method"),
        }
    elif event_type == "inventory_updated":
        order_id = _optional(row, "order_id")
        payload: dict[str, object] = {
            "product_id": _cell(row, "product_id"),
            "warehouse_id": _cell(row, "warehouse_id"),
            "quantity_on_hand": _int_cell(row, "quantity_on_hand"),
            "delta": _int_cell(row, "inventory_delta"),
        }
        if order_id is not None:
            payload["order_id"] = order_id
        envelope["payload"] = payload
    else:
        raise ValueError(f"event_type {event_type!r} is not supported in CSV import")
    from titan_contracts.parsing import parse_commerce_object

    return parse_commerce_object(envelope)


def _cell(row: dict[str, str | None], name: str) -> str:
    value = _optional(row, name)
    if value is None:
        raise ValueError(f"{name} is required")
    return value


def _optional(row: dict[str, str | None], name: str) -> str | None:
    value = row.get(name)
    if value is None:
        return None
    text = value.strip()
    return text or None


def _int_cell(row: dict[str, str | None], name: str) -> int:
    text = _cell(row, name)
    if text.startswith(("+", "-")) and text[1:].isdigit() or text.isdigit():
        return int(text)
    raise ValueError(f"{name} must be an integer")


def _error_text(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        return exc.json()[:2000]
    return str(exc)[:2000]
