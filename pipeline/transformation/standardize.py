"""Field validation and standardization for PrepAPig Transformation."""
from __future__ import annotations

from datetime import date, datetime, time as time_value, timezone
from decimal import Decimal, InvalidOperation
import uuid
from typing import Any


class RowValidationError(ValueError):
    def __init__(self, rule_id: str, field: str, message: str):
        super().__init__(message)
        self.rule_id = rule_id
        self.field = field


SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    "pig_batches": ("id", "batch_code", "pig_count", "breed", "start_weight", "current_weight", "date_acquired", "status", "created_at"),
    "feed_records": ("id", "batch_id", "feed_type", "quantity_kg", "feeding_date", "feeding_time", "notes", "created_at"),
    "vaccination_records": ("id", "batch_id", "vaccine_name", "vaccination_date", "next_due_date", "administered_by", "dosage", "notes", "status", "created_at"),
    "expenses": ("id", "batch_id", "expense_type", "amount", "expense_date", "description", "created_at"),
}
REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "pig_batches": ("id", "batch_code", "pig_count", "created_at"),
    "feed_records": ("id", "batch_id", "feed_type", "quantity_kg", "feeding_date", "created_at"),
    "vaccination_records": ("id", "batch_id", "vaccine_name", "vaccination_date", "created_at"),
    "expenses": ("id", "expense_type", "amount", "expense_date", "created_at"),
}
DECIMAL_FIELDS: dict[str, tuple[str, ...]] = {
    "pig_batches": ("start_weight", "current_weight"),
    "feed_records": ("quantity_kg",),
    "expenses": ("amount",),
}
DATE_FIELDS: dict[str, tuple[str, ...]] = {
    "pig_batches": ("date_acquired",),
    "feed_records": ("feeding_date",),
    "vaccination_records": ("vaccination_date", "next_due_date"),
    "expenses": ("expense_date",),
}
TEXT_FIELDS: dict[str, tuple[str, ...]] = {
    "pig_batches": ("batch_code", "breed", "status"),
    "feed_records": ("feed_type", "notes"),
    "vaccination_records": ("vaccine_name", "administered_by", "dosage", "notes", "status"),
    "expenses": ("expense_type", "description"),
}


def _uuid(value: str, field: str) -> str:
    try:
        return str(uuid.UUID((value or "").strip()))
    except (ValueError, AttributeError) as exc:
        rule = "C-01" if field == "id" else "R-01"
        raise RowValidationError(rule, field, f"{field} is not a valid UUID") from exc


def _decimal(value: str, field: str, *, required: bool) -> str:
    text = (value or "").strip()
    if not text:
        if required:
            raise RowValidationError("C-04", field, f"{field} is required")
        return ""
    try:
        number = Decimal(text)
        if not number.is_finite():
            raise InvalidOperation
        scaled = number.quantize(Decimal("0.01"))
    except InvalidOperation as exc:
        raise RowValidationError("C-05", field, f"{field} must be a valid decimal with at most two decimal places") from exc
    if number < 0:
        raise RowValidationError("C-05", field, f"{field} must not be negative")
    if number != scaled:
        raise RowValidationError("C-05", field, f"{field} has more than two meaningful decimal places")
    if scaled > Decimal("99999999.99"):
        raise RowValidationError("C-05", field, f"{field} exceeds the documented DECIMAL(10,2) range")
    return format(scaled, ".2f")


def _date(value: str, field: str, *, required: bool) -> str:
    text = (value or "").strip()
    if not text:
        if required:
            raise RowValidationError("C-04", field, f"{field} is required")
        return ""
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise RowValidationError("C-07", field, f"{field} must be a valid ISO date") from exc


def _timestamp(value: str, field: str) -> str:
    text = (value or "").strip()
    if not text:
        raise RowValidationError("C-07", field, f"{field} is required")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError as exc:
        raise RowValidationError("C-07", field, f"{field} must be an ISO timestamp") from exc


def _time(value: str, field: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    parsed = None
    try:
        parsed = time_value.fromisoformat(text)
    except ValueError:
        for time_format in ("%I:%M:%S %p", "%I:%M %p"):
            try:
                parsed = datetime.strptime(text.upper(), time_format).time()
                break
            except ValueError:
                continue
    if parsed is None:
        raise RowValidationError("C-07", field, f"{field} must be a valid time")
    precision = "seconds" if parsed.second or parsed.microsecond else "minutes"
    return parsed.isoformat(timespec=precision)


def standardize_record(table: str, source: dict[str, Any]) -> dict[str, str]:
    if table not in SOURCE_COLUMNS:
        raise ValueError(f"Unsupported table: {table}")
    row = {str(key): "" if value is None else str(value) for key, value in source.items()}
    required = set(REQUIRED_FIELDS[table])
    for field in required:
        if not row.get(field, "").strip():
            rule = "C-01" if field == "id" else "R-01" if field == "batch_id" else "C-03" if field == "batch_code" else "C-04"
            raise RowValidationError(rule, field, f"{field} is required")
    for field in ("id", "batch_id"):
        if field in row and (field in required or row[field].strip()):
            row[field] = _uuid(row[field], field)
    if table == "pig_batches":
        try:
            count = int(row["pig_count"].strip())
        except ValueError as exc:
            raise RowValidationError("C-05", "pig_count", "pig_count must be a whole number") from exc
        if count < 0 or count > 2147483647:
            raise RowValidationError("C-05", "pig_count", "pig_count is outside the non-negative INTEGER range")
        row["pig_count"] = str(count)
    for field in DECIMAL_FIELDS.get(table, ()):
        row[field] = _decimal(row.get(field, ""), field, required=field in required)
    for field in DATE_FIELDS.get(table, ()):
        row[field] = _date(row.get(field, ""), field, required=field in required)
    row["created_at"] = _timestamp(row.get("created_at", ""), "created_at")
    if table == "feed_records":
        row["feeding_time"] = _time(row.get("feeding_time", ""), "feeding_time")
    for field in TEXT_FIELDS.get(table, ()):
        row[field] = (row.get(field) or "").strip()
    return row


def validate_window_timestamp(value: str, field: str) -> datetime | None:
    """Parse timestamps with the same UTC convention used by extraction validation.

    Source `created_at` values may be timezone-naive. Extraction validation
    interprets those values as UTC; normalize them here as well for window
    comparisons while leaving the original CSV representation unchanged.
    """
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} is not a valid ISO timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
