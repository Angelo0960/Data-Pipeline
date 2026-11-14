"""Approved deterministic business calculations for transformed farm data."""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any


def apply_business_rules(
    accepted: dict[str, list[dict[str, str]]],
    pipeline_run_id: str,
    window_start: Any,
    window_end: Any,
) -> tuple[dict[str, list[dict[str, str]]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    """Return detail rows with approved derived fields and separate summaries."""
    transformed = {table: [dict(row) for row in rows] for table, rows in accepted.items()}
    for row in transformed["pig_batches"]:
        start = row.get("start_weight", "")
        current = row.get("current_weight", "")
        row["weight_change"] = format(Decimal(current) - Decimal(start), ".2f") if start and current else ""

    feed_totals: defaultdict[str, Decimal] = defaultdict(Decimal)
    for row in transformed["feed_records"]:
        feed_totals[row["batch_id"]] += Decimal(row["quantity_kg"])
    expense_totals: defaultdict[str, Decimal] = defaultdict(Decimal)
    farm_expense_totals: defaultdict[str, Decimal] = defaultdict(Decimal)
    farm_expense_counts: defaultdict[str, int] = defaultdict(int)
    for row in transformed["expenses"]:
        batch_id = row.get("batch_id", "").strip()
        if batch_id:
            expense_totals[batch_id] += Decimal(row["amount"])
        else:
            expense_type = row["expense_type"]
            farm_expense_totals[expense_type] += Decimal(row["amount"])
            farm_expense_counts[expense_type] += 1

    start_window = "" if window_start is None else str(window_start)
    end_window = "" if window_end is None else str(window_end)
    feed_summary = [
        {"batch_id": batch_id, "total_feed_kg": format(amount, ".2f"),
         "pipeline_run_id": pipeline_run_id, "window_start": start_window, "window_end": end_window}
        for batch_id, amount in sorted(feed_totals.items())
    ]
    expense_summary = [
        {"batch_id": batch_id, "total_expense": format(amount, ".2f"),
         "pipeline_run_id": pipeline_run_id, "window_start": start_window, "window_end": end_window}
        for batch_id, amount in sorted(expense_totals.items())
    ]
    farm_expense_summary = [
        {"expense_type": expense_type, "record_count": str(farm_expense_counts[expense_type]),
         "total_expense": format(amount, ".2f"), "pipeline_run_id": pipeline_run_id,
         "window_start": start_window, "window_end": end_window}
        for expense_type, amount in sorted(farm_expense_totals.items())
    ]
    # Vaccination due status is intentionally omitted until its business rule is approved.
    return transformed, feed_summary, expense_summary, farm_expense_summary


def validate_summaries(
    feed_rows: list[dict[str, str]],
    expense_rows: list[dict[str, str]],
    feed_summary: list[dict[str, str]],
    expense_summary: list[dict[str, str]],
    farm_expense_summary: list[dict[str, str]],
) -> list[str]:
    errors: list[str] = []
    expected_feed: defaultdict[str, Decimal] = defaultdict(Decimal)
    for row in feed_rows:
        expected_feed[row["batch_id"]] += Decimal(row["quantity_kg"])
    observed_feed = {row["batch_id"]: Decimal(row["total_feed_kg"]) for row in feed_summary}
    if dict(expected_feed) != observed_feed:
        errors.append("feed_summary totals do not match accepted feed detail")

    expected_expense: defaultdict[str, Decimal] = defaultdict(Decimal)
    expected_farm_expense: defaultdict[str, Decimal] = defaultdict(Decimal)
    expected_farm_count: defaultdict[str, int] = defaultdict(int)
    for row in expense_rows:
        batch_id = row.get("batch_id", "").strip()
        if batch_id:
            expected_expense[batch_id] += Decimal(row["amount"])
        else:
            expense_type = row["expense_type"]
            expected_farm_expense[expense_type] += Decimal(row["amount"])
            expected_farm_count[expense_type] += 1
    observed_expense = {row["batch_id"]: Decimal(row["total_expense"]) for row in expense_summary}
    if dict(expected_expense) != observed_expense:
        errors.append("expense_summary totals do not match accepted expense detail")
    observed_farm_expense = {
        row["expense_type"]: (int(row["record_count"]), Decimal(row["total_expense"]))
        for row in farm_expense_summary
    }
    expected_farm = {
        key: (expected_farm_count[key], amount)
        for key, amount in expected_farm_expense.items()
    }
    if expected_farm != observed_farm_expense:
        errors.append("farm_expense_summary does not reconcile with farm-level expense detail")
    return errors
