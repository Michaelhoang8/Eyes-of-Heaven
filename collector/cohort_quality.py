from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from collector.audit_market import (MAX_BTC_BOUNDARY_AGE_SECONDS,
                                    MAX_BTC_GAP_SECONDS, utc)
from collector.markets import parse_utc
from dashboard.queries import MAX_CHECKPOINT_AGE_SECONDS, open_connection

CHECKPOINTS = (60, 30, 10, 5, 1)
INTERVALS = (5, 15)
PRICE_EXPR = "COALESCE(price, CASE WHEN best_bid IS NOT NULL AND best_ask IS NOT NULL THEN (best_bid+best_ask)/2 END)"


def expected_starts(first: datetime, last: datetime, minutes: int) -> list[datetime]:
    """Return aligned UTC boundaries inclusively between first and last."""
    first, last = utc(first), utc(last)
    step = minutes * 60
    first_epoch = int(first.timestamp())
    aligned_epoch = ((first_epoch + step - 1) // step) * step
    current = datetime.fromtimestamp(aligned_epoch, timezone.utc)
    result = []
    while current <= last:
        result.append(current)
        current += timedelta(seconds=step)
    return result


def interval_minutes(row: dict[str, Any]) -> int | None:
    start, end = utc(row.get("start_time")), utc(row.get("end_time"))
    if not start or not end:
        return None
    seconds = round((end - start).total_seconds())
    return seconds // 60 if seconds in (300, 900) else None


def _btc_quality(con: duckdb.DuckDBPyConnection, start: datetime, end: datetime) -> dict[str, Any]:
    rows = con.execute(f"""SELECT exchange_timestamp,{PRICE_EXPR} AS btc_value
        FROM bitcoin_events WHERE exchange_timestamp BETWEEN ? AND ?
        AND {PRICE_EXPR} IS NOT NULL ORDER BY exchange_timestamp""", [start, end]).fetchall()
    times = [utc(row[0]) for row in rows]
    values = [row[1] for row in rows]
    first = times[0] if times else None
    last = times[-1] if times else None
    start_age = (first - start).total_seconds() if first else None
    end_age = (end - last).total_seconds() if last else None
    max_gap = max(((right - left).total_seconds() for left, right in zip(times, times[1:])),
                  default=None)
    reasons = []
    if not times:
        reasons.append("no BTC observations")
    else:
        if start_age > MAX_BTC_BOUNDARY_AGE_SECONDS:
            reasons.append("BTC start coverage")
        if end_age > MAX_BTC_BOUNDARY_AGE_SECONDS:
            reasons.append("BTC end coverage")
        if len(times) < 2:
            reasons.append("fewer than 2 BTC observations")
        elif max_gap > MAX_BTC_GAP_SECONDS:
            reasons.append("BTC gap")
        if len(set(values)) < 2:
            reasons.append("constant BTC price")
    return {"btc_first_timestamp": first, "btc_last_timestamp": last,
            "btc_start_age_seconds": start_age, "btc_end_age_seconds": end_age,
            "btc_observation_count": len(times), "btc_max_gap_seconds": max_gap,
            "btc_reasons": reasons, "btc_feature_eligible": not reasons}


def _checkpoint_quality(con: duckdb.DuckDBPyConnection, market_id: str,
                        start: datetime, end: datetime) -> tuple[dict[str, Any], list[str]]:
    result: dict[str, Any] = {}
    reasons = []
    for seconds in CHECKPOINTS:
        target = end - timedelta(seconds=seconds)
        ages = []
        for outcome in ("Up", "Down"):
            quote = con.execute("""SELECT (best_bid+best_ask)/2,state_as_of,
                    epoch(?-state_as_of) age
                FROM polymarket_quote_points
                WHERE market_id=? AND outcome=? AND state_as_of BETWEEN ? AND ?
                  AND best_bid IS NOT NULL AND best_ask IS NOT NULL
                ORDER BY state_as_of DESC,last_changed_at DESC,
                         receipt_timestamp DESC,event_id DESC LIMIT 1""",
                [target, market_id, outcome, start, target]).fetchone()
            prefix = f"t{seconds}_{outcome.lower()}"
            result[f"{prefix}_midpoint"] = quote[0] if quote else None
            result[f"{prefix}_state_timestamp"] = utc(quote[1]) if quote else None
            result[f"{prefix}_state_age_seconds"] = quote[2] if quote else None
            fresh = bool(quote and quote[2] <= MAX_CHECKPOINT_AGE_SECONDS)
            result[f"{prefix}_fresh"] = fresh
            if not quote:
                reasons.append(f"missing T-{seconds} {outcome} Polymarket checkpoint")
            elif not fresh:
                reasons.append(f"stale T-{seconds} {outcome} Polymarket state")
            if quote:
                ages.append(float(quote[2]))
        result[f"t{seconds}_state_age_seconds"] = max(ages) if ages else None
        result[f"t{seconds}_fresh"] = all(
            result[f"t{seconds}_{outcome.lower()}_fresh"] for outcome in ("Up", "Down"))
    return result, reasons


def market_quality(con: duckdb.DuckDBPyConnection, market: dict[str, Any]) -> dict[str, Any]:
    start, end = utc(market["start_time"]), utc(market["end_time"])
    row = {"market_id": str(market["market_id"]),
           "interval_minutes": interval_minutes(market), "start_time": start,
           "end_time": end, "status": market.get("status"),
           "resolved_outcome": market.get("resolved_outcome")}
    row.update(_btc_quality(con, start, end))
    checkpoints, poly_reasons = _checkpoint_quality(con, row["market_id"], start, end)
    row.update(checkpoints)
    valid_mapping = bool(market.get("up_token_id") and market.get("down_token_id")
                         and market["up_token_id"] != market["down_token_id"])
    metadata_reasons = []
    if market.get("status") != "completed":
        metadata_reasons.append("market not completed")
    if market.get("resolved_outcome") not in ("Up", "Down"):
        metadata_reasons.append("missing resolution")
    if not valid_mapping:
        metadata_reasons.append("invalid token mapping")
    row["polymarket_reasons"] = metadata_reasons + poly_reasons
    row["polymarket_calibration_eligible"] = not row["polymarket_reasons"]
    row["fully_eligible"] = (row["polymarket_calibration_eligible"]
                             and row["btc_feature_eligible"])
    row["exclusion_reasons"] = row["polymarket_reasons"] + row["btc_reasons"]
    return row


def _market_rows(con: duckdb.DuckDBPyConnection, since: datetime) -> list[dict[str, Any]]:
    frame = con.execute("SELECT * FROM markets WHERE start_time>? ORDER BY start_time,market_id",
                        [since]).df()
    rows = [row.to_dict() for _, row in frame.iterrows()]
    return [row for row in rows if interval_minutes(row) in INTERVALS]


def classify_expected_start(candidates: list[dict[str, Any]], quality_by_id: dict[str, dict]) -> str:
    if not candidates:
        return "no market row at all"
    if any(row.get("status") == "completed"
           and row.get("resolved_outcome") in ("Up", "Down") for row in candidates):
        return "completed/resolved"
    if any(row.get("status") == "active" or row.get("resolved_outcome") not in ("Up", "Down")
           for row in candidates):
        return "active/unresolved"
    return "other status"


def evaluate_cohort(con: duckdb.DuckDBPyConnection, since: datetime):
    all_rows = _market_rows(con, since)
    completed = [row for row in all_rows if row.get("status") == "completed"]
    quality = [market_quality(con, row) for row in completed]
    quality_by_id = {row["market_id"]: row for row in quality}
    missing = []
    interval_summary = {}
    for minutes in INTERVALS:
        completed_type = [row for row in completed if interval_minutes(row) == minutes]
        expected = (expected_starts(min(utc(row["start_time"]) for row in completed_type),
                                    max(utc(row["start_time"]) for row in completed_type), minutes)
                    if completed_type else [])
        rows_type = [row for row in all_rows if interval_minutes(row) == minutes]
        coverage_counts = Counter()
        quality_eligible_starts = 0
        for start in expected:
            candidates = [row for row in rows_type if utc(row["start_time"]) == start]
            state = classify_expected_start(candidates, quality_by_id)
            coverage_counts[state] += 1
            completed_ids = [str(row["market_id"]) for row in candidates
                             if str(row["market_id"]) in quality_by_id]
            if any(quality_by_id[market_id]["fully_eligible"] for market_id in completed_ids):
                quality_eligible_starts += 1
            if state != "completed/resolved":
                missing.append({"interval_minutes": minutes, "start_time": start,
                                "database_state": state})
        interval_summary[minutes] = {"expected": len(expected),
                                     "stored": len(expected) - coverage_counts["no market row at all"],
                                     "completed_resolved": coverage_counts["completed/resolved"],
                                     "active_unresolved": coverage_counts["active/unresolved"],
                                     "other_status": coverage_counts["other status"],
                                     "no_row": coverage_counts["no market row at all"],
                                     "quality_eligible": quality_eligible_starts,
                                     "first": expected[0] if expected else None,
                                     "last": expected[-1] if expected else None}
    return quality, interval_summary, missing


def _display(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def print_report(since: datetime, quality: list[dict[str, Any]], interval_summary: dict,
                 missing: list[dict[str, Any]]) -> None:
    print("COHORT QUALITY")
    print(f"Since: {_display(since)}")
    print(f"Stored completed markets: {len(quality)}")
    for minutes in INTERVALS:
        summary = interval_summary[minutes]
        print(f"\n{minutes}m:")
        print(f"Expected intervals: {summary['expected']}")
        if summary["first"]:
            print(f"Expected range: {_display(summary['first'])} through {_display(summary['last'])}")
        print(f"Stored market rows: {summary['stored']}")
        print(f"Completed/resolved: {summary['completed_resolved']}")
        print(f"Active/unresolved: {summary['active_unresolved']}")
        print(f"Other status: {summary['other_status']}")
        print(f"No market row: {summary['no_row']}")
        print(f"Quality eligible: {summary['quality_eligible']}")
    total = len(quality)
    poly = sum(row["polymarket_calibration_eligible"] for row in quality)
    btc = sum(row["btc_feature_eligible"] for row in quality)
    full = sum(row["fully_eligible"] for row in quality)
    print("\nEligibility:")
    print(f"Polymarket calibration eligible: {poly} / {total}")
    print(f"BTC-feature eligible: {btc} / {total}")
    print(f"Fully eligible: {full} / {total}")
    counts = Counter(reason for row in quality for reason in row["exclusion_reasons"])
    print("\nExclusion reasons:")
    if counts:
        for reason, count in sorted(counts.items()):
            print(f"* {reason}: {count}")
    else:
        print("None")
    print("\nStored completed market quality:")
    if not quality:
        print("None")
    else:
        checkpoint_header = " | ".join(f"T-{seconds} Up/Down/Age/Fresh" for seconds in CHECKPOINTS)
        print("Market ID | Interval | Start UTC | End UTC | Outcome | BTC first | BTC last | "
              "BTC start age | BTC end age | BTC count | BTC max gap | " + checkpoint_header +
              " | Poly eligible | BTC eligible | Fully eligible")
        for row in quality:
            checkpoints = []
            for seconds in CHECKPOINTS:
                up = row[f"t{seconds}_up_midpoint"]
                down = row[f"t{seconds}_down_midpoint"]
                age = row[f"t{seconds}_state_age_seconds"]
                fresh = row[f"t{seconds}_fresh"]
                checkpoints.append(f"{up if up is not None else 'NULL'}/"
                                   f"{down if down is not None else 'NULL'}/"
                                   f"{age:.3f}s/{'Y' if fresh else 'N'}" if age is not None
                                   else f"{up or 'NULL'}/{down or 'NULL'}/NULL/N")
            print(f"{row['market_id']} | {row['interval_minutes']}m | {_display(row['start_time'])} | "
                  f"{_display(row['end_time'])} | {row['resolved_outcome'] or 'NULL'} | "
                  f"{_display(row['btc_first_timestamp']) if row['btc_first_timestamp'] else 'NULL'} | "
                  f"{_display(row['btc_last_timestamp']) if row['btc_last_timestamp'] else 'NULL'} | "
                  f"{row['btc_start_age_seconds']} | {row['btc_end_age_seconds']} | "
                  f"{row['btc_observation_count']} | {row['btc_max_gap_seconds']} | " +
                  " | ".join(checkpoints) +
                  f" | {row['polymarket_calibration_eligible']} | "
                  f"{row['btc_feature_eligible']} | {row['fully_eligible']}")
    print("\nExpected intervals without a completed resolution:")
    if not missing:
        print("None")
    else:
        print("Interval | Start UTC | Database state/reason")
        for row in missing:
            print(f"{row['interval_minutes']}m | {_display(row['start_time'])} | {row['database_state']}")


def _csv_rows(quality: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for row in quality:
        exported = {key: value for key, value in row.items()
                    if key not in ("btc_reasons", "polymarket_reasons", "exclusion_reasons")}
        exported["btc_exclusion_reasons"] = "; ".join(row["btc_reasons"])
        exported["polymarket_exclusion_reasons"] = "; ".join(row["polymarket_reasons"])
        exported["exclusion_reasons"] = "; ".join(row["exclusion_reasons"])
        rows.append(exported)
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only quality report for a collector cohort.")
    parser.add_argument("--since", required=True, help="Exclusive UTC cohort boundary")
    parser.add_argument("--csv", type=Path, help="Optional per-market CSV output path")
    parser.add_argument("--db", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    since = parse_utc(args.since)
    con = open_connection(args.db) if args.db else open_connection()
    try:
        quality, interval_summary, missing = evaluate_cohort(con, since)
    finally:
        con.close()
    print_report(since, quality, interval_summary, missing)
    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        _csv_rows(quality).to_csv(args.csv, index=False)
        print(f"\nCSV: {args.csv.resolve()}")


if __name__ == "__main__":
    main()
