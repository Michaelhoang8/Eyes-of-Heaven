from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from collector.audit_market import utc
from collector.cohort_quality import CHECKPOINTS, interval_minutes
from collector.markets import DEFAULT_DB, parse_utc
from dashboard.queries import MAX_CHECKPOINT_AGE_SECONDS, open_connection

BINS = ((0.00, 0.10), (0.10, 0.30), (0.30, 0.70),
        (0.70, 0.90), (0.90, 1.00))


def outcome_value(outcome: str) -> int:
    if outcome == "Up":
        return 1
    if outcome == "Down":
        return 0
    raise ValueError(f"Resolved outcome must be Up or Down, received {outcome!r}")


def checkpoint_observation(con: duckdb.DuckDBPyConnection, *, market_id: str,
                           outcome: str, start: datetime,
                           checkpoint: datetime) -> dict[str, Any] | None:
    row = con.execute("""SELECT (best_bid+best_ask)/2,state_as_of,
               epoch(?-state_as_of) age
        FROM polymarket_quote_points
        WHERE market_id=? AND outcome=? AND state_as_of BETWEEN ? AND ?
          AND best_bid IS NOT NULL AND best_ask IS NOT NULL
        ORDER BY state_as_of DESC,last_changed_at DESC,
                 receipt_timestamp DESC,event_id DESC LIMIT 1""",
        [checkpoint, market_id, outcome, start, checkpoint]).fetchone()
    if not row:
        return None
    return {"midpoint": float(row[0]), "state_as_of": utc(row[1]),
            "state_age_seconds": float(row[2]),
            "fresh": float(row[2]) <= MAX_CHECKPOINT_AGE_SECONDS}


def calibration_rows(con: duckdb.DuckDBPyConnection, since: datetime) -> pd.DataFrame:
    frame = con.execute("""SELECT * FROM markets
        WHERE start_time>? AND status='completed' AND resolved_outcome IN ('Up','Down')
        ORDER BY start_time,market_id""", [since]).df()
    rows = []
    for _, stored in frame.iterrows():
        market = stored.to_dict()
        minutes = interval_minutes(market)
        if minutes not in (5, 15):
            continue
        if (not market.get("up_token_id") or not market.get("down_token_id")
                or market["up_token_id"] == market["down_token_id"]):
            continue
        start, end = utc(market["start_time"]), utc(market["end_time"])
        observations: dict[int, dict[str, dict[str, Any] | None]] = {}
        eligible = True
        for seconds in CHECKPOINTS:
            checkpoint = end - timedelta(seconds=seconds)
            values = {outcome: checkpoint_observation(
                con, market_id=str(market["market_id"]), outcome=outcome,
                start=start, checkpoint=checkpoint) for outcome in ("Up", "Down")}
            observations[seconds] = values
            if any(value is None or not value["fresh"] for value in values.values()):
                eligible = False
        if not eligible:
            continue
        y = outcome_value(str(market["resolved_outcome"]))
        for seconds in CHECKPOINTS:
            checkpoint = end - timedelta(seconds=seconds)
            up = observations[seconds]["Up"]
            down = observations[seconds]["Down"]
            rows.append({"market_id": str(market["market_id"]),
                         "interval_minutes": minutes, "start_time": start,
                         "end_time": end, "resolved_outcome": market["resolved_outcome"],
                         "checkpoint_seconds": seconds,
                         "checkpoint_timestamp": checkpoint,
                         "p_up": up["midpoint"], "y": y,
                         "up_state_as_of": up["state_as_of"],
                         "down_state_as_of": down["state_as_of"],
                         "up_state_age_seconds": up["state_age_seconds"],
                         "down_state_age_seconds": down["state_age_seconds"]})
    return pd.DataFrame(rows)


def descriptive_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {"n": 0, "up_count": 0, "up_pct": None, "mean_p_up": None,
                "brier": None, "mae": None, "directional_accuracy": None,
                "mean_winner_probability": None, "calibration_in_large": None}
    p = rows["p_up"].astype(float)
    y = rows["y"].astype(int)
    winner_probability = p.where(y == 1, 1-p)
    predictions = (p >= .50).astype(int)
    return {"n": len(rows), "up_count": int(y.sum()), "up_pct": float(y.mean()*100),
            "mean_p_up": float(p.mean()), "brier": float(((p-y)**2).mean()),
            "mae": float((p-y).abs().mean()),
            "directional_accuracy": float((predictions == y).mean()*100),
            "mean_winner_probability": float(winner_probability.mean()),
            "calibration_in_large": float(y.mean()-p.mean())}


def probability_bin(value: float) -> str:
    for index, (lower, upper) in enumerate(BINS):
        if lower <= value < upper or (index == len(BINS)-1 and value == upper):
            return f"{lower:.2f}-{upper:.2f}"
    raise ValueError(f"Probability outside [0,1]: {value}")


def calibration_bins(rows: pd.DataFrame) -> list[dict[str, Any]]:
    if rows.empty:
        return []
    work = rows.copy()
    work["bin"] = work["p_up"].map(probability_bin)
    result = []
    for lower, upper in BINS:
        label = f"{lower:.2f}-{upper:.2f}"
        group = work[work["bin"] == label]
        if group.empty:
            continue
        predicted = float(group.p_up.mean())
        actual = float(group.y.mean())
        result.append({"bin": label, "n": len(group), "mean_p_up": predicted,
                       "actual_up_frequency": actual,
                       "actual_minus_predicted": actual-predicted,
                       "sample_status": "OK" if len(group) >= 5 else "INSUFFICIENT SAMPLE"})
    return result


def _number(value: float | None, *, percent: bool = False) -> str:
    if value is None:
        return "NULL"
    return f"{value:.2f}%" if percent else f"{value:.6f}"


def print_section(label: str, rows: pd.DataFrame) -> None:
    print(f"\n{label}")
    print("Checkpoint | N | Up outcomes | Mean p_up | Brier | MAE | Directional accuracy | "
          "Mean probability assigned to winner (descriptive only) | Actual Up rate - mean p_up")
    for seconds in CHECKPOINTS:
        checkpoint_rows = rows[rows.checkpoint_seconds == seconds]
        metrics = descriptive_metrics(checkpoint_rows)
        up = (f"{metrics['up_count']} / {metrics['n']} ({_number(metrics['up_pct'], percent=True)})"
              if metrics["n"] else "0 / 0 (NULL)")
        print(f"T-{seconds} | {metrics['n']} | {up} | {_number(metrics['mean_p_up'])} | "
              f"{_number(metrics['brier'])} | {_number(metrics['mae'])} | "
              f"{_number(metrics['directional_accuracy'], percent=True)} | "
              f"{_number(metrics['mean_winner_probability'])} | "
              f"{_number(metrics['calibration_in_large'])}")
        print("  Calibration bins: bin | N | mean p_up | actual Up frequency | actual-predicted | sample")
        bins = calibration_bins(checkpoint_rows)
        if not bins:
            print("  None")
        for item in bins:
            print(f"  {item['bin']} | {item['n']} | {_number(item['mean_p_up'])} | "
                  f"{_number(item['actual_up_frequency'])} | "
                  f"{_number(item['actual_minus_predicted'])} | {item['sample_status']}")


def print_report(since: datetime, rows: pd.DataFrame) -> None:
    markets = rows.market_id.nunique() if not rows.empty else 0
    print("POLYMARKET CALIBRATION - DESCRIPTIVE ONLY")
    print(f"Since: {since.astimezone(timezone.utc).isoformat().replace('+00:00','Z')}")
    print(f"Eligible markets: {markets}")
    print("Predictor: recorded Up midpoint available at or before each checkpoint")
    print("Directional rule: predict Up when p_up >= 0.50, otherwise Down")
    print("No BTC variables, outcome-derived predictors, optimization, or trading-edge claims.")
    for minutes in (5, 15):
        print_section(f"{minutes}m markets", rows[rows.interval_minutes == minutes])
    print_section("Combined 5m + 15m markets (combined within each checkpoint only)", rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Descriptive Polymarket calibration analysis.")
    parser.add_argument("--since", required=True, help="Exclusive cohort start in UTC")
    parser.add_argument("--csv", type=Path, help="Export market/checkpoint input rows")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help=argparse.SUPPRESS)
    args = parser.parse_args()
    since = parse_utc(args.since)
    con = open_connection(args.db)
    try:
        rows = calibration_rows(con, since)
    finally:
        con.close()
    print_report(since, rows)
    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        rows.to_csv(args.csv, index=False)
        print(f"\nCSV: {args.csv.resolve()}")


if __name__ == "__main__":
    main()
