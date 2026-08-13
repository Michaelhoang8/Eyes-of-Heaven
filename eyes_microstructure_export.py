from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

CHECKPOINTS = (60, 30)
LOOKBACK_SECONDS = 30
MAX_STATE_AGE_SECONDS = 5.0


def utc(value: Any) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").to_pydatetime()


def parse_utc(text: str) -> datetime:
    value = utc(pd.Timestamp(text))
    if value is None:
        raise ValueError("Invalid UTC timestamp")
    return value


def quote_at_or_before(
    con: duckdb.DuckDBPyConnection,
    *,
    market_id: str,
    outcome: str,
    start: datetime,
    target: datetime,
) -> dict[str, Any] | None:
    row = con.execute(
        """
        SELECT best_bid,
               best_ask,
               (best_bid + best_ask) / 2 AS midpoint,
               state_as_of,
               last_changed_at,
               receipt_timestamp,
               event_id,
               epoch(? - state_as_of) AS age_seconds
        FROM polymarket_quote_points
        WHERE market_id = ?
          AND outcome = ?
          AND state_as_of BETWEEN ? AND ?
          AND best_bid IS NOT NULL
          AND best_ask IS NOT NULL
        ORDER BY state_as_of DESC,
                 last_changed_at DESC,
                 receipt_timestamp DESC,
                 event_id DESC
        LIMIT 1
        """,
        [target, market_id, outcome, start, target],
    ).fetchone()

    if row is None:
        return None

    return {
        "best_bid": float(row[0]),
        "best_ask": float(row[1]),
        "spread": float(row[1]) - float(row[0]),
        "midpoint": float(row[2]),
        "state_as_of": utc(row[3]),
        "last_changed_at": utc(row[4]),
        "receipt_timestamp": utc(row[5]),
        "event_id": str(row[6]),
        "age_seconds": float(row[7]),
    }


def build_rows(con: duckdb.DuckDBPyConnection, since: datetime) -> pd.DataFrame:
    markets = con.execute(
        """
        SELECT market_id,
               start_time,
               end_time,
               resolved_outcome,
               up_token_id,
               down_token_id
        FROM markets
        WHERE start_time > ?
          AND status = 'completed'
          AND resolved_outcome IN ('Up', 'Down')
        ORDER BY start_time, market_id
        """,
        [since],
    ).fetchall()

    rows: list[dict[str, Any]] = []

    for market_id, start_raw, end_raw, resolved_outcome, up_token, down_token in markets:
        start, end = utc(start_raw), utc(end_raw)
        if start is None or end is None:
            continue
        if round((end - start).total_seconds()) != 300:
            continue
        if not up_token or not down_token or up_token == down_token:
            continue

        y = 1 if resolved_outcome == "Up" else 0

        for checkpoint_seconds in CHECKPOINTS:
            checkpoint = end - timedelta(seconds=checkpoint_seconds)
            prior_target = checkpoint - timedelta(seconds=LOOKBACK_SECONDS)

            current_up = quote_at_or_before(
                con, market_id=str(market_id), outcome="Up", start=start, target=checkpoint
            )
            current_down = quote_at_or_before(
                con, market_id=str(market_id), outcome="Down", start=start, target=checkpoint
            )
            prior_up = quote_at_or_before(
                con, market_id=str(market_id), outcome="Up", start=start, target=prior_target
            )
            prior_down = quote_at_or_before(
                con, market_id=str(market_id), outcome="Down", start=start, target=prior_target
            )

            selected = (current_up, current_down, prior_up, prior_down)
            all_present = all(v is not None for v in selected)
            eligible = bool(
                all_present
                and all(v["age_seconds"] <= MAX_STATE_AGE_SECONDS for v in selected)
            )

            row: dict[str, Any] = {
                "market_id": str(market_id),
                "start_time": start,
                "end_time": end,
                "resolved_outcome": resolved_outcome,
                "y": y,
                "checkpoint_seconds": checkpoint_seconds,
                "checkpoint_timestamp": checkpoint,
                "lookback_seconds": LOOKBACK_SECONDS,
                "prior_target_timestamp": prior_target,
                "all_states_present": all_present,
                "microstructure_eligible": eligible,
            }

            def add_state(prefix: str, state: dict[str, Any] | None) -> None:
                row[f"{prefix}_best_bid"] = state["best_bid"] if state else None
                row[f"{prefix}_best_ask"] = state["best_ask"] if state else None
                row[f"{prefix}_midpoint"] = state["midpoint"] if state else None
                row[f"{prefix}_spread"] = (
                    state["best_ask"] - state["best_bid"] if state else None
                )
                row[f"{prefix}_state_as_of"] = state["state_as_of"] if state else None
                row[f"{prefix}_age_seconds"] = state["age_seconds"] if state else None

            add_state("current_up", current_up)
            add_state("current_down", current_down)
            add_state("prior_up", prior_up)
            add_state("prior_down", prior_down)

            if all_present:
                row["p_up"] = current_up["midpoint"]
                row["p_down"] = current_down["midpoint"]
                row["prior_p_up"] = prior_up["midpoint"]
                row["prior_p_down"] = prior_down["midpoint"]
                row["momentum_30s"] = current_up["midpoint"] - prior_up["midpoint"]
                row["abs_momentum_30s"] = abs(row["momentum_30s"])
                row["current_midpoint_sum"] = current_up["midpoint"] + current_down["midpoint"]
                row["prior_midpoint_sum"] = prior_up["midpoint"] + prior_down["midpoint"]
                row["mean_current_spread"] = (
                    current_up["spread"] + current_down["spread"]
                ) / 2
            else:
                for key in (
                    "p_up", "p_down", "prior_p_up", "prior_p_down",
                    "momentum_30s", "abs_momentum_30s",
                    "current_midpoint_sum", "prior_midpoint_sum",
                    "mean_current_spread",
                ):
                    row[key] = None

            rows.append(row)

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only Polymarket microstructure development export."
    )
    parser.add_argument("--since", required=True, help="Exclusive cohort start in UTC")
    parser.add_argument("--csv", type=Path, default=Path("microstructure-rows.csv"))
    parser.add_argument("--db", type=Path, default=Path("data/research.duckdb"))
    args = parser.parse_args()

    since = parse_utc(args.since)
    con = duckdb.connect(str(args.db), read_only=True)
    try:
        rows = build_rows(con, since)
    finally:
        con.close()

    args.csv.parent.mkdir(parents=True, exist_ok=True)
    rows.to_csv(args.csv, index=False)

    print("POLYMARKET MICROSTRUCTURE DEVELOPMENT EXPORT - READ ONLY")
    print(f"Since: {since.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')}")
    print("Interval: 5m only")
    print("Checkpoints: T-60 and T-30")
    print("Momentum: current p_up minus p_up 30 seconds earlier")
    print(f"Rows: {len(rows)}")
    print(f"Markets: {rows.market_id.nunique() if not rows.empty else 0}")
    if not rows.empty:
        for checkpoint in CHECKPOINTS:
            part = rows[rows.checkpoint_seconds == checkpoint]
            eligible = int(part.microstructure_eligible.sum())
            print(f"T-{checkpoint}: {eligible} / {len(part)} microstructure-eligible")
        future_current = (
            rows["current_up_state_as_of"] > rows["checkpoint_timestamp"]
        ).fillna(False).sum()
        future_prior = (
            rows["prior_up_state_as_of"] > rows["prior_target_timestamp"]
        ).fillna(False).sum()
        print(f"Future current-state selections: {int(future_current)}")
        print(f"Future prior-state selections: {int(future_prior)}")
    print(f"CSV: {args.csv.resolve()}")


if __name__ == "__main__":
    main()
