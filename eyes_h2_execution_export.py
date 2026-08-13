from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd

CHECKPOINT_SECONDS = 30
MAX_POLY_AGE_SECONDS = 5.0
MAX_BTC_BOUNDARY_AGE_SECONDS = 5.0
MAX_BTC_GAP_SECONDS = 30.0
MAX_BTC_CHECKPOINT_AGE_SECONDS = 5.0
PRICE_EXPR = "COALESCE(price, CASE WHEN best_bid IS NOT NULL AND best_ask IS NOT NULL THEN (best_bid+best_ask)/2 END)"


def parse_utc(text: str) -> datetime:
    dt = pd.Timestamp(text)
    if dt.tzinfo is None:
        dt = dt.tz_localize("UTC")
    return dt.tz_convert("UTC").to_pydatetime()


def utc(value) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    dt = pd.Timestamp(value)
    if dt.tzinfo is None:
        dt = dt.tz_localize("UTC")
    return dt.tz_convert("UTC").to_pydatetime()


def btc_series(con, start: datetime, end: datetime):
    return con.execute(f"""
        SELECT exchange_timestamp, {PRICE_EXPR} AS btc_value
        FROM bitcoin_events
        WHERE exchange_timestamp BETWEEN ? AND ?
          AND {PRICE_EXPR} IS NOT NULL
        ORDER BY exchange_timestamp
    """, [start, end]).fetchall()


def btc_market_eligible(rows, start: datetime, end: datetime):
    if not rows:
        return False, None, None
    times = [utc(r[0]) for r in rows]
    values = [float(r[1]) for r in rows]
    if (times[0] - start).total_seconds() > MAX_BTC_BOUNDARY_AGE_SECONDS:
        return False, None, None
    if (end - times[-1]).total_seconds() > MAX_BTC_BOUNDARY_AGE_SECONDS:
        return False, None, None
    if len(times) < 2:
        return False, None, None
    max_gap = max((b-a).total_seconds() for a, b in zip(times, times[1:]))
    if max_gap > MAX_BTC_GAP_SECONDS:
        return False, None, None
    if len(set(values)) < 2:
        return False, None, None
    return True, times[0], values[0]


def latest_btc_at_or_before(rows, checkpoint: datetime):
    selected = None
    for ts, value in rows:
        ts = utc(ts)
        if ts <= checkpoint:
            selected = (ts, float(value))
        else:
            break
    return selected


def poly_checkpoint(con, market_id: str, outcome: str, start: datetime, checkpoint: datetime):
    row = con.execute("""
        SELECT best_bid, best_ask, (best_bid+best_ask)/2 AS midpoint,
               state_as_of, epoch(?-state_as_of) AS age
        FROM polymarket_top_of_book
        WHERE market_id=? AND outcome=?
          AND state_as_of BETWEEN ? AND ?
          AND best_bid IS NOT NULL AND best_ask IS NOT NULL
        ORDER BY state_as_of DESC, last_changed_at DESC, event_id DESC
        LIMIT 1
    """, [checkpoint, market_id, outcome, start, checkpoint]).fetchone()
    if not row:
        return None
    return {
        "best_bid": float(row[0]),
        "best_ask": float(row[1]),
        "midpoint": float(row[2]),
        "state_as_of": utc(row[3]),
        "age_seconds": float(row[4]),
    }


def build_rows(con, since: datetime) -> pd.DataFrame:
    markets = con.execute("""
        SELECT market_id,start_time,end_time,resolved_outcome,up_token_id,down_token_id
        FROM markets
        WHERE start_time>? AND status='completed' AND resolved_outcome IN ('Up','Down')
        ORDER BY start_time,market_id
    """, [since]).fetchall()

    output = []
    for market_id, start_raw, end_raw, outcome, up_token, down_token in markets:
        start, end = utc(start_raw), utc(end_raw)
        if round((end-start).total_seconds()) != 300:
            continue
        if not up_token or not down_token or up_token == down_token:
            continue

        btc = btc_series(con, start, end)
        btc_ok, btc_open_ts, btc_open = btc_market_eligible(btc, start, end)
        if not btc_ok:
            continue

        cp = end - timedelta(seconds=CHECKPOINT_SECONDS)
        up = poly_checkpoint(con, str(market_id), "Up", start, cp)
        down = poly_checkpoint(con, str(market_id), "Down", start, cp)
        if not up or not down:
            continue
        if up["age_seconds"] > MAX_POLY_AGE_SECONDS or down["age_seconds"] > MAX_POLY_AGE_SECONDS:
            continue

        btc_cp = latest_btc_at_or_before(btc, cp)
        if not btc_cp:
            continue
        btc_ts, btc_price = btc_cp
        btc_age = (cp - btc_ts).total_seconds()
        if btc_age > MAX_BTC_CHECKPOINT_AGE_SECONDS:
            continue

        distance_bps = 10000.0 * (btc_price - btc_open) / btc_open

        # Frozen Hypothesis 2 only: 5m, T-30, -5 <= Binance distance < 0 bps.
        if not (-5.0 <= distance_bps < 0.0):
            continue

        y = 1 if outcome == "Up" else 0
        output.append({
            "market_id": str(market_id),
            "start_time": start,
            "end_time": end,
            "resolved_outcome": outcome,
            "y": y,
            "checkpoint_seconds": CHECKPOINT_SECONDS,
            "checkpoint_timestamp": cp,
            "btc_open_timestamp": btc_open_ts,
            "btc_open": btc_open,
            "btc_checkpoint_timestamp": btc_ts,
            "btc_checkpoint_price": btc_price,
            "btc_checkpoint_age_seconds": btc_age,
            "btc_distance_bps": distance_bps,
            "up_best_bid": up["best_bid"],
            "up_best_ask": up["best_ask"],
            "up_midpoint": up["midpoint"],
            "up_spread": up["best_ask"] - up["best_bid"],
            "down_best_bid": down["best_bid"],
            "down_best_ask": down["best_ask"],
            "down_midpoint": down["midpoint"],
            "poly_up_age_seconds": up["age_seconds"],
            "poly_down_age_seconds": down["age_seconds"],
            # Descriptive realized payoff minus observed quote; no fees/slippage included.
            "realized_midpoint_pnl_per_up_share": y - up["midpoint"],
            "realized_ask_cross_pnl_per_up_share_before_fees": y - up["best_ask"],
        })

    return pd.DataFrame(output)


def main():
    parser = argparse.ArgumentParser(
        description="Read-only execution-feasibility export for frozen Eyes-of-Heaven Hypothesis 2."
    )
    parser.add_argument("--since", required=True, help="Exclusive validation cohort start UTC")
    parser.add_argument("--db", type=Path, default=Path("data/research.duckdb"))
    parser.add_argument("--csv", type=Path, default=Path("h2-execution-rows.csv"))
    args = parser.parse_args()

    since = parse_utc(args.since)
    con = duckdb.connect(str(args.db), read_only=True)
    try:
        rows = build_rows(con, since)
    finally:
        con.close()

    args.csv.parent.mkdir(parents=True, exist_ok=True)
    rows.to_csv(args.csv, index=False)

    print("FROZEN H2 EXECUTION FEASIBILITY - READ ONLY")
    print(f"Since: {since.astimezone(timezone.utc).isoformat().replace('+00:00','Z')}")
    print(f"Rows: {len(rows)}")
    if not rows.empty:
        print(f"Observed Up rate: {rows.y.mean():.6f}")
        print(f"Mean Up midpoint: {rows.up_midpoint.mean():.6f}")
        print(f"Mean Up best ask: {rows.up_best_ask.mean():.6f}")
        print(f"Mean Up spread: {rows.up_spread.mean():.6f}")
        print(f"Mean realized midpoint P&L/share: {rows.realized_midpoint_pnl_per_up_share.mean():.6f}")
        print("Mean realized ask-cross P&L/share before fees/slippage: "
              f"{rows.realized_ask_cross_pnl_per_up_share_before_fees.mean():.6f}")
    print(f"CSV: {args.csv.resolve()}")


if __name__ == "__main__":
    main()
