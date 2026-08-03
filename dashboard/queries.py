from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from collector.markets import DASHBOARD_SNAPSHOT, DEFAULT_DB

CHECKPOINTS = (60, 30, 10, 5, 1)
MAX_CHECKPOINT_AGE_SECONDS = 5


def midpoint(bid, ask):
    if pd.isna(bid) or pd.isna(ask):
        return None
    return (float(bid) + float(ask)) / 2


def open_connection(path: str | Path = DEFAULT_DB, read_only: bool = True):
    try:
        return duckdb.connect(str(path), read_only=read_only)
    except duckdb.IOException:
        snapshot = Path(path).parent / DASHBOARD_SNAPSHOT.name
        tables = ("markets", "bitcoin_events", "polymarket_quotes", "polymarket_trades", "collector_status")
        if not all((snapshot / f"{table}.parquet").exists() for table in tables):
            raise
        con = duckdb.connect(":memory:")
        for table in tables:
            filename = str(snapshot / f"{table}.parquet").replace("'", "''")
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{filename}')")
        return con


def _checkpoint_sql(seconds: int, outcome: str) -> str:
    label = outcome.lower()
    return f"""
      (SELECT CASE WHEN q.best_bid IS NOT NULL AND q.best_ask IS NOT NULL
                    AND epoch(m.end_time - q.ts) - {seconds} <= {MAX_CHECKPOINT_AGE_SECONDS}
                   THEN (q.best_bid + q.best_ask) / 2 END
         FROM quote_points q WHERE q.market_id=m.market_id AND q.outcome='{outcome}'
          AND q.ts <= m.end_time - INTERVAL '{seconds} seconds'
         ORDER BY q.ts DESC LIMIT 1) AS {label}_{seconds}s"""


def completed_market_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    checkpoints = ",\n".join(_checkpoint_sql(sec, outcome) for outcome in ("Up", "Down") for sec in CHECKPOINTS)
    sql = f"""
    WITH btc_points AS (
      SELECT exchange_timestamp ts, COALESCE(price, (best_bid+best_ask)/2) price
      FROM bitcoin_events WHERE price IS NOT NULL OR (best_bid IS NOT NULL AND best_ask IS NOT NULL)
    ), quote_points AS (
      SELECT market_id, outcome, COALESCE(source_timestamp, receipt_timestamp) ts, best_bid, best_ask
      FROM polymarket_quotes
    )
    SELECT m.market_id, m.start_time, m.end_time, m.resolved_outcome winner,
      (SELECT price FROM btc_points b WHERE b.ts >= m.start_time AND b.ts <= m.end_time ORDER BY b.ts LIMIT 1) btc_open,
      (SELECT price FROM btc_points b WHERE b.ts >= m.start_time AND b.ts <= m.end_time ORDER BY b.ts DESC LIMIT 1) btc_close,
      (SELECT max(price) FROM btc_points b WHERE b.ts BETWEEN m.start_time AND m.end_time) btc_high,
      (SELECT min(price) FROM btc_points b WHERE b.ts BETWEEN m.start_time AND m.end_time) btc_low,
      (SELECT count(*) FROM bitcoin_events b WHERE b.exchange_timestamp BETWEEN m.start_time AND m.end_time) binance_count,
      ((SELECT count(*) FROM polymarket_quotes q WHERE q.market_id=m.market_id AND COALESCE(q.source_timestamp,q.receipt_timestamp) BETWEEN m.start_time AND m.end_time) +
       (SELECT count(*) FROM polymarket_trades t WHERE t.market_id=m.market_id AND COALESCE(t.source_timestamp,t.receipt_timestamp) BETWEEN m.start_time AND m.end_time)) polymarket_count,
      (SELECT max(gap) FROM (SELECT epoch(ts-lag(ts) OVER (ORDER BY ts)) gap FROM btc_points b
                             WHERE b.ts BETWEEN m.start_time AND m.end_time)) max_binance_gap_seconds,
      (SELECT max(gap) FROM (SELECT epoch(ts-lag(ts) OVER (ORDER BY ts)) gap FROM quote_points q
                             WHERE q.market_id=m.market_id AND q.ts BETWEEN m.start_time AND m.end_time)) max_polymarket_gap_seconds,
      {checkpoints}
    FROM markets m WHERE m.status='completed' OR m.resolved_outcome IS NOT NULL
    ORDER BY m.start_time DESC
    """
    df = con.execute(sql).df()
    if not df.empty:
        df["btc_change"] = df.btc_close - df.btc_open
        df["btc_change_pct"] = df.btc_change / df.btc_open * 100
        df["btc_change_bps"] = df.btc_change / df.btc_open * 10000
        df["btc_range"] = df.btc_high - df.btc_low
    return df


def market_series(con, market_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    btc = con.execute("""
      SELECT exchange_timestamp AS event_time, COALESCE(price,(best_bid+best_ask)/2) AS price
      FROM bitcoin_events, markets m WHERE m.market_id=? AND exchange_timestamp BETWEEN m.start_time AND m.end_time
        AND (price IS NOT NULL OR (best_bid IS NOT NULL AND best_ask IS NOT NULL)) ORDER BY event_time
    """, [market_id]).df()
    poly = con.execute("""
      SELECT COALESCE(source_timestamp,receipt_timestamp) AS event_time, outcome,
             CASE WHEN best_bid IS NOT NULL AND best_ask IS NOT NULL THEN (best_bid+best_ask)/2 END AS midpoint
      FROM polymarket_quotes q, markets m WHERE q.market_id=? AND m.market_id=q.market_id
        AND COALESCE(q.source_timestamp,q.receipt_timestamp) BETWEEN m.start_time AND m.end_time ORDER BY event_time
    """, [market_id]).df()
    return btc, poly


def checkpoint_table(con, market_id: str, seconds=(60, 30, 20, 10, 5, 3, 2, 1)) -> pd.DataFrame:
    market = con.execute("SELECT start_time,end_time FROM markets WHERE market_id=?", [market_id]).fetchone()
    if not market: return pd.DataFrame()
    start, end = market
    opening = con.execute("SELECT price FROM bitcoin_events WHERE exchange_timestamp BETWEEN ? AND ? AND price IS NOT NULL ORDER BY exchange_timestamp LIMIT 1", [start, end]).fetchone()
    rows = []
    for sec in seconds:
        target = con.execute("SELECT ?::TIMESTAMPTZ - ? * INTERVAL '1 second'", [end, sec]).fetchone()[0]
        btc = con.execute("SELECT price FROM bitcoin_events WHERE exchange_timestamp<=? AND exchange_timestamp>=? AND price IS NOT NULL ORDER BY exchange_timestamp DESC LIMIT 1", [target, start]).fetchone()
        values = {}
        for outcome in ("Up", "Down"):
            quote = con.execute("""SELECT (best_bid+best_ask)/2, epoch(?-COALESCE(source_timestamp,receipt_timestamp))
              FROM polymarket_quotes WHERE market_id=? AND outcome=? AND COALESCE(source_timestamp,receipt_timestamp)<=?
              AND best_bid IS NOT NULL AND best_ask IS NOT NULL ORDER BY COALESCE(source_timestamp,receipt_timestamp) DESC LIMIT 1""",
              [target, market_id, outcome, target]).fetchone()
            values[outcome] = quote[0] if quote and quote[1] <= MAX_CHECKPOINT_AGE_SECONDS else None
        price = btc[0] if btc else None; open_price = opening[0] if opening else None
        rows.append({"seconds_remaining": sec, "btc_price": price, "btc_vs_open": price-open_price if price is not None and open_price is not None else None,
                     "up_midpoint": values["Up"], "down_midpoint": values["Down"]})
    return pd.DataFrame(rows)


def checkpoint_outcome_statistics(summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for sec in CHECKPOINTS:
        col = f"up_{sec}s"; usable = summary.dropna(subset=[col, "winner"])
        above = usable[usable[col] > .5]; below = usable[usable[col] < .5]
        rows.append({"checkpoint": f"{sec}s", "usable_markets": len(usable), "average_up_midpoint": usable[col].mean(),
          "up_win_pct": (usable.winner == "Up").mean()*100 if len(usable) else None,
          "up_gt_50_resolved_up_pct": (above.winner == "Up").mean()*100 if len(above) else None,
          "up_lt_50_resolved_down_pct": (below.winner == "Down").mean()*100 if len(below) else None})
    return pd.DataFrame(rows)


def calibration_table(summary: pd.DataFrame, seconds: int) -> pd.DataFrame:
    col = f"up_{seconds}s"; usable = summary.dropna(subset=[col, "winner"]).copy()
    bins = pd.IntervalIndex.from_breaks([i/10 for i in range(11)], closed="left")
    usable["bin"] = pd.cut(usable[col], bins=[i/10 for i in range(11)], right=False, include_lowest=True)
    result = usable.groupby("bin", observed=False).agg(market_count=(col,"count"), average_up_midpoint=(col,"mean"),
      actual_up_win_pct=("winner",lambda x:(x=="Up").mean()*100 if len(x) else None)).reset_index()
    result["bin"] = result["bin"].astype(str)
    return result
