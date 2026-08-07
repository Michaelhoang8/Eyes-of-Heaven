# Polymarket Bitcoin Up/Down Research

A small, read-only research collector and dashboard for completed Polymarket Bitcoin Up/Down markets. It records raw Binance trades/top-of-book updates and Polymarket top-of-book/trade events in UTC. It does not trade, place orders, interpolate missing observations, or run strategies.

## Install

Python 3.12 is recommended.

```text
python -m venv .venv
```

Windows:

```text
.venv\Scripts\activate
```

Mac/Linux:

```text
source .venv/bin/activate
```

Then install dependencies:

```text
pip install -r requirements.txt
```

## Run collector

```text
python -m collector.main
```

The collector continuously discovers active Bitcoin markets with explicit `Up` and `Down` outcomes, records the nearest-ending active market, and switches automatically as discovery changes. Stop it with Ctrl+C.

## Run dashboard

In a second terminal (with the same virtual environment active):

```text
streamlit run dashboard/app.py
```

The collector and dashboard should normally run in separate terminals. The dashboard contains completed-market history, a selectable market view, static charts, checkpoint values, distributions, descriptive checkpoint statistics, and calibration bins.

On Windows, the collector owns the live DuckDB file. It publishes Parquet files under `data/dashboard_snapshot` when it starts recording each new market, and Streamlit loads that snapshot into memory. Refresh the browser after a market transition to see newly completed data.

## Database

DuckDB is stored at `data/research.duckdb` and is created automatically. Tables are:

- `markets`: discovery metadata, explicit Up/Down token mapping, status, and resolution
- `bitcoin_events`: Binance trades and best bid/ask observations
- `polymarket_top_of_book`: compact top-of-book seeds, changes, and once-per-second state confirmations
- `polymarket_quotes`: preserved legacy raw quote history (new collection does not append to it)
- `polymarket_trades`: Polymarket trade messages
- `collector_status`: last-message times, reconnects, totals, malformed messages, duplicates, and last errors

Raw observations are append-only apart from exact duplicate IDs, which are ignored and counted. Missing values remain null. All timestamps are stored as UTC `TIMESTAMPTZ` values.

The compact representation is an additive migration: collector startup creates the new table and a compatibility view named `polymarket_quote_points`. Existing `polymarket_quotes` rows are never rewritten or deleted. The view retains legacy observations before each token's first compact state, then uses compact observations; markets with no compact data continue to use all preserved legacy rows.

## Checkpoint behavior

Each checkpoint selects the latest valid state confirmation at or before the checkpoint. It never looks forward and never interpolates. A Polymarket state confirmation more than five seconds old at the checkpoint is considered insufficiently close and returns null. Midpoints require both bid and ask. `last_changed_at` remains available separately from `state_as_of`, so an unchanged but recently confirmed quote is fresh without losing when its price actually changed.

## Test

```text
pytest -q
```

## Audit one completed market

The audit is read-only. It independently traces metadata, raw event windows, BTC open/close, official public Polymarket resolution metadata, token mapping, checkpoints, and dashboard values:

```text
python -m collector.audit_market --latest
```

Or select an exact market:

```text
python -m collector.audit_market --market-id MARKET_ID
```

Checkpoint quotes older than five seconds are reported as stale and returned as null.

## Evaluate a collector cohort

The cohort-quality command reads DuckDB without changing it, reports aligned 5m/15m interval coverage and separate Polymarket/BTC eligibility, and can optionally export its per-market rows:

```text
python -m collector.cohort_quality --since 2026-08-06T12:02:10.894Z
python -m collector.cohort_quality --since 2026-08-06T12:02:10.894Z --csv cohort-quality.csv
```

Ended markets whose official outcome was not captured during live discovery can be checked and backfilled from exact Gamma metadata. Preview first; the update changes only market resolution status and leaves every observation table untouched:

```text
python -m collector.backfill_resolutions --since 2026-08-06T12:02:10.894Z --dry-run
python -m collector.backfill_resolutions --since 2026-08-06T12:02:10.894Z
```

## Data sources and limitations

- Binance public Spot WebSocket: `BTCUSDT` aggregate trades and book ticker
- Polymarket Gamma API: market discovery and resolution metadata
- Polymarket public CLOB market WebSocket: book, price, best-bid/ask, and last-trade events

The dashboard only shows markets collected through completion; it does not backfill old raw tick data. Polymarket bid/ask sizes are available on book snapshots, but not consistently on every best-bid/ask message. Binance `bookTicker` does not include an exchange event timestamp, so its local UTC receipt time is used. Polymarket trade size is stored when the feed supplies it. Dashboard data refreshes from the collector snapshot at market transitions rather than tick by tick.
