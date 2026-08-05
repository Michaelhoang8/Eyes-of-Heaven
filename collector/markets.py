from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import httpx

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "research.duckdb"
DASHBOARD_SNAPSHOT = ROOT / "data" / "dashboard_snapshot"
GAMMA_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_EVENT_SLUG_URL = "https://gamma-api.polymarket.com/events/slug/{slug}"
DATABASE_WRITE_LOCK = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
  market_id VARCHAR PRIMARY KEY, event_id VARCHAR, slug VARCHAR, question VARCHAR,
  start_time TIMESTAMPTZ, end_time TIMESTAMPTZ, up_token_id VARCHAR NOT NULL,
  down_token_id VARCHAR NOT NULL, status VARCHAR NOT NULL, discovered_at TIMESTAMPTZ NOT NULL,
  resolved_outcome VARCHAR, updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS bitcoin_events (
  event_id VARCHAR PRIMARY KEY, exchange_timestamp TIMESTAMPTZ NOT NULL,
  receipt_timestamp TIMESTAMPTZ NOT NULL, event_type VARCHAR NOT NULL,
  price DOUBLE, quantity DOUBLE, best_bid DOUBLE, best_ask DOUBLE
);
CREATE TABLE IF NOT EXISTS polymarket_quotes (
  event_id VARCHAR PRIMARY KEY, source_timestamp TIMESTAMPTZ,
  receipt_timestamp TIMESTAMPTZ NOT NULL, market_id VARCHAR NOT NULL,
  token_id VARCHAR NOT NULL, outcome VARCHAR NOT NULL, event_type VARCHAR NOT NULL,
  best_bid DOUBLE, best_ask DOUBLE, bid_size DOUBLE, ask_size DOUBLE
);
CREATE TABLE IF NOT EXISTS polymarket_trades (
  event_id VARCHAR PRIMARY KEY, source_timestamp TIMESTAMPTZ,
  receipt_timestamp TIMESTAMPTZ NOT NULL, market_id VARCHAR NOT NULL,
  token_id VARCHAR NOT NULL, outcome VARCHAR NOT NULL, event_type VARCHAR NOT NULL,
  price DOUBLE NOT NULL, size DOUBLE
);
CREATE TABLE IF NOT EXISTS collector_status (
  source VARCHAR PRIMARY KEY, last_message_timestamp TIMESTAMPTZ,
  reconnect_count BIGINT NOT NULL DEFAULT 0, total_events BIGINT NOT NULL DEFAULT 0,
  malformed_messages BIGINT NOT NULL DEFAULT 0, duplicate_messages BIGINT NOT NULL DEFAULT 0,
  last_error VARCHAR, updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS btc_time_idx ON bitcoin_events(exchange_timestamp);
CREATE INDEX IF NOT EXISTS quote_market_time_idx ON polymarket_quotes(market_id, source_timestamp);
CREATE INDEX IF NOT EXISTS trade_market_time_idx ON polymarket_trades(market_id, source_timestamp);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def is_query_interrupted(exc: BaseException) -> bool:
    """DuckDB raises RuntimeError when Ctrl+C interrupts an active query."""
    return "query interrupted" in str(exc).lower()


def parse_utc(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timezone-naive timestamp is not allowed: {value!r}")
    return parsed.astimezone(timezone.utc)


def interval_start(raw: dict[str, Any]) -> datetime | None:
    if raw.get("eventStartTime"):
        return parse_utc(raw["eventStartTime"])
    slug = str(raw.get("slug") or "")
    match = re.search(r"-(?:5|15)m-(\d{10})$", slug)
    if match:
        return datetime.fromtimestamp(int(match.group(1)), timezone.utc)
    return parse_utc(raw.get("startDate") or raw.get("startDateIso"))


def connect_db(path: str | Path = DEFAULT_DB) -> duckdb.DuckDBPyConnection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(SCHEMA)
    return con


def _array(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        parsed = json.loads(value)
        return [str(v) for v in parsed]
    return []


def map_outcome_tokens(market: dict[str, Any]) -> tuple[str, str]:
    outcomes = _array(market.get("outcomes"))
    tokens = _array(market.get("clobTokenIds"))
    if len(outcomes) != len(tokens):
        raise ValueError("Outcome and token arrays differ in length")
    mapping = {label.strip().lower(): token for label, token in zip(outcomes, tokens)}
    if "up" not in mapping or "down" not in mapping:
        raise ValueError(f"Explicit Up/Down outcomes unavailable: {outcomes!r}")
    if mapping["up"] == mapping["down"]:
        raise ValueError("Up and Down token IDs must be different")
    return mapping["up"], mapping["down"]


def normalize_market(raw: dict[str, Any], discovered_at: datetime | None = None) -> dict[str, Any]:
    up, down = map_outcome_tokens(raw)
    closed = bool(raw.get("closed"))
    active = bool(raw.get("active"))
    prices = _array(raw.get("outcomePrices"))
    outcomes = _array(raw.get("outcomes"))
    winner = None
    if closed and len(prices) == len(outcomes):
        winners = [o for o, p in zip(outcomes, prices) if float(p) >= 0.999]
        winner = winners[0].title() if len(winners) == 1 else None
    return {
        "market_id": str(raw.get("conditionId") or raw["id"]),
        "event_id": str(raw.get("events", [{}])[0].get("id")) if raw.get("events") else None,
        "slug": raw.get("slug"), "question": raw.get("question"),
        "start_time": interval_start(raw),
        "end_time": parse_utc(raw.get("endDate") or raw.get("endDateIso")),
        "up_token_id": up, "down_token_id": down,
        "status": "completed" if closed else ("active" if active else "inactive"),
        "discovered_at": discovered_at or utcnow(), "resolved_outcome": winner,
        "updated_at": utcnow(),
    }


def is_bitcoin_up_down(raw: dict[str, Any]) -> bool:
    text = f"{raw.get('question', '')} {raw.get('slug', '')}".lower()
    if "bitcoin" not in text and "btc" not in text:
        return False
    try:
        map_outcome_tokens(raw)
        return True
    except (ValueError, TypeError, json.JSONDecodeError):
        return False


async def discover_markets(client: httpx.AsyncClient | None = None, *, active: bool = True) -> list[dict[str, Any]]:
    owned = client is None
    client = client or httpx.AsyncClient(timeout=20)
    try:
        response = await client.get(GAMMA_URL, params={"active": str(active).lower(), "closed": "false" if active else "true", "limit": 500})
        response.raise_for_status()
        candidates = list(response.json())
        # High-frequency crypto markets use predictable event slugs and may not occur
        # in the first page of the very large active-market listing.
        if active:
            now = int(time.time())
            for interval, prefix in ((300, "btc-updown-5m"), (900, "btc-updown-15m")):
                floor = now - now % interval
                for offset in (-1, 0, 1):
                    slug = f"{prefix}-{floor + offset * interval}"
                    event_response = await client.get(GAMMA_EVENT_SLUG_URL.format(slug=slug))
                    if event_response.status_code == 404:
                        continue
                    event_response.raise_for_status()
                    event = event_response.json()
                    for item in event.get("markets", []):
                        item.setdefault("events", [{"id": event.get("id")}])
                        candidates.append(item)
        unique = {}
        for item in candidates:
            if is_bitcoin_up_down(item):
                normalized = normalize_market(item)
                unique[normalized["market_id"]] = normalized
        return list(unique.values())
    finally:
        if owned:
            await client.aclose()


def upsert_market(con: duckdb.DuckDBPyConnection, market: dict[str, Any]) -> None:
    con.execute("""
      INSERT INTO markets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (market_id) DO UPDATE SET event_id=excluded.event_id, slug=excluded.slug,
      question=excluded.question, start_time=excluded.start_time, end_time=excluded.end_time,
      status=excluded.status, resolved_outcome=COALESCE(excluded.resolved_outcome, markets.resolved_outcome),
      updated_at=excluded.updated_at
    """, [market[k] for k in ("market_id", "event_id", "slug", "question", "start_time", "end_time",
          "up_token_id", "down_token_id", "status", "discovered_at", "resolved_outcome", "updated_at")])


def repair_interval_starts(con: duckdb.DuckDBPyConnection) -> int:
    """Repair legacy high-frequency rows whose start_time stored listing time."""
    rows = con.execute("SELECT market_id,slug,start_time FROM markets").fetchall()
    repaired = 0
    for market_id, slug, current in rows:
        match = re.search(r"-(?:5|15)m-(\d{10})$", str(slug or ""))
        if not match:
            continue
        correct = datetime.fromtimestamp(int(match.group(1)), timezone.utc)
        if parse_utc(current) != correct:
            con.execute("UPDATE markets SET start_time=?,updated_at=? WHERE market_id=?", [correct,utcnow(),market_id])
            repaired += 1
    return repaired


def bump_status(con: duckdb.DuckDBPyConnection, source: str, *, events: int = 0, reconnects: int = 0,
                malformed: int = 0, duplicates: int = 0, error: str | None = None,
                message_time: datetime | None = None) -> None:
    now = utcnow()
    con.execute("""
      INSERT INTO collector_status VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT (source) DO UPDATE SET
        last_message_timestamp=COALESCE(excluded.last_message_timestamp, collector_status.last_message_timestamp),
        reconnect_count=collector_status.reconnect_count+excluded.reconnect_count,
        total_events=collector_status.total_events+excluded.total_events,
        malformed_messages=collector_status.malformed_messages+excluded.malformed_messages,
        duplicate_messages=collector_status.duplicate_messages+excluded.duplicate_messages,
        last_error=COALESCE(excluded.last_error, collector_status.last_error), updated_at=excluded.updated_at
    """, [source, message_time, reconnects, events, malformed, duplicates, error, now])


def publish_dashboard_snapshot(con: duckdb.DuckDBPyConnection,
                               directory: str | Path = DASHBOARD_SNAPSHOT) -> None:
    """Publish atomic Parquet snapshots without sharing the live DuckDB file."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    tables = ("markets", "bitcoin_events", "polymarket_quotes", "polymarket_trades", "collector_status")
    for table in tables:
        target = directory / f"{table}.parquet"
        temporary = directory / f".{table}.{os.getpid()}.tmp.parquet"
        escaped = str(temporary).replace("'", "''")
        con.execute(f"COPY (SELECT * FROM {table}) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        os.replace(temporary, target)
