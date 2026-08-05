from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable

import duckdb
import websockets

from .markets import DATABASE_WRITE_LOCK, bump_status, is_query_interrupted, utcnow

URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker"


def ms_time(value: int | str | None, fallback=None):
    if value is None:
        return fallback
    numeric = int(value)
    if numeric < 100_000_000_000:
        raise ValueError(f"Expected Unix milliseconds, received possible seconds value: {value}")
    return datetime.fromtimestamp(numeric / 1000, timezone.utc)


def parse_binance(payload: str, receipt=None) -> dict:
    wrapper = json.loads(payload); data = wrapper.get("data", wrapper); stream = wrapper.get("stream", "")
    receipt = receipt or utcnow()
    if data.get("e") == "aggTrade" or "aggTrade" in stream:
        return {"event_id": f"trade:{data['a']}", "exchange_timestamp": ms_time(data.get("T") or data.get("E"), receipt),
                "receipt_timestamp": receipt, "event_type": "trade", "price": float(data["p"]),
                "quantity": float(data["q"]), "best_bid": None, "best_ask": None}
    if "bookTicker" in stream or all(k in data for k in ("u", "b", "a")):
        return {"event_id": f"book:{data['u']}", "exchange_timestamp": ms_time(data.get("E"), receipt),
                "receipt_timestamp": receipt, "event_type": "book_ticker", "price": None, "quantity": None,
                "best_bid": float(data["b"]), "best_ask": float(data["a"])}
    raise ValueError("Unsupported Binance message")


def insert_event(con: duckdb.DuckDBPyConnection, event: dict) -> bool:
    before = con.execute("SELECT count(*) FROM bitcoin_events WHERE event_id=?", [event["event_id"]]).fetchone()[0]
    con.execute("INSERT OR IGNORE INTO bitcoin_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [event[k] for k in ("event_id", "exchange_timestamp", "receipt_timestamp", "event_type",
                                    "price", "quantity", "best_bid", "best_ask")])
    return before == 0


def persist_event(con: duckdb.DuckDBPyConnection, event: dict) -> None:
    with DATABASE_WRITE_LOCK:
        fresh = insert_event(con, event)
        bump_status(con, "binance", events=int(fresh), duplicates=int(not fresh),
                    message_time=event["exchange_timestamp"])


def bump_status_safely(con, *args, **kwargs) -> None:
    with DATABASE_WRITE_LOCK:
        bump_status(con, *args, **kwargs)


async def run_database_call(function, *args, **kwargs):
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


async def collect_binance(con: duckdb.DuckDBPyConnection, stop: asyncio.Event, *,
                          connector: Callable[..., Awaitable] = websockets.connect, base_delay: float = 1) -> None:
    delay = base_delay
    writer_con = con.cursor()
    try:
        while not stop.is_set():
            try:
                async with connector(URL, ping_interval=20, ping_timeout=20) as ws:
                    delay = base_delay
                    async for raw in ws:
                        if stop.is_set(): return
                        try:
                            event = parse_binance(raw)
                            await run_database_call(persist_event, writer_con, event)
                        except Exception as exc:
                            if stop.is_set() or is_query_interrupted(exc):
                                return
                            logging.exception("Malformed Binance message")
                            await run_database_call(
                                bump_status_safely, writer_con, "binance", malformed=1, error=str(exc))
            except asyncio.CancelledError: raise
            except Exception as exc:
                if stop.is_set() or is_query_interrupted(exc):
                    return
                logging.warning("Binance websocket disconnected; reconnecting in %.1fs: %s", delay, exc)
                await run_database_call(
                    bump_status_safely, writer_con, "binance", reconnects=1, error=str(exc))
                try: await asyncio.wait_for(stop.wait(), timeout=delay)
                except (TimeoutError, asyncio.TimeoutError): pass
                delay = min(delay * 2, 30)
    finally:
        writer_con.close()
