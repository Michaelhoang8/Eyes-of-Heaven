from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable

import duckdb
import websockets

from .markets import bump_status, is_query_interrupted, utcnow

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _ts(value, receipt):
    if not value:
        return receipt
    numeric = int(value)
    if numeric < 100_000_000_000:
        raise ValueError(f"Expected Unix milliseconds, received possible seconds value: {value}")
    return datetime.fromtimestamp(numeric / 1000, timezone.utc)


def _id(message: dict, token: str, suffix: str = "") -> str:
    canonical = json.dumps(message, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{token}:{suffix}:{canonical}".encode()).hexdigest()


def parse_polymarket(payload: str, market: dict, receipt=None) -> tuple[list[dict], list[dict]]:
    receipt = receipt or utcnow(); parsed = json.loads(payload)
    messages = parsed if isinstance(parsed, list) else [parsed]
    token_outcome = {market["up_token_id"]: "Up", market["down_token_id"]: "Down"}
    quotes, trades = [], []
    for msg in messages:
        kind = msg.get("event_type")
        if kind == "price_change":
            changes = msg.get("price_changes", [])
        else:
            changes = [msg]
        for i, item in enumerate(changes):
            token = str(item.get("asset_id", msg.get("asset_id", "")))
            if token not in token_outcome: continue
            source = _ts(msg.get("timestamp"), receipt)
            if kind in {"book", "best_bid_ask", "price_change"}:
                bids = msg.get("bids", []) if kind == "book" else []
                asks = msg.get("asks", []) if kind == "book" else []
                best_bid = max((float(x["price"]) for x in bids), default=None) if bids else _float(item.get("best_bid"))
                best_ask = min((float(x["price"]) for x in asks), default=None) if asks else _float(item.get("best_ask"))
                bid_size = next((_float(x["size"]) for x in bids if float(x["price"]) == best_bid), None)
                ask_size = next((_float(x["size"]) for x in asks if float(x["price"]) == best_ask), None)
                quotes.append({"event_id": _id(msg, token, str(i)), "source_timestamp": source, "receipt_timestamp": receipt,
                    "market_id": market["market_id"], "token_id": token, "outcome": token_outcome[token], "event_type": kind,
                    "best_bid": best_bid, "best_ask": best_ask, "bid_size": bid_size, "ask_size": ask_size})
            elif kind == "last_trade_price":
                trades.append({"event_id": _id(msg, token), "source_timestamp": source, "receipt_timestamp": receipt,
                    "market_id": market["market_id"], "token_id": token, "outcome": token_outcome[token],
                    "event_type": "trade", "price": float(msg["price"]), "size": _float(msg.get("size"))})
    return quotes, trades


def _float(value):
    return float(value) if value not in (None, "") else None


def insert_events(con, quotes, trades) -> tuple[int, int]:
    inserted = duplicates = 0
    for table, rows in (("polymarket_quotes", quotes), ("polymarket_trades", trades)):
        for row in rows:
            exists = con.execute(f"SELECT count(*) FROM {table} WHERE event_id=?", [row["event_id"]]).fetchone()[0]
            columns = ("event_id", "source_timestamp", "receipt_timestamp", "market_id", "token_id", "outcome",
                       "event_type", "best_bid", "best_ask", "bid_size", "ask_size") if table == "polymarket_quotes" else (
                       "event_id", "source_timestamp", "receipt_timestamp", "market_id", "token_id", "outcome",
                       "event_type", "price", "size")
            marks = ",".join("?" for _ in columns)
            con.execute(f"INSERT OR IGNORE INTO {table} VALUES ({marks})", [row[k] for k in columns])
            inserted += int(not exists); duplicates += int(bool(exists))
    return inserted, duplicates


async def collect_polymarket(con: duckdb.DuckDBPyConnection, market: dict, stop: asyncio.Event, *,
                             connector: Callable[..., Awaitable] = websockets.connect, base_delay=1):
    delay = base_delay
    while not stop.is_set():
        try:
            async with connector(URL, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"assets_ids": [market["up_token_id"], market["down_token_id"]],
                                          "type": "market", "custom_feature_enabled": True}))
                delay = base_delay
                async for raw in ws:
                    if stop.is_set(): return
                    try:
                        quotes, trades = parse_polymarket(raw, market)
                        added, dup = insert_events(con, quotes, trades)
                        bump_status(con, "polymarket", events=added, duplicates=dup,
                                    message_time=utcnow() if added else None)
                    except Exception as exc:
                        if stop.is_set() or is_query_interrupted(exc):
                            return
                        logging.exception("Malformed Polymarket message")
                        bump_status(con, "polymarket", malformed=1, error=str(exc))
        except asyncio.CancelledError: raise
        except Exception as exc:
            if stop.is_set() or is_query_interrupted(exc):
                return
            logging.warning("Polymarket websocket disconnected; reconnecting in %.1fs: %s", delay, exc)
            bump_status(con, "polymarket", reconnects=1, error=str(exc))
            try: await asyncio.wait_for(stop.wait(), timeout=delay)
            except (TimeoutError, asyncio.TimeoutError): pass
            delay = min(delay * 2, 30)
