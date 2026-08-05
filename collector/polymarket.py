from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable

import duckdb
import pandas as pd
import websockets

from .markets import DATABASE_WRITE_LOCK, bump_status, is_query_interrupted, utcnow

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
QUEUE_MAXSIZE = 10_000
QUEUE_WARNING_DEPTH = 8_000
WRITE_BATCH_SIZE = 250


async def application_heartbeat(websocket, interval: float = 10) -> None:
    while True:
        await asyncio.sleep(interval)
        await websocket.send("PING")


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
        if not rows:
            continue
        columns = ("event_id", "source_timestamp", "receipt_timestamp", "market_id", "token_id", "outcome",
                   "event_type", "best_bid", "best_ask", "bid_size", "ask_size") if table == "polymarket_quotes" else (
                   "event_id", "source_timestamp", "receipt_timestamp", "market_id", "token_id", "outcome",
                   "event_type", "price", "size")
        relation = f"_{table}_batch"
        frame = pd.DataFrame.from_records(rows, columns=columns)
        con.register(relation, frame)
        try:
            added = len(con.execute(
                f"INSERT INTO {table} SELECT * FROM {relation} "
                "ON CONFLICT DO NOTHING RETURNING event_id"
            ).fetchall())
        finally:
            con.unregister(relation)
        inserted += added
        duplicates += len(rows) - added
    return inserted, duplicates


def persist_messages(con, market: dict, messages: list[tuple[str, datetime]]) -> None:
    quotes, trades = [], []
    malformed = 0
    last_error = None
    for payload, receipt in messages:
        try:
            message_quotes, message_trades = parse_polymarket(payload, market, receipt)
            quotes.extend(message_quotes)
            trades.extend(message_trades)
        except Exception as exc:
            if is_query_interrupted(exc):
                raise
            malformed += 1
            last_error = str(exc)
            logging.exception("Malformed Polymarket message")
    with DATABASE_WRITE_LOCK:
        added, dup = insert_events(con, quotes, trades)
        message_time = max((receipt for _, receipt in messages), default=None) if added else None
        bump_status(con, "polymarket", events=added, duplicates=dup, malformed=malformed,
                    error=last_error, message_time=message_time)


def bump_status_safely(con, *args, **kwargs) -> None:
    with DATABASE_WRITE_LOCK:
        bump_status(con, *args, **kwargs)


async def read_messages(websocket, queue: asyncio.Queue, market_id: str, stop: asyncio.Event,
                        *, receipt_clock=utcnow, queue_warning_depth=QUEUE_WARNING_DEPTH,
                        queue_stats: dict | None = None) -> int:
    """Drain the socket promptly; database persistence is owned by the writer task."""
    maximum_depth = queue.qsize()
    warned = maximum_depth >= queue_warning_depth
    saturated = False
    async for raw in websocket:
        if stop.is_set():
            return maximum_depth
        receipt = receipt_clock()
        if raw == "PONG":
            continue
        try:
            queue.put_nowait((raw, receipt))
        except asyncio.QueueFull:
            if not saturated:
                logging.error(
                    "Polymarket queue saturated; no observations dropped market_id=%s depth=%d",
                    market_id, queue.maxsize,
                )
                saturated = True
            await queue.put((raw, receipt))
        depth = queue.qsize()
        maximum_depth = max(maximum_depth, depth)
        if queue_stats is not None:
            queue_stats["maximum_depth"] = max(queue_stats.get("maximum_depth", 0), depth)
        if depth >= queue_warning_depth and not warned:
            logging.warning("Polymarket queue depth high market_id=%s depth=%d", market_id, depth)
            warned = True
        elif depth < queue_warning_depth:
            warned = False
        if depth < queue.maxsize:
            saturated = False
    return maximum_depth


async def write_messages(con, market: dict, queue: asyncio.Queue) -> None:
    """Persist queued frames in order without blocking the asyncio event loop."""
    while True:
        messages = [await queue.get()]
        while len(messages) < WRITE_BATCH_SIZE:
            try:
                messages.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        persistence = asyncio.create_task(asyncio.to_thread(
            persist_messages, con, market, messages))
        try:
            await asyncio.shield(persistence)
        except asyncio.CancelledError:
            # A running thread cannot be cancelled safely. Finish this one write;
            # remaining queued frames are explicitly abandoned by the owner.
            await persistence
            raise
        except Exception as exc:
            if is_query_interrupted(exc):
                return
            logging.exception("Malformed Polymarket message")
            await asyncio.to_thread(
                bump_status_safely, con, "polymarket", malformed=1, error=str(exc))
        finally:
            for _ in messages:
                queue.task_done()


async def collect_polymarket(con: duckdb.DuckDBPyConnection, market: dict, stop: asyncio.Event, *,
                             connector: Callable[..., Awaitable] = websockets.connect, base_delay=1,
                             queue_maxsize=QUEUE_MAXSIZE):
    delay = base_delay
    queue = asyncio.Queue(maxsize=queue_maxsize)
    queue_stats = {"maximum_depth": 0}
    writer_con = con.cursor()
    writer_task = asyncio.create_task(write_messages(writer_con, market, queue))
    try:
        while not stop.is_set():
            try:
                # Polymarket requires text PING/PONG heartbeats. Disable the websockets
                # control-frame heartbeat so only the protocol-required mechanism runs.
                async with connector(URL, ping_interval=None) as ws:
                    await ws.send(json.dumps({"assets_ids": [market["up_token_id"], market["down_token_id"]],
                                              "type": "market", "custom_feature_enabled": True}))
                    delay = base_delay
                    heartbeat_task = asyncio.create_task(application_heartbeat(ws))
                    reader_task = asyncio.create_task(read_messages(
                        ws, queue, market["market_id"], stop, queue_stats=queue_stats))
                    try:
                        done, _ = await asyncio.wait(
                            (reader_task, writer_task), return_when=asyncio.FIRST_COMPLETED)
                        if writer_task in done:
                            writer_task.result()
                            raise RuntimeError("Polymarket writer stopped unexpectedly")
                        maximum_depth = reader_task.result()
                        logging.info("Polymarket websocket reader ended market_id=%s maximum_queue_depth=%d",
                                     market["market_id"], maximum_depth)
                    finally:
                        reader_task.cancel()
                        heartbeat_task.cancel()
                        await asyncio.gather(reader_task, heartbeat_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if stop.is_set() or is_query_interrupted(exc):
                    return
                logging.warning("Polymarket websocket disconnected; reconnecting in %.1fs: %s", delay, exc)
                await asyncio.to_thread(
                    bump_status_safely, writer_con, "polymarket", reconnects=1, error=str(exc))
                try: await asyncio.wait_for(stop.wait(), timeout=delay)
                except (TimeoutError, asyncio.TimeoutError): pass
                delay = min(delay * 2, 30)
    finally:
        if not writer_task.done() and queue.qsize():
            logging.info("Polymarket shutdown draining queued observations market_id=%s count=%d",
                         market["market_id"], queue.qsize())
            await queue.join()
        writer_task.cancel()
        results = await asyncio.gather(writer_task, return_exceptions=True)
        abandoned = queue.qsize()
        if abandoned:
            logging.error("Polymarket shutdown abandoned queued observations after writer failure "
                          "market_id=%s count=%d", market["market_id"], abandoned)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                logging.error("Polymarket writer shutdown failure market_id=%s",
                              market["market_id"],
                              exc_info=(type(result), result, result.__traceback__))
        logging.info("Polymarket collector maximum queue depth market_id=%s depth=%d",
                     market["market_id"], queue_stats["maximum_depth"])
        writer_con.close()
