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
            if kind in {"book", "best_bid_ask"}:
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


def insert_top_states(con, rows) -> tuple[int, int]:
    if not rows:
        return 0, 0
    columns = ("event_id", "market_id", "token_id", "outcome", "event_type",
               "best_bid", "best_ask", "last_changed_at", "state_as_of")
    frame = pd.DataFrame.from_records(rows, columns=columns)
    con.register("_top_of_book_batch", frame)
    try:
        added = len(con.execute("""
            INSERT INTO polymarket_top_of_book SELECT * FROM _top_of_book_batch
            ON CONFLICT DO NOTHING RETURNING event_id
        """).fetchall())
    finally:
        con.unregister("_top_of_book_batch")
    return added, len(rows) - added


def _book_top(msg: dict) -> tuple[float | None, float | None]:
    bids, asks = msg.get("bids", []), msg.get("asks", [])
    return (max((float(row["price"]) for row in bids), default=None),
            min((float(row["price"]) for row in asks), default=None))


def persist_compact_messages(con, market: dict, messages, state: dict) -> None:
    token_outcome = {market["up_token_id"]: "Up", market["down_token_id"]: "Down"}
    top_rows, trades = [], []
    malformed = 0
    last_error = None
    for payload, receipt, generation in messages:
        try:
            if state.get("_generation") != generation:
                state.clear()
                state["_generation"] = generation
            parsed = json.loads(payload)
            for msg in parsed if isinstance(parsed, list) else [parsed]:
                kind = msg.get("event_type")
                token = str(msg.get("asset_id", ""))
                if token not in token_outcome:
                    continue
                source = _ts(msg.get("timestamp"), receipt)
                if kind == "book" and token not in state:
                    best_bid, best_ask = _book_top(msg)
                    current = {"best_bid": best_bid, "best_ask": best_ask,
                               "last_changed_at": source, "state_as_of": receipt,
                               "last_snapshot_second": None}
                    state[token] = current
                    top_rows.append({"event_id": _id(msg, token, "book_seed"),
                        "market_id": market["market_id"], "token_id": token,
                        "outcome": token_outcome[token], "event_type": "book_seed", **current})
                    top_rows[-1].pop("last_snapshot_second")
                elif kind == "best_bid_ask" and token in state:
                    current = state[token]
                    current.update(best_bid=_float(msg.get("best_bid")),
                                   best_ask=_float(msg.get("best_ask")),
                                   last_changed_at=source, state_as_of=receipt)
                    top_rows.append({"event_id": _id(msg, token),
                        "market_id": market["market_id"], "token_id": token,
                        "outcome": token_outcome[token], "event_type": "best_bid_ask",
                        "best_bid": current["best_bid"], "best_ask": current["best_ask"],
                        "last_changed_at": source, "state_as_of": receipt})
                elif kind == "last_trade_price":
                    trades.append({"event_id": _id(msg, token), "source_timestamp": source,
                        "receipt_timestamp": receipt, "market_id": market["market_id"],
                        "token_id": token, "outcome": token_outcome[token], "event_type": "trade",
                        "price": float(msg["price"]), "size": _float(msg.get("size"))})
                # price_change and recurring book messages are intentionally not persisted.
        except Exception as exc:
            if is_query_interrupted(exc):
                raise
            malformed += 1
            last_error = str(exc)
            logging.exception("Malformed Polymarket message")
    with DATABASE_WRITE_LOCK:
        added, dup = insert_top_states(con, top_rows)
        trade_added, trade_dup = insert_events(con, [], trades)
        bump_status(con, "polymarket", events=added + trade_added,
                    duplicates=dup + trade_dup, malformed=malformed, error=last_error,
                    message_time=max((receipt for _, receipt, _ in messages), default=None))


def persist_state_snapshots(con, market: dict, state: dict, as_of: datetime) -> None:
    second = as_of.replace(microsecond=0)
    rows = []
    outcomes = {market["up_token_id"]: "Up", market["down_token_id"]: "Down"}
    for token, current in list(state.items()):
        if token.startswith("_") or current["last_snapshot_second"] == second:
            continue
        current["last_snapshot_second"] = second
        current["state_as_of"] = as_of
        event_id = hashlib.sha256(
            f"{market['market_id']}:{token}:snapshot:{second.isoformat()}".encode()).hexdigest()
        rows.append({"event_id": event_id, "market_id": market["market_id"],
            "token_id": token, "outcome": outcomes[token], "event_type": "snapshot",
            "best_bid": current["best_bid"], "best_ask": current["best_ask"],
            "last_changed_at": current["last_changed_at"], "state_as_of": as_of})
    if rows:
        with DATABASE_WRITE_LOCK:
            added, dup = insert_top_states(con, rows)
            bump_status(con, "polymarket", events=added, duplicates=dup,
                        message_time=as_of if added else None)


def bump_status_safely(con, *args, **kwargs) -> None:
    with DATABASE_WRITE_LOCK:
        bump_status(con, *args, **kwargs)


async def read_messages(websocket, queue: asyncio.Queue, market_id: str, stop: asyncio.Event,
                        *, receipt_clock=utcnow, queue_warning_depth=QUEUE_WARNING_DEPTH,
                        queue_stats: dict | None = None, subscription_generation: int = 0) -> int:
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
            queue.put_nowait((raw, receipt, subscription_generation))
        except asyncio.QueueFull:
            if not saturated:
                logging.error(
                    "Polymarket queue saturated; no observations dropped market_id=%s depth=%d",
                    market_id, queue.maxsize,
                )
                saturated = True
            await queue.put((raw, receipt, subscription_generation))
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
    state = {}
    while True:
        try:
            messages = [await asyncio.wait_for(queue.get(), timeout=.25)]
        except asyncio.TimeoutError:
            await asyncio.to_thread(persist_state_snapshots, con, market, state, utcnow())
            continue
        while len(messages) < WRITE_BATCH_SIZE:
            try:
                messages.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        persistence = asyncio.create_task(asyncio.to_thread(
            persist_compact_messages, con, market, messages, state))
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
        # While draining a backlog, never claim that an older queued state was
        # current at wall-clock writer time. The newest reader receipt in this
        # batch is the latest defensible state confirmation.
        await asyncio.to_thread(
            persist_state_snapshots, con, market, state,
            max(receipt for _, receipt, _ in messages))


async def collect_polymarket(con: duckdb.DuckDBPyConnection, market: dict, stop: asyncio.Event, *,
                             connector: Callable[..., Awaitable] = websockets.connect, base_delay=1,
                             queue_maxsize=QUEUE_MAXSIZE):
    delay = base_delay
    queue = asyncio.Queue(maxsize=queue_maxsize)
    queue_stats = {"maximum_depth": 0}
    subscription_generation = 0
    writer_con = con.cursor()
    writer_task = asyncio.create_task(write_messages(writer_con, market, queue))
    try:
        while not stop.is_set():
            try:
                # Polymarket requires text PING/PONG heartbeats. Disable the websockets
                # control-frame heartbeat so only the protocol-required mechanism runs.
                async with connector(URL, ping_interval=None) as ws:
                    subscription_generation += 1
                    await ws.send(json.dumps({"assets_ids": [market["up_token_id"], market["down_token_id"]],
                                              "type": "market", "custom_feature_enabled": True}))
                    delay = base_delay
                    heartbeat_task = asyncio.create_task(application_heartbeat(ws))
                    reader_task = asyncio.create_task(read_messages(
                        ws, queue, market["market_id"], stop, queue_stats=queue_stats,
                        subscription_generation=subscription_generation))
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
