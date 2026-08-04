from __future__ import annotations

import asyncio
import logging
import signal
from datetime import datetime, timedelta

from .bitcoin import collect_binance
from .markets import connect_db, discover_markets, is_query_interrupted, publish_dashboard_snapshot, repair_interval_starts, upsert_market, utcnow
from .polymarket import collect_polymarket


def log_task_outcome(task: asyncio.Task, label: str, *, market_id: str | None = None) -> BaseException | None:
    if getattr(task, "_collector_outcome_logged", False):
        return None
    task._collector_outcome_logged = True
    identity = f" market_id={market_id}" if market_id else ""
    if task.cancelled():
        logging.info("%s task cancelled%s", label, identity)
        return None
    exception = task.exception()
    if exception is not None:
        logging.error("%s task raised%s", label, identity,
                      exc_info=(type(exception), exception, exception.__traceback__))
        return exception
    logging.warning("%s task returned normally%s", label, identity)
    return None


def supervise_binance_task(task: asyncio.Task) -> None:
    if not task.done():
        return
    exception = log_task_outcome(task, "Binance collector")
    failure = RuntimeError("Binance collector task stopped unexpectedly")
    if exception is not None:
        raise failure from exception
    raise failure


def transition_stop(stop: asyncio.Event, reason: str) -> None:
    if not stop.is_set():
        logging.info("Collector stop event set reason=%s UTC=%s", reason, utcnow().isoformat())
        stop.set()


def warn_event_loop_lag(expected: float, actual: float, threshold: float = 1) -> None:
    lag = actual - expected
    if lag > threshold:
        logging.warning("Event loop wake lag %.3fs exceeded %.3fs threshold", lag, threshold)


async def reconcile_polymarket_tasks(con, stop: asyncio.Event, tasks: dict[str, asyncio.Task],
                                     eligible: list[dict]) -> bool:
    """Make the running Polymarket tasks match the currently eligible markets."""
    markets = {market["market_id"]: market for market in eligible}
    stale_ids = [market_id for market_id, task in tasks.items()
                 if market_id not in markets or task.done()]
    stale_tasks = [(market_id, tasks.pop(market_id)) for market_id in stale_ids]
    for _, task in stale_tasks:
        if not task.done():
            task.cancel()
    if stale_tasks:
        await asyncio.gather(*(task for _, task in stale_tasks), return_exceptions=True)
        for market_id, task in stale_tasks:
            log_task_outcome(task, "Polymarket", market_id=market_id)

    started = False
    for market_id, market in markets.items():
        if market_id not in tasks:
            logging.info("Recording Polymarket market: %s", market["question"])
            tasks[market_id] = asyncio.create_task(collect_polymarket(con, market, stop))
            started = True
    return bool(stale_tasks) or started


async def cancel_polymarket_tasks(tasks: dict[str, asyncio.Task]) -> None:
    active = list(tasks.items())
    for _, task in active:
        task.cancel()
    if active:
        await asyncio.gather(*(task for _, task in active), return_exceptions=True)
        for market_id, task in active:
            log_task_outcome(task, "Polymarket", market_id=market_id)
    tasks.clear()


def parsed_time(value):
    if not value: return None
    if isinstance(value, datetime): return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def eligible_markets(markets: list[dict], now: datetime) -> list[dict]:
    return [market for market in markets
            if (not parsed_time(market["start_time"]) or parsed_time(market["start_time"]) <= now)
            and (not parsed_time(market["end_time"]) or now < parsed_time(market["end_time"]))]


def next_market_boundary(markets: list[dict], now: datetime) -> datetime | None:
    boundaries = []
    for market in markets:
        start = parsed_time(market.get("start_time"))
        end = parsed_time(market.get("end_time"))
        if start and now < start: boundaries.append(start)
        if end and now < end: boundaries.append(end)
    return min(boundaries, default=None)


async def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.info("Collector run start UTC=%s", utcnow().isoformat())
    con = connect_db(); stop = asyncio.Event()
    repaired = repair_interval_starts(con)
    if repaired:
        logging.warning("Repaired incorrect interval start timestamps for %d existing markets", repaired)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, transition_stop, stop, f"signal {sig.name}")
        except NotImplementedError: pass
    bitcoin_task = asyncio.create_task(collect_binance(con, stop))
    poly_tasks: dict[str, asyncio.Task] = {}
    known_markets: list[dict] = []
    next_discovery: datetime | None = None
    try:
        while not stop.is_set():
            supervise_binance_task(bitcoin_task)
            changed = False
            try:
                now = utcnow()
                changed = await reconcile_polymarket_tasks(
                    con, stop, poly_tasks, eligible_markets(known_markets, now))

                if next_discovery is None or now >= next_discovery:
                    next_discovery = now + timedelta(seconds=30)
                    boundary = next_market_boundary(known_markets, now)
                    boundary_seconds = ((boundary-now).total_seconds() if boundary else None)
                    try:
                        if boundary_seconds is not None:
                            found = await asyncio.wait_for(discover_markets(), timeout=max(.01, boundary_seconds))
                        else:
                            found = await discover_markets()
                    except (TimeoutError, asyncio.TimeoutError):
                        continue
                    for market in found: upsert_market(con, market)
                    known_markets = found
                    now = utcnow()
                    next_discovery = now + timedelta(seconds=30)
                    changed = await reconcile_polymarket_tasks(
                        con, stop, poly_tasks, eligible_markets(known_markets, now)) or changed

                if changed:
                    publish_dashboard_snapshot(con)
            except Exception as exc:
                if stop.is_set() or is_query_interrupted(exc):
                    break
                logging.exception("Market discovery failed")
            now = utcnow()
            wakeups = [value for value in (next_discovery, next_market_boundary(known_markets, now))
                       if value is not None]
            refresh_seconds = max(.01, (min(wakeups)-now).total_seconds()) if wakeups else 30.0
            expected_wake = loop.time() + refresh_seconds
            try:
                await asyncio.wait_for(stop.wait(), timeout=refresh_seconds)
            except (TimeoutError, asyncio.TimeoutError):
                pass
            finally:
                if not stop.is_set():
                    warn_event_loop_lag(expected_wake, loop.time())
    finally:
        logging.info("Collector shutdown beginning UTC=%s", utcnow().isoformat())
        transition_stop(stop, "shutdown")
        try:
            if not bitcoin_task.done():
                bitcoin_task.cancel()
            await cancel_polymarket_tasks(poly_tasks)
            await asyncio.gather(bitcoin_task, return_exceptions=True)
            log_task_outcome(bitcoin_task, "Binance collector")
        finally:
            try:
                con.close()
            finally:
                logging.info("Collector shutdown complete UTC=%s", utcnow().isoformat())
                logging.info("Collector run exit UTC=%s", utcnow().isoformat())


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logging.info("Collector stopped by user")
