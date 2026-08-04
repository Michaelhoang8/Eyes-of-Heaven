from __future__ import annotations

import asyncio
import logging
import signal
from datetime import datetime

from .bitcoin import collect_binance
from .markets import connect_db, discover_markets, is_query_interrupted, publish_dashboard_snapshot, repair_interval_starts, upsert_market, utcnow
from .polymarket import collect_polymarket


async def reconcile_polymarket_tasks(con, stop: asyncio.Event, tasks: dict[str, asyncio.Task],
                                     eligible: list[dict]) -> bool:
    """Make the running Polymarket tasks match the currently eligible markets."""
    markets = {market["market_id"]: market for market in eligible}
    stale_ids = [market_id for market_id, task in tasks.items()
                 if market_id not in markets or task.done()]
    stale_tasks = [tasks.pop(market_id) for market_id in stale_ids]
    for task in stale_tasks:
        task.cancel()
    if stale_tasks:
        await asyncio.gather(*stale_tasks, return_exceptions=True)

    started = False
    for market_id, market in markets.items():
        if market_id not in tasks:
            logging.info("Recording Polymarket market: %s", market["question"])
            tasks[market_id] = asyncio.create_task(collect_polymarket(con, market, stop))
            started = True
    return bool(stale_tasks) or started


async def cancel_polymarket_tasks(tasks: dict[str, asyncio.Task]) -> None:
    active = list(tasks.values())
    for task in active:
        task.cancel()
    if active:
        await asyncio.gather(*active, return_exceptions=True)
    tasks.clear()


async def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    con = connect_db(); stop = asyncio.Event()
    repaired = repair_interval_starts(con)
    if repaired:
        logging.warning("Repaired incorrect interval start timestamps for %d existing markets", repaired)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, stop.set)
        except NotImplementedError: pass
    bitcoin_task = asyncio.create_task(collect_binance(con, stop))
    poly_tasks: dict[str, asyncio.Task] = {}
    try:
        while not stop.is_set():
            eligible = []
            try:
                found = await discover_markets()
                for market in found: upsert_market(con, market)
                now = utcnow()
                def parsed(value):
                    if not value: return None
                    if isinstance(value, datetime): return value
                    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                eligible = [m for m in found if (not parsed(m["start_time"]) or parsed(m["start_time"]) <= now)
                            and (not parsed(m["end_time"]) or now < parsed(m["end_time"]))]
                if await reconcile_polymarket_tasks(con, stop, poly_tasks, eligible):
                    publish_dashboard_snapshot(con)
            except Exception as exc:
                if stop.is_set() or is_query_interrupted(exc):
                    break
                logging.exception("Market discovery failed")
            refresh_seconds = 30.0
            ends = [parsed(m.get("end_time")) for m in eligible if parsed(m.get("end_time"))]
            if ends:
                refresh_seconds = max(.1, min(30.0, (min(ends)-utcnow()).total_seconds()+.05))
            try: await asyncio.wait_for(stop.wait(), timeout=refresh_seconds)
            except (TimeoutError, asyncio.TimeoutError): pass
    finally:
        stop.set()
        bitcoin_task.cancel()
        await cancel_polymarket_tasks(poly_tasks)
        await asyncio.gather(bitcoin_task, return_exceptions=True)
        con.close()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logging.info("Collector stopped by user")
