from __future__ import annotations

import asyncio
import logging
import signal
from datetime import datetime

from .bitcoin import collect_binance
from .markets import connect_db, discover_markets, publish_dashboard_snapshot, repair_interval_starts, upsert_market, utcnow
from .polymarket import collect_polymarket


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
    poly_task = None; current_id = None
    try:
        while not stop.is_set():
            chosen = None
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
                chosen = min(eligible, key=lambda m: parsed(m["end_time"]) or datetime.max.replace(tzinfo=now.tzinfo), default=None)
                if chosen and chosen["market_id"] != current_id:
                    if poly_task:
                        poly_task.cancel(); await asyncio.gather(poly_task, return_exceptions=True)
                    logging.info("Recording Polymarket market: %s", chosen["question"])
                    current_id = chosen["market_id"]
                    poly_task = asyncio.create_task(collect_polymarket(con, chosen, stop))
                    publish_dashboard_snapshot(con)
            except Exception:
                logging.exception("Market discovery failed")
            refresh_seconds = 30.0
            if chosen and parsed(chosen.get("end_time")):
                refresh_seconds = max(.1, min(30.0, (parsed(chosen["end_time"])-utcnow()).total_seconds()+.05))
            try: await asyncio.wait_for(stop.wait(), timeout=refresh_seconds)
            except (TimeoutError, asyncio.TimeoutError): pass
    finally:
        stop.set()
        tasks = [bitcoin_task] + ([poly_task] if poly_task else [])
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        con.close()


if __name__ == "__main__":
    asyncio.run(run())
