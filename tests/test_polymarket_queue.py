import asyncio
import json
import logging
import threading
from datetime import datetime, timedelta, timezone

from collector import polymarket
from collector.markets import connect_db


MARKET = {"market_id": "market", "up_token_id": "up", "down_token_id": "down"}
BASE = datetime(2026, 8, 5, 13, 0, tzinfo=timezone.utc)


def payload(index):
    return json.dumps({"event_type": "best_bid_ask", "asset_id": "up",
                       "best_bid": str(.4 + index / 100), "best_ask": ".6",
                       "timestamp": str(1785934800000 + index)})


class Messages:
    def __init__(self, values): self.values = iter(values)
    def __aiter__(self): return self
    async def __anext__(self):
        try: return next(self.values)
        except StopIteration: raise StopAsyncIteration


def test_slow_writer_does_not_prevent_reader_accepting_messages(monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    persisted = []

    def slow_persist(_con, _market, messages):
        entered.set()
        release.wait(timeout=2)
        persisted.extend(raw for raw, _receipt in messages)

    monkeypatch.setattr(polymarket, "persist_messages", slow_persist)

    async def exercise():
        queue = asyncio.Queue(maxsize=10)
        writer = asyncio.create_task(polymarket.write_messages(None, MARKET, queue))
        await queue.put((payload(0), BASE))
        await asyncio.to_thread(entered.wait, 1)
        reader = asyncio.create_task(polymarket.read_messages(
            Messages([payload(1), payload(2), payload(3)]), queue, "market", asyncio.Event()))
        await reader
        assert queue.qsize() == 3
        release.set()
        await queue.join()
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)
        assert persisted == [payload(i) for i in range(4)]

    asyncio.run(exercise())


def test_receipt_timestamp_is_captured_by_reader_and_order_is_preserved(tmp_path):
    receipts = iter(BASE + timedelta(milliseconds=i) for i in range(3))

    async def exercise():
        queue = asyncio.Queue(maxsize=10)
        await polymarket.read_messages(
            Messages([payload(0), "PONG", payload(1)]), queue, "market", asyncio.Event(),
            receipt_clock=lambda: next(receipts))
        items = [queue.get_nowait(), queue.get_nowait()]
        queue.task_done(); queue.task_done()
        assert [item[0] for item in items] == [payload(0), payload(1)]
        assert [item[1] for item in items] == [BASE, BASE + timedelta(milliseconds=2)]

        con = connect_db(tmp_path / "ordered.duckdb")
        writer = asyncio.create_task(polymarket.write_messages(con.cursor(), MARKET, queue))
        for item in items: await queue.put(item)
        await queue.join()
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)
        rows = con.execute("SELECT event_id,receipt_timestamp FROM polymarket_quotes ORDER BY receipt_timestamp").fetchall()
        expected = [polymarket.parse_polymarket(payload(i), MARKET, items[i][1])[0][0]
                    for i in range(2)]
        assert rows == [(row["event_id"], row["receipt_timestamp"]) for row in expected]

    asyncio.run(exercise())


def test_queue_saturation_is_logged_and_does_not_drop(caplog):
    async def exercise():
        queue = asyncio.Queue(maxsize=1)
        stats = {"maximum_depth": 0}
        await queue.put(("existing", BASE))
        reader = asyncio.create_task(polymarket.read_messages(
            Messages(["next"]), queue, "market", asyncio.Event(), receipt_clock=lambda: BASE,
            queue_warning_depth=1, queue_stats=stats))
        await asyncio.sleep(0)
        assert not reader.done()
        assert queue.get_nowait()[0] == "existing"
        queue.task_done()
        await reader
        assert queue.get_nowait()[0] == "next"
        queue.task_done()
        assert stats["maximum_depth"] == 1

    with caplog.at_level(logging.WARNING):
        asyncio.run(exercise())
    assert "queue saturated" in caplog.text
    assert "no observations dropped" in caplog.text


def test_shutdown_drains_queued_messages(monkeypatch, caplog, tmp_path):
    write_started = threading.Event()
    release = threading.Event()

    persisted = []

    def blocked_persist(_con, _market, messages):
        write_started.set()
        release.wait(timeout=2)
        persisted.extend(raw for raw, _receipt in messages)

    monkeypatch.setattr(polymarket, "persist_messages", blocked_persist)

    class WebSocket(Messages):
        def __init__(self): super().__init__([payload(0), payload(1), payload(2)])
        async def send(self, _value): pass
        async def __anext__(self):
            try: return next(self.values)
            except StopIteration: await asyncio.Event().wait()

    class Connection:
        async def __aenter__(self): return WebSocket()
        async def __aexit__(self, *_args): return False

    async def exercise():
        task = asyncio.create_task(polymarket.collect_polymarket(
            connect_db(tmp_path / "shutdown.duckdb"), MARKET, asyncio.Event(),
            connector=lambda *_args, **_kwargs: Connection(), base_delay=.001))
        await asyncio.to_thread(write_started.wait, 1)
        task.cancel()
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled()

    with caplog.at_level(logging.ERROR):
        asyncio.run(exercise())
    assert persisted == [payload(0), payload(1), payload(2)]
    assert "abandoned queued observations" not in caplog.text
