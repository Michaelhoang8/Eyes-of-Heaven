import asyncio
import json

from collector.markets import connect_db
from collector import polymarket


MARKET = {"market_id": "market", "up_token_id": "up", "down_token_id": "down"}


def test_application_heartbeat_sends_ping_on_cadence(monkeypatch):
    sent = []
    sleeps = []

    class Finished(Exception): pass

    class WebSocket:
        async def send(self, value):
            sent.append(value)
            if len(sent) == 3: raise Finished

    async def fake_sleep(interval):
        sleeps.append(interval)

    async def exercise():
        try: await polymarket.application_heartbeat(WebSocket())
        except Finished: pass

    monkeypatch.setattr(polymarket.asyncio, "sleep", fake_sleep)
    asyncio.run(exercise())
    assert sent == ["PING", "PING", "PING"]
    assert sleeps == [10, 10, 10]


def test_pong_is_control_traffic_and_market_json_still_inserts(tmp_path):
    stop = asyncio.Event()
    connection_options = {}
    payload = json.dumps({"event_type": "best_bid_ask", "asset_id": "up",
                          "best_bid": "0.4", "best_ask": "0.6",
                          "timestamp": "1785822901000"})

    class WebSocket:
        def __init__(self):
            self.messages = iter(["PONG", payload])
            self.sent = []
        async def send(self, value): self.sent.append(value)
        def __aiter__(self): return self
        async def __anext__(self):
            try: return next(self.messages)
            except StopIteration:
                stop.set()
                raise StopAsyncIteration

    websocket = WebSocket()

    class Connection:
        async def __aenter__(self): return websocket
        async def __aexit__(self, *_args): return False

    def connector(_url, **kwargs):
        connection_options.update(kwargs)
        return Connection()

    con = connect_db(tmp_path / "test.duckdb")
    asyncio.run(polymarket.collect_polymarket(con, MARKET, stop, connector=connector))

    assert connection_options["ping_interval"] is None
    assert con.execute("SELECT count(*) FROM polymarket_quotes").fetchone()[0] == 1
    assert con.execute("SELECT malformed_messages FROM collector_status WHERE source='polymarket'").fetchone()[0] == 0


def test_reconnect_and_shutdown_do_not_leak_heartbeat_tasks(monkeypatch, tmp_path):
    entered_second_connection = asyncio.Event()
    created = finalized = active = 0
    connections = 0

    async def tracked_heartbeat(_websocket, interval=10):
        nonlocal created, finalized, active
        created += 1
        active += 1
        try:
            await asyncio.Event().wait()
        finally:
            active -= 1
            finalized += 1

    class WebSocket:
        def __init__(self, number): self.number = number
        async def send(self, _value): pass
        def __aiter__(self): return self
        async def __anext__(self):
            await asyncio.sleep(0)
            if self.number == 1:
                raise ConnectionError("disconnect")
            entered_second_connection.set()
            await asyncio.Event().wait()

    class Connection:
        def __init__(self, number): self.websocket = WebSocket(number)
        async def __aenter__(self): return self.websocket
        async def __aexit__(self, *_args): return False

    def connector(_url, **_kwargs):
        nonlocal connections
        connections += 1
        return Connection(connections)

    monkeypatch.setattr(polymarket, "application_heartbeat", tracked_heartbeat)
    con = connect_db(tmp_path / "test.duckdb")

    async def exercise():
        task = asyncio.create_task(polymarket.collect_polymarket(
            con, MARKET, asyncio.Event(), connector=connector, base_delay=.001))
        await asyncio.wait_for(entered_second_connection.wait(), timeout=1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(exercise())
    assert connections == 2
    assert created == finalized == 2
    assert active == 0
