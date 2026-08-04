from datetime import datetime, timezone

from collector.bitcoin import insert_event, ms_time, parse_binance
from collector.markets import connect_db, is_query_interrupted, map_outcome_tokens, normalize_market, repair_interval_starts, upsert_market
from collector.polymarket import parse_polymarket
from collector.bitcoin import collect_binance


def raw_market(**overrides):
    value = {"id":"1","conditionId":"condition-1","question":"Bitcoin Up or Down?","slug":"bitcoin-up-down",
      "outcomes":"[\"Down\", \"Up\"]","clobTokenIds":"[\"down-id\", \"up-id\"]","outcomePrices":"[\"0\",\"1\"]",
      "active":False,"closed":True,"startDate":"2026-01-01T00:00:00Z","endDate":"2026-01-01T00:05:00Z"}
    value.update(overrides); return value


def test_database_creation_and_market_insertion(tmp_path):
    con=connect_db(tmp_path/"test.duckdb"); upsert_market(con,normalize_market(raw_market()))
    assert con.execute("SELECT up_token_id,down_token_id,resolved_outcome FROM markets").fetchone()==("up-id","down-id","Up")


def test_market_interval_uses_timezone_aware_event_start():
    market=normalize_market(raw_market(slug="btc-updown-5m-1767225600",eventStartTime="2026-01-01T00:00:00Z"))
    assert market["start_time"].tzinfo == timezone.utc
    assert market["end_time"].tzinfo == timezone.utc
    assert (market["end_time"]-market["start_time"]).total_seconds()==300


def test_explicit_up_down_mapping_not_token_order():
    assert map_outcome_tokens(raw_market()) == ("up-id","down-id")


def test_mapping_rejects_ambiguous_labels():
    import pytest
    with pytest.raises(ValueError): map_outcome_tokens(raw_market(outcomes='["Yes","No"]'))


def test_mapping_rejects_identical_token_ids():
    import pytest
    with pytest.raises(ValueError): map_outcome_tokens(raw_market(clobTokenIds='["same","same"]'))


def test_binance_parse_and_duplicate_handling(tmp_path):
    con=connect_db(tmp_path/"test.duckdb")
    event=parse_binance('{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","a":7,"T":1767225600000,"p":"10","q":"2"}}')
    assert insert_event(con,event) is True
    assert insert_event(con,event) is False
    assert con.execute("SELECT count(*) FROM bitcoin_events").fetchone()[0]==1


def test_polymarket_midpoint_inputs():
    market={"market_id":"m","up_token_id":"up","down_token_id":"down"}
    payload='{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.4","best_ask":"0.6","timestamp":"1767225600000"}'
    quotes,trades=parse_polymarket(payload,market,datetime.now(timezone.utc))
    assert not trades and (quotes[0]["best_bid"]+quotes[0]["best_ask"])/2 == .5


def test_seconds_timestamp_is_detected():
    import pytest
    with pytest.raises(ValueError,match="milliseconds"): ms_time(1767225600)


def test_duckdb_ctrl_c_interruption_is_recognized():
    assert is_query_interrupted(RuntimeError("Query interrupted"))
    assert not is_query_interrupted(RuntimeError("different failure"))


def test_resolution_and_token_mapping_match_official_metadata():
    market=normalize_market(raw_market(outcomes='["Up","Down"]',clobTokenIds='["up","down"]',outcomePrices='["1","0"]'))
    assert market["resolved_outcome"]=="Up"
    assert (market["up_token_id"],market["down_token_id"])==("up","down")


def test_legacy_interval_start_repair(tmp_path):
    con=connect_db(tmp_path/"test.duckdb")
    market=normalize_market(raw_market(slug="btc-updown-5m-1767225600"))
    market["start_time"]=datetime(2025,12,31,tzinfo=timezone.utc)
    upsert_market(con,market)
    assert repair_interval_starts(con)==1
    assert con.execute("SELECT start_time FROM markets").fetchone()[0].astimezone(timezone.utc)==datetime(2026,1,1,tzinfo=timezone.utc)


def test_websocket_reconnect_is_counted(tmp_path):
    import asyncio
    con=connect_db(tmp_path/"test.duckdb"); stop=asyncio.Event()
    class BrokenConnection:
        async def __aenter__(self): raise ConnectionError("test disconnect")
        async def __aexit__(self,*args): return False
    def connector(*args,**kwargs): return BrokenConnection()
    async def exercise():
        async def halt(): await asyncio.sleep(.01); stop.set()
        await asyncio.gather(collect_binance(con,stop,connector=connector,base_delay=.001),halt())
    asyncio.run(exercise())
    assert con.execute("SELECT reconnect_count FROM collector_status WHERE source='binance'").fetchone()[0] >= 1


def test_overlapping_5m_and_15m_markets_are_both_collected(monkeypatch):
    import asyncio
    from collector import main

    now = datetime(2026, 8, 4, 3, 15, tzinfo=timezone.utc)
    markets = [
        {"market_id": "five", "question": "5m", "start_time": now,
         "end_time": datetime(2026, 8, 4, 3, 20, tzinfo=timezone.utc)},
        {"market_id": "fifteen", "question": "15m", "start_time": now,
         "end_time": datetime(2026, 8, 4, 3, 30, tzinfo=timezone.utc)},
    ]
    started = []

    class Connection:
        def close(self): pass

    async def discover(): return markets
    async def idle(): await asyncio.Event().wait()
    def collect(_con, market, _stop):
        started.append(market["market_id"])
        return idle()
    async def stop_after_first_cycle(awaitable, timeout):
        awaitable.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(main, "connect_db", Connection)
    monkeypatch.setattr(main, "repair_interval_starts", lambda _con: 0)
    monkeypatch.setattr(main, "collect_binance", lambda *_args: idle())
    monkeypatch.setattr(main, "collect_polymarket", collect)
    monkeypatch.setattr(main, "discover_markets", discover)
    monkeypatch.setattr(main, "upsert_market", lambda *_args: None)
    monkeypatch.setattr(main, "publish_dashboard_snapshot", lambda _con: None)
    monkeypatch.setattr(main, "utcnow", lambda: now)
    monkeypatch.setattr(main.asyncio, "wait_for", stop_after_first_cycle)

    try:
        asyncio.run(main.run())
    except KeyboardInterrupt:
        pass

    assert set(started) == {"five", "fifteen"}


def test_sequential_discovery_does_not_duplicate_market_tasks(monkeypatch):
    import asyncio
    from collector import main

    started = []
    markets = [{"market_id": market_id, "question": market_id}
               for market_id in ("five", "fifteen")]

    async def idle(): await asyncio.Event().wait()
    def collect(_con, market, _stop):
        started.append(market["market_id"])
        return idle()

    monkeypatch.setattr(main, "collect_polymarket", collect)

    async def exercise():
        tasks = {}
        stop = asyncio.Event()
        await main.reconcile_polymarket_tasks(None, stop, tasks, markets)
        original = dict(tasks)
        await main.reconcile_polymarket_tasks(None, stop, tasks, markets)
        assert tasks == original
        assert started == ["five", "fifteen"]
        await main.cancel_polymarket_tasks(tasks)

    asyncio.run(exercise())


def test_expired_market_task_is_cancelled_and_removed(monkeypatch):
    import asyncio
    from collector import main

    markets = [{"market_id": market_id, "question": market_id}
               for market_id in ("five", "fifteen")]

    async def idle(): await asyncio.Event().wait()
    monkeypatch.setattr(main, "collect_polymarket", lambda *_args: idle())

    async def exercise():
        tasks = {}
        stop = asyncio.Event()
        await main.reconcile_polymarket_tasks(None, stop, tasks, markets)
        expired_task = tasks["five"]
        await main.reconcile_polymarket_tasks(None, stop, tasks, [markets[1]])
        assert set(tasks) == {"fifteen"}
        assert expired_task.cancelled()
        await main.cancel_polymarket_tasks(tasks)

    asyncio.run(exercise())


def test_shutdown_cancels_and_awaits_multiple_polymarket_tasks(monkeypatch):
    import asyncio
    from collector import main

    cancelled = set()
    markets = [{"market_id": market_id, "question": market_id}
               for market_id in ("five", "fifteen")]

    async def collect(_con, market, _stop):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.add(market["market_id"])

    monkeypatch.setattr(main, "collect_polymarket", collect)

    async def exercise():
        tasks = {}
        await main.reconcile_polymarket_tasks(None, asyncio.Event(), tasks, markets)
        await asyncio.sleep(0)
        await main.cancel_polymarket_tasks(tasks)
        assert tasks == {}

    asyncio.run(exercise())
    assert cancelled == {"five", "fifteen"}
