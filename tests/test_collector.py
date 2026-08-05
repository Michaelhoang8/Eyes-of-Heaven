from datetime import datetime, timedelta, timezone

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
    wakeups = 0
    async def stop_after_first_cycle(_stop, _bitcoin, _discovery, _timeout):
        nonlocal wakeups
        wakeups += 1
        await asyncio.sleep(0)
        if wakeups == 2: raise KeyboardInterrupt

    monkeypatch.setattr(main, "connect_db", Connection)
    monkeypatch.setattr(main, "repair_interval_starts", lambda _con: 0)
    monkeypatch.setattr(main, "collect_binance", lambda *_args: idle())
    monkeypatch.setattr(main, "collect_polymarket", collect)
    monkeypatch.setattr(main, "discover_markets", discover)
    monkeypatch.setattr(main, "upsert_market", lambda *_args: None)
    monkeypatch.setattr(main, "publish_dashboard_snapshot", lambda _con: None)
    monkeypatch.setattr(main, "utcnow", lambda: now)
    monkeypatch.setattr(main, "wait_for_lifecycle_wakeup", stop_after_first_cycle)

    try:
        asyncio.run(main.run())
    except KeyboardInterrupt:
        pass

    assert set(started) == {"five", "fifteen"}


def test_future_market_wakes_at_start_boundary_not_next_poll(monkeypatch):
    import asyncio
    from collector import main

    now = datetime(2026, 8, 4, 4, 59, 50, tzinfo=timezone.utc)
    market = {"market_id": "future", "question": "future", "start_time":
              datetime(2026, 8, 4, 5, 0, tzinfo=timezone.utc), "end_time":
              datetime(2026, 8, 4, 5, 5, tzinfo=timezone.utc)}
    waits = []

    class Connection:
        def close(self): pass

    async def idle(): await asyncio.Event().wait()
    async def discover(): return [market]
    wakeups = 0
    async def capture_wait(_stop, _bitcoin, _discovery, timeout):
        nonlocal wakeups
        wakeups += 1
        await asyncio.sleep(0)
        if wakeups == 2:
            waits.append(timeout)
            raise KeyboardInterrupt

    monkeypatch.setattr(main, "connect_db", Connection)
    monkeypatch.setattr(main, "repair_interval_starts", lambda _con: 0)
    monkeypatch.setattr(main, "collect_binance", lambda *_args: idle())
    monkeypatch.setattr(main, "discover_markets", discover)
    monkeypatch.setattr(main, "upsert_market", lambda *_args: None)
    monkeypatch.setattr(main, "utcnow", lambda: now)
    monkeypatch.setattr(main, "wait_for_lifecycle_wakeup", capture_wait)

    try:
        asyncio.run(main.run())
    except KeyboardInterrupt:
        pass

    assert waits[0] <= 10.1


def test_expired_collector_is_cancelled_before_next_discovery(monkeypatch):
    import asyncio
    from collector import main

    clock = [datetime(2026, 8, 4, 4, 59, 50, tzinfo=timezone.utc)]
    end = datetime(2026, 8, 4, 5, 0, tzinfo=timezone.utc)
    market = {"market_id": "ending", "question": "ending", "start_time": clock[0],
              "end_time": end}
    cancelled = set()
    observed_at_second_wakeup = []
    discoveries = 0
    waits = 0

    class Connection:
        def close(self): pass

    async def idle(): await asyncio.Event().wait()
    async def collect(_con, selected, _stop):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.add(selected["market_id"])
    async def discover():
        nonlocal discoveries
        discoveries += 1
        return [market]
    async def advance_to_end(_stop, _bitcoin, _discovery, _timeout):
        nonlocal waits
        waits += 1
        await asyncio.sleep(0)
        if waits == 3:
            observed_at_second_wakeup.append(("ending" in cancelled, discoveries))
            raise KeyboardInterrupt
        if waits == 2:
            clock[0] = end

    monkeypatch.setattr(main, "connect_db", Connection)
    monkeypatch.setattr(main, "repair_interval_starts", lambda _con: 0)
    monkeypatch.setattr(main, "collect_binance", lambda *_args: idle())
    monkeypatch.setattr(main, "collect_polymarket", collect)
    monkeypatch.setattr(main, "discover_markets", discover)
    monkeypatch.setattr(main, "upsert_market", lambda *_args: None)
    monkeypatch.setattr(main, "publish_dashboard_snapshot", lambda _con: None)
    monkeypatch.setattr(main, "utcnow", lambda: clock[0])
    monkeypatch.setattr(main, "wait_for_lifecycle_wakeup", advance_to_end)

    try:
        asyncio.run(main.run())
    except KeyboardInterrupt:
        pass

    assert observed_at_second_wakeup == [(True, 1)]


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


def test_market_discovered_minutes_early_starts_once_at_boundary(monkeypatch):
    import asyncio
    from collector import main

    clock = [datetime(2026, 8, 4, 4, 57, tzinfo=timezone.utc)]
    start = datetime(2026, 8, 4, 5, 0, tzinfo=timezone.utc)
    market = {"market_id": "future", "question": "future", "start_time": start,
              "end_time": datetime(2026, 8, 4, 5, 5, tzinfo=timezone.utc)}
    started = []

    async def idle(): await asyncio.Event().wait()
    def collect(_con, selected, _stop):
        started.append((selected["market_id"], clock[0]))
        return idle()

    monkeypatch.setattr(main, "collect_polymarket", collect)

    async def exercise():
        tasks = {}
        stop = asyncio.Event()
        for minute in (57, 58, 59):
            clock[0] = datetime(2026, 8, 4, 4, minute, tzinfo=timezone.utc)
            await main.reconcile_polymarket_tasks(
                None, stop, tasks, main.eligible_markets([market], clock[0]))
            assert tasks == {}
        clock[0] = start
        eligible = main.eligible_markets([market], clock[0])
        await main.reconcile_polymarket_tasks(None, stop, tasks, eligible)
        original = tasks["future"]
        await main.reconcile_polymarket_tasks(None, stop, tasks, eligible)
        assert tasks["future"] is original
        await main.cancel_polymarket_tasks(tasks)

    asyncio.run(exercise())
    assert started == [("future", start)]


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


def test_unexpected_binance_exception_is_logged_and_fatal(caplog):
    import asyncio
    import logging
    import pytest
    from collector import main

    async def exercise():
        async def fail(): raise ValueError("binance died")
        task = asyncio.create_task(fail())
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="Binance collector task stopped"):
            main.supervise_binance_task(task)

    with caplog.at_level(logging.ERROR):
        asyncio.run(exercise())
    assert "Binance collector task raised" in caplog.text
    assert "binance died" in caplog.text


def test_polymarket_exception_is_logged_before_restart(monkeypatch, caplog):
    import asyncio
    import logging
    from collector import main

    market = {"market_id": "broken-market", "question": "broken"}
    async def fail(): raise ValueError("polymarket died")
    async def idle(): await asyncio.Event().wait()
    monkeypatch.setattr(main, "collect_polymarket", lambda *_args: idle())

    async def exercise():
        failed = asyncio.create_task(fail())
        await asyncio.sleep(0)
        tasks = {"broken-market": failed}
        await main.reconcile_polymarket_tasks(None, asyncio.Event(), tasks, [market])
        assert tasks["broken-market"] is not failed
        await main.cancel_polymarket_tasks(tasks)

    with caplog.at_level(logging.INFO):
        asyncio.run(exercise())
    assert "Polymarket task raised market_id=broken-market" in caplog.text
    assert "polymarket died" in caplog.text


def test_cancelled_polymarket_task_is_not_reported_as_failure(monkeypatch, caplog):
    import asyncio
    import logging
    from collector import main

    market = {"market_id": "cancelled-market", "question": "cancelled"}
    async def idle(): await asyncio.Event().wait()
    monkeypatch.setattr(main, "collect_polymarket", lambda *_args: idle())

    async def exercise():
        cancelled = asyncio.create_task(idle())
        await asyncio.sleep(0)
        cancelled.cancel()
        await asyncio.gather(cancelled, return_exceptions=True)
        tasks = {"cancelled-market": cancelled}
        await main.reconcile_polymarket_tasks(None, asyncio.Event(), tasks, [market])
        await main.cancel_polymarket_tasks(tasks)

    with caplog.at_level(logging.INFO):
        asyncio.run(exercise())
    assert "Polymarket task cancelled market_id=cancelled-market" in caplog.text
    assert not [record for record in caplog.records
                if record.levelno >= logging.ERROR and "cancelled-market" in record.message]


def test_graceful_shutdown_logs_full_lifecycle(monkeypatch, caplog):
    import asyncio
    import logging
    from collector import main

    class Connection:
        def close(self): pass

    async def idle(): await asyncio.Event().wait()
    async def discover(): return []
    async def stop_run(_stop, _bitcoin, _discovery, _timeout):
        await asyncio.sleep(0)
        raise KeyboardInterrupt

    monkeypatch.setattr(main, "connect_db", Connection)
    monkeypatch.setattr(main, "repair_interval_starts", lambda _con: 0)
    monkeypatch.setattr(main, "collect_binance", lambda *_args: idle())
    monkeypatch.setattr(main, "discover_markets", discover)
    monkeypatch.setattr(main, "wait_for_lifecycle_wakeup", stop_run)

    with caplog.at_level(logging.INFO):
        try: asyncio.run(main.run())
        except KeyboardInterrupt: pass

    for message in ("Collector run start UTC", "Collector stop event set",
                    "Collector shutdown beginning", "Collector shutdown complete",
                    "Collector run exit UTC"):
        assert message in caplog.text


def test_event_loop_lag_warns_only_above_threshold(caplog):
    import logging
    from collector import main

    with caplog.at_level(logging.WARNING):
        main.warn_event_loop_lag(expected=10, actual=10.9, threshold=1)
        assert "Event loop wake lag" not in caplog.text
        main.warn_event_loop_lag(expected=10, actual=11.1, threshold=1)
    assert "Event loop wake lag 1.100s" in caplog.text


def test_blocked_discovery_does_not_delay_known_start_boundary(monkeypatch):
    import asyncio
    from collector import main

    clock = [datetime(2026, 8, 4, 1, 0, tzinfo=timezone.utc)]
    start = clock[0] + timedelta(seconds=40)
    market = {"market_id": "future", "question": "future", "start_time": start,
              "end_time": start + timedelta(minutes=5)}
    discovery_started = asyncio.Event()
    release_discovery = asyncio.Event()
    started = []
    discoveries = 0

    class Connection:
        def close(self): pass

    async def idle(): await asyncio.Event().wait()
    def collect(_con, selected, _stop):
        started.append((selected["market_id"], clock[0]))
        return idle()
    async def discover():
        nonlocal discoveries
        discoveries += 1
        if discoveries == 1: return [market]
        discovery_started.set()
        await release_discovery.wait()
        return [market]
    async def controlled_wait(_stop, _bitcoin, discovery_task, timeout):
        await asyncio.sleep(0)
        if discovery_task is not None and discovery_started.is_set():
            clock[0] = start
        elif timeout is not None and timeout >= 29:
            clock[0] += timedelta(seconds=30)

    monkeypatch.setattr(main, "connect_db", Connection)
    monkeypatch.setattr(main, "repair_interval_starts", lambda _con: 0)
    monkeypatch.setattr(main, "collect_binance", lambda *_args: idle())
    monkeypatch.setattr(main, "collect_polymarket", collect)
    monkeypatch.setattr(main, "discover_markets", discover)
    monkeypatch.setattr(main, "upsert_market", lambda *_args: None)
    monkeypatch.setattr(main, "publish_dashboard_snapshot", lambda _con: None)
    monkeypatch.setattr(main, "utcnow", lambda: clock[0])
    monkeypatch.setattr(main, "wait_for_lifecycle_wakeup", controlled_wait)

    async def exercise():
        task = asyncio.create_task(main.run())
        await discovery_started.wait()
        clock[0] = start
        for _ in range(3): await asyncio.sleep(0)
        try:
            assert started == [("future", start)]
        finally:
            release_discovery.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(exercise())


def test_blocked_discovery_does_not_delay_known_end_boundary(monkeypatch):
    import asyncio
    from collector import main

    clock = [datetime(2026, 8, 4, 1, 0, tzinfo=timezone.utc)]
    end = clock[0] + timedelta(seconds=40)
    market = {"market_id": "active", "question": "active", "start_time": clock[0],
              "end_time": end}
    discovery_started = asyncio.Event()
    release_discovery = asyncio.Event()
    cancelled = set()
    discoveries = 0

    class Connection:
        def close(self): pass

    async def idle(): await asyncio.Event().wait()
    async def collect(_con, selected, _stop):
        try: await asyncio.Event().wait()
        finally: cancelled.add(selected["market_id"])
    async def discover():
        nonlocal discoveries
        discoveries += 1
        if discoveries == 1: return [market]
        discovery_started.set()
        await release_discovery.wait()
        return [market]
    async def controlled_wakeup(_stop, _bitcoin, discovery_task, timeout):
        await asyncio.sleep(0)
        if discovery_task is not None and discovery_started.is_set():
            clock[0] = end
        elif timeout is not None and timeout >= 29:
            clock[0] += timedelta(seconds=30)

    monkeypatch.setattr(main, "connect_db", Connection)
    monkeypatch.setattr(main, "repair_interval_starts", lambda _con: 0)
    monkeypatch.setattr(main, "collect_binance", lambda *_args: idle())
    monkeypatch.setattr(main, "collect_polymarket", collect)
    monkeypatch.setattr(main, "discover_markets", discover)
    monkeypatch.setattr(main, "upsert_market", lambda *_args: None)
    monkeypatch.setattr(main, "publish_dashboard_snapshot", lambda _con: None)
    monkeypatch.setattr(main, "utcnow", lambda: clock[0])
    monkeypatch.setattr(main, "wait_for_lifecycle_wakeup", controlled_wakeup)

    async def exercise():
        task = asyncio.create_task(main.run())
        await discovery_started.wait()
        for _ in range(5): await asyncio.sleep(0)
        try:
            assert "active" in cancelled
        finally:
            release_discovery.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(exercise())


def test_failed_discovery_preserves_known_markets(caplog):
    import asyncio
    import logging
    from collector import main

    known = [{"market_id": "known"}]

    async def exercise():
        async def fail(): raise ConnectionError("gamma failed")
        task = asyncio.create_task(fail())
        await asyncio.sleep(0)
        retained, updated = main.apply_discovery_result(None, task, known)
        assert retained is known
        assert updated is False

    with caplog.at_level(logging.ERROR):
        asyncio.run(exercise())
    assert "Market discovery failed" in caplog.text
    assert "gamma failed" in caplog.text


def test_successful_discovery_replaces_known_markets(monkeypatch):
    import asyncio
    from collector import main

    known = [{"market_id": "old"}]
    found = [{"market_id": "new"}]
    inserted = []
    monkeypatch.setattr(main, "upsert_market", lambda _con, market: inserted.append(market["market_id"]))

    async def exercise():
        async def succeed(): return found
        task = asyncio.create_task(succeed())
        await asyncio.sleep(0)
        updated_markets, updated = main.apply_discovery_result(None, task, known)
        assert updated_markets is found
        assert updated is True

    asyncio.run(exercise())
    assert inserted == ["new"]


def test_only_one_discovery_task_exists_at_a_time(monkeypatch):
    import asyncio
    from collector import main

    started = 0
    async def blocked():
        nonlocal started
        started += 1
        await asyncio.Event().wait()
    monkeypatch.setattr(main, "discover_markets", blocked)

    async def exercise():
        now = datetime.now(timezone.utc)
        first = main.start_discovery_if_due(None, None, now)
        second = main.start_discovery_if_due(first, None, now)
        await asyncio.sleep(0)
        assert second is first
        await main.cancel_discovery_task(first)

    asyncio.run(exercise())
    assert started == 1


def test_shutdown_cancels_inflight_discovery_cleanly(monkeypatch):
    import asyncio
    from collector import main

    finalized = asyncio.Event()
    async def blocked():
        try: await asyncio.Event().wait()
        finally: finalized.set()
    monkeypatch.setattr(main, "discover_markets", blocked)

    async def exercise():
        task = main.start_discovery_if_due(None, None, datetime.now(timezone.utc))
        await asyncio.sleep(0)
        await main.cancel_discovery_task(task)
        assert task.cancelled()
        assert finalized.is_set()

    asyncio.run(exercise())
