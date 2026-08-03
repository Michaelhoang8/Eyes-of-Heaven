from datetime import datetime, timezone

from collector.bitcoin import insert_event, ms_time, parse_binance
from collector.markets import connect_db, map_outcome_tokens, normalize_market, repair_interval_starts, upsert_market
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
