from datetime import datetime, timedelta, timezone

from collector.markets import connect_db, upsert_market
from dashboard.queries import completed_market_summary, market_series, midpoint


def setup_market(con):
    start=datetime(2026,1,1,tzinfo=timezone.utc); end=start+timedelta(minutes=5)
    upsert_market(con,{"market_id":"m","event_id":None,"slug":"s","question":"q","start_time":start,"end_time":end,
      "up_token_id":"u","down_token_id":"d","status":"completed","discovered_at":start,"resolved_outcome":"Up","updated_at":end})
    for i,price in [(1,100.0),(299,110.0),(300,999.0)]:
        ts=start+timedelta(seconds=i)
        con.execute("INSERT INTO bitcoin_events VALUES (?,?,?,?,?,?,?,?)",[f"b{i}",ts,ts,"trade",price,1,None,None])
    return start,end


def test_midpoint_missing_and_valid():
    assert midpoint(.4,.6)==.5
    assert midpoint(None,.6) is None


def test_btc_open_close_and_completed_summary(tmp_path):
    con=connect_db(tmp_path/"x.duckdb"); setup_market(con)
    row=completed_market_summary(con).iloc[0]
    assert row.btc_open==100 and row.btc_close==999 and row.btc_change==899
    assert row.binance_count==3 and row.winner=="Up"


def test_market_series_queries_use_portable_aliases(tmp_path):
    con=connect_db(tmp_path/"x.duckdb"); setup_market(con)
    start=con.execute("SELECT start_time FROM markets WHERE market_id='m'").fetchone()[0]
    con.execute("INSERT INTO polymarket_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
      ["inside",start,start,"m","u","Up","book",.4,.6,None,None])
    outside=start-timedelta(seconds=1)
    con.execute("INSERT INTO polymarket_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
      ["outside",outside,outside,"m","u","Up","book",.1,.2,None,None])
    btc,poly=market_series(con,"m")
    assert list(btc.columns)==["event_time","price"]
    assert len(btc)==3
    assert list(poly.columns)==["event_time","outcome","midpoint"]
    assert len(poly)==1 and poly.iloc[0].midpoint==.5


def test_completed_summary_excludes_outside_market_events(tmp_path):
    con=connect_db(tmp_path/"x.duckdb"); start,end=setup_market(con)
    for event_id,ts in (("inside",start+timedelta(seconds=10)),("outside",end+timedelta(seconds=1))):
        con.execute("INSERT INTO polymarket_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
          [event_id,ts,ts,"m","u","Up","book",.4,.6,None,None])
    assert completed_market_summary(con).iloc[0].polymarket_count==1
