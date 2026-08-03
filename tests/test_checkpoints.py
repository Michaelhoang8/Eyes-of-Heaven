from datetime import timedelta
import pytest

from collector.markets import connect_db
from dashboard.queries import checkpoint_table
from tests.test_queries import setup_market


def add_quote(con,event_id,ts,bid=.4,ask=.6):
    con.execute("INSERT INTO polymarket_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
      [event_id,ts,ts,"m","u","Up","best_bid_ask",bid,ask,None,None])


def test_checkpoint_uses_latest_not_future(tmp_path):
    con=connect_db(tmp_path/"x.duckdb"); start,end=setup_market(con); target=end-timedelta(seconds=60)
    add_quote(con,"past",target-timedelta(seconds=2),.2,.4)
    add_quote(con,"future",target+timedelta(seconds=1),.8,1.0)
    row=checkpoint_table(con,"m",seconds=(60,)).iloc[0]
    assert row.up_midpoint == pytest.approx(.3)


def test_missing_or_stale_checkpoint_is_null(tmp_path):
    con=connect_db(tmp_path/"x.duckdb"); start,end=setup_market(con)
    add_quote(con,"stale",end-timedelta(seconds=90))
    assert checkpoint_table(con,"m",seconds=(60,)).iloc[0].up_midpoint is None
