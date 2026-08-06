import json
from datetime import datetime, timezone

from collector.markets import connect_db
from collector.polymarket import (insert_top_states, parse_polymarket,
                                  persist_compact_messages)


MARKET = {"market_id": "market", "up_token_id": "up", "down_token_id": "down"}
RECEIPT = datetime(2026, 8, 5, 12, 0, 1, tzinfo=timezone.utc)


def top_row(event_id="one"):
    return {"event_id": event_id, "market_id": "market", "token_id": "up",
            "outcome": "Up", "event_type": "best_bid_ask", "best_bid": .4,
            "best_ask": .6, "last_changed_at": RECEIPT, "state_as_of": RECEIPT}


def test_compact_batch_insert_preserves_rows_and_deduplication(tmp_path):
    con = connect_db(tmp_path / "compact.duckdb")
    rows = [top_row("one"), top_row("two")]
    assert insert_top_states(con, rows) == (2, 0)
    assert insert_top_states(con, rows + [top_row("three")]) == (1, 2)
    assert con.execute("SELECT count(*) FROM polymarket_top_of_book").fetchone()[0] == 3


def test_price_change_no_longer_produces_quote_rows(tmp_path):
    payload = json.dumps({"event_type": "price_change", "timestamp": "1785931200000",
        "price_changes": [{"asset_id": "up", "best_bid": ".4", "best_ask": ".6"}]})
    quotes, trades = parse_polymarket(payload, MARKET, RECEIPT)
    assert quotes == [] and trades == []
    con = connect_db(tmp_path / "ignored.duckdb")
    persist_compact_messages(con, MARKET, [(payload, RECEIPT, 1)], {})
    assert con.execute("SELECT count(*) FROM polymarket_top_of_book").fetchone()[0] == 0


def test_trades_remain_unchanged(tmp_path):
    payload = json.dumps({"event_type": "last_trade_price", "asset_id": "up",
        "price": ".55", "size": "12", "timestamp": "1785931200001"})
    expected = parse_polymarket(payload, MARKET, RECEIPT)[1][0]
    con = connect_db(tmp_path / "trade.duckdb")
    persist_compact_messages(con, MARKET, [(payload, RECEIPT, 1)], {})
    stored = con.execute("SELECT * FROM polymarket_trades").fetchone()
    assert stored == tuple(expected.values())
