import json
import time
from datetime import datetime, timezone

from collector.markets import connect_db
from collector.polymarket import insert_events, parse_polymarket


MARKET = {"market_id": "market", "up_token_id": "up", "down_token_id": "down"}
RECEIPT = datetime(2026, 8, 5, 12, 0, 1, tzinfo=timezone.utc)


def price_change_payload(change_count=20):
    changes = []
    for index in range(change_count):
        token = "up" if index % 2 == 0 else "down"
        changes.append({"asset_id": token, "best_bid": str(.40 + index / 1000),
                        "best_ask": str(.60 + index / 1000)})
    return json.dumps({"event_type": "price_change", "timestamp": "1785931200000",
                       "price_changes": changes})


def test_batch_insert_preserves_all_rows_counts_and_event_ids(tmp_path):
    con = connect_db(tmp_path / "batch.duckdb")
    quotes, trades = parse_polymarket(price_change_payload(), MARKET, RECEIPT)
    expected_ids = [row["event_id"] for row in quotes]

    assert insert_events(con, quotes, trades) == (20, 0)
    stored = con.execute("""
        SELECT event_id,market_id,source_timestamp,receipt_timestamp
        FROM polymarket_quotes ORDER BY event_id
    """).fetchall()
    assert [row[0] for row in stored] == sorted(expected_ids)
    assert {row[1] for row in stored} == {"market"}
    assert {row[2] for row in stored} == {datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)}
    assert {row[3] for row in stored} == {RECEIPT}
    assert con.execute("SELECT count(*) FROM polymarket_trades").fetchone()[0] == 0


def test_batch_insert_rejects_existing_and_in_batch_duplicates(tmp_path):
    con = connect_db(tmp_path / "duplicates.duckdb")
    quotes, _ = parse_polymarket(price_change_payload(4), MARKET, RECEIPT)

    assert insert_events(con, quotes[:2], []) == (2, 0)
    assert insert_events(con, quotes + [quotes[2]], []) == (2, 3)
    assert con.execute("SELECT count(*) FROM polymarket_quotes").fetchone()[0] == 4


def test_batch_insert_quotes_and_trades_have_exact_counts(tmp_path):
    con = connect_db(tmp_path / "mixed.duckdb")
    quotes, _ = parse_polymarket(price_change_payload(6), MARKET, RECEIPT)
    _, trades = parse_polymarket(json.dumps({
        "event_type": "last_trade_price", "asset_id": "up", "price": "0.55",
        "size": "12", "timestamp": "1785931200001",
    }), MARKET, RECEIPT)

    assert insert_events(con, quotes, trades) == (7, 0)
    assert con.execute("SELECT count(*) FROM polymarket_quotes").fetchone()[0] == 6
    assert con.execute("SELECT count(*) FROM polymarket_trades").fetchone()[0] == 1


def test_message_batch_reduces_database_calls_and_local_cost(tmp_path):
    quotes, _ = parse_polymarket(price_change_payload(200), MARKET, RECEIPT)

    old_con = connect_db(tmp_path / "old.duckdb")
    started = time.perf_counter()
    for row in quotes:
        old_con.execute("SELECT count(*) FROM polymarket_quotes WHERE event_id=?", [row["event_id"]]).fetchone()
        old_con.execute("INSERT OR IGNORE INTO polymarket_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        list(row.values()))
    old_elapsed = time.perf_counter() - started

    new_con = connect_db(tmp_path / "new.duckdb")
    started = time.perf_counter()
    assert insert_events(new_con, quotes, []) == (200, 0)
    new_elapsed = time.perf_counter() - started

    assert old_con.execute("SELECT * FROM polymarket_quotes ORDER BY event_id").fetchall() == \
           new_con.execute("SELECT * FROM polymarket_quotes ORDER BY event_id").fetchall()
    assert new_elapsed < old_elapsed
