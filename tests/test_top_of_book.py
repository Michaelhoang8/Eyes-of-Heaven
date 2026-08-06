import json
from datetime import datetime, timedelta, timezone

import pytest

from collector.markets import connect_db
from collector.polymarket import persist_compact_messages, persist_state_snapshots
from dashboard.queries import checkpoint_table


MARKET = {"market_id": "market", "up_token_id": "up", "down_token_id": "down"}
BASE = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)


def message(kind, token, timestamp, **values):
    body = {"event_type": kind, "asset_id": token,
            "timestamp": str(int(timestamp.timestamp() * 1000)), **values}
    return json.dumps(body)


def book(token, timestamp, bid, ask):
    return message("book", token, timestamp,
                   bids=[{"price": str(bid), "size": "10"}],
                   asks=[{"price": str(ask), "size": "10"}])


def bba(token, timestamp, bid, ask):
    return message("best_bid_ask", token, timestamp,
                   best_bid=str(bid), best_ask=str(ask))


def test_reconnect_book_resets_state_and_bba_updates_it(tmp_path):
    con = connect_db(tmp_path / "reconnect.duckdb")
    state = {}
    persist_compact_messages(con, MARKET, [
        (book("up", BASE, .40, .60), BASE, 1),
        (bba("up", BASE + timedelta(seconds=1), .45, .55), BASE + timedelta(seconds=1, milliseconds=10), 1),
    ], state)
    assert state["up"]["best_bid"] == pytest.approx(.45)

    # A new subscription generation clears all old token state. Its fresh book
    # is the authoritative seed, even when the prior connection had newer data.
    reconnect = BASE + timedelta(seconds=2)
    persist_compact_messages(
        con, MARKET, [(book("up", reconnect, .20, .80), reconnect + timedelta(milliseconds=10), 2)], state)
    assert state["_generation"] == 2
    assert state["up"]["best_bid"] == pytest.approx(.20)
    assert con.execute("""SELECT event_type,best_bid,best_ask FROM polymarket_top_of_book
                          ORDER BY state_as_of DESC,event_id DESC LIMIT 1""").fetchone() == (
        "book_seed", .20, .80)


def test_periodic_snapshots_refresh_state_as_of_but_not_last_changed(tmp_path):
    con = connect_db(tmp_path / "snapshots.duckdb")
    state = {}
    source = BASE - timedelta(seconds=20)
    persist_compact_messages(con, MARKET, [(book("up", source, .4, .6), BASE, 1)], state)
    persist_state_snapshots(con, MARKET, state, BASE + timedelta(seconds=1, milliseconds=100))
    persist_state_snapshots(con, MARKET, state, BASE + timedelta(seconds=1, milliseconds=900))
    persist_state_snapshots(con, MARKET, state, BASE + timedelta(seconds=2, milliseconds=100))

    rows = con.execute("""SELECT event_type,last_changed_at,state_as_of
                          FROM polymarket_top_of_book ORDER BY state_as_of""").fetchall()
    assert [row[0] for row in rows] == ["book_seed", "snapshot", "snapshot"]
    assert all(row[1] == source for row in rows)
    assert rows[-1][2] == BASE + timedelta(seconds=2, milliseconds=100)


def _insert_market(con):
    end = BASE + timedelta(minutes=5)
    con.execute("""INSERT INTO markets VALUES
        ('market','event','slug','question',?,?,?,?,'completed',?,?,?)""", [
        BASE, end, "up", "down", BASE, "Up", BASE])
    return end


def _insert_state(con, event_id, outcome, bid, ask, changed, as_of):
    token = outcome.lower()
    con.execute("INSERT INTO polymarket_top_of_book VALUES (?,?,?,?,?,?,?,?,?)", [
        event_id, "market", token, outcome, "snapshot", bid, ask, changed, as_of])


def test_checkpoint_is_deterministic_never_looks_forward_and_uses_state_as_of_freshness(tmp_path):
    con = connect_db(tmp_path / "checkpoint.duckdb")
    end = _insert_market(con)
    target = end - timedelta(seconds=60)
    old_change = target - timedelta(minutes=2)

    # Same state timestamp and change timestamp: event_id is the final tie-breaker.
    _insert_state(con, "a", "Up", .30, .50, old_change, target - timedelta(seconds=1))
    _insert_state(con, "z", "Up", .50, .70, old_change, target - timedelta(seconds=1))
    _insert_state(con, "future", "Up", .80, .90, target, target + timedelta(microseconds=1))
    _insert_state(con, "down", "Down", .35, .45, old_change, target - timedelta(seconds=1))

    row = checkpoint_table(con, "market", seconds=(60,)).iloc[0]
    assert row.up_midpoint == pytest.approx(.60)
    assert row.down_midpoint == pytest.approx(.40)
    # last_changed_at is deliberately stale; freshness comes from state_as_of.
    assert old_change < target - timedelta(seconds=5)


def test_additive_migration_keeps_raw_history_and_uses_it_only_as_fallback(tmp_path):
    con = connect_db(tmp_path / "migration.duckdb")
    legacy = ["legacy", BASE, BASE, "legacy-market", "up", "Up",
              "best_bid_ask", .4, .6, None, None]
    con.execute("INSERT INTO polymarket_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?)", legacy)
    assert con.execute("SELECT event_id FROM polymarket_quote_points").fetchall() == [("legacy",)]

    con.execute("INSERT INTO polymarket_top_of_book VALUES (?,?,?,?,?,?,?,?,?)", [
        "compact", "legacy-market", "up", "Up", "snapshot", .45, .55, BASE, BASE])
    assert con.execute("SELECT event_id FROM polymarket_quote_points").fetchall() == [("compact",)]
    assert con.execute("SELECT count(*) FROM polymarket_quotes").fetchone()[0] == 1

    earlier = ["earlier", BASE - timedelta(seconds=1), BASE - timedelta(seconds=1),
               "legacy-market", "up", "Up", "best_bid_ask", .35, .65, None, None]
    con.execute("INSERT INTO polymarket_quotes VALUES (?,?,?,?,?,?,?,?,?,?,?)", earlier)
    assert con.execute("SELECT event_id FROM polymarket_quote_points ORDER BY state_as_of").fetchall() == [
        ("earlier",), ("compact",)]
