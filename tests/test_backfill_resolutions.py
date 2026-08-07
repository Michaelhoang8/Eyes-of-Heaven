from datetime import datetime, timedelta, timezone

from collector.backfill_resolutions import backfill
from collector.markets import connect_db


NOW = datetime(2026, 8, 7, 12, tzinfo=timezone.utc)


def add_market(con, market_id="market", *, status="active", outcome=None):
    start = NOW - timedelta(hours=1)
    end = start + timedelta(minutes=5)
    con.execute("INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
        market_id, "event", "btc-updown-5m-slug", "question", start, end,
        "up", "down", status, start-timedelta(minutes=5), outcome, start])
    return start, end


def official(*, closed=True, prices=("1", "0"), tokens=("up", "down")):
    event = {"id": "event", "markets": [{
        "conditionId": "market", "slug": "btc-updown-5m-slug", "closed": closed,
        "outcomes": ["Up", "Down"], "clobTokenIds": list(tokens),
        "outcomePrices": list(prices)}]}
    return lambda _slug: (event, None)


def test_ended_active_market_is_completed_from_official_resolution(tmp_path):
    con = connect_db(tmp_path / "resolved.duckdb")
    add_market(con)
    results = backfill(con, now=NOW, fetcher=official(), clock=lambda: NOW)
    assert [(result.action, result.outcome) for result in results] == [("updated", "Up")]
    assert con.execute("SELECT status,resolved_outcome,up_token_id,down_token_id FROM markets").fetchone() == (
        "completed", "Up", "up", "down")


def test_unresolved_official_market_remains_unchanged(tmp_path):
    con = connect_db(tmp_path / "unresolved.duckdb")
    add_market(con)
    results = backfill(con, now=NOW, fetcher=official(closed=False))
    assert results[0].action == "unchanged"
    assert con.execute("SELECT status,resolved_outcome FROM markets").fetchone() == ("active", None)


def test_already_completed_market_is_not_selected(tmp_path):
    con = connect_db(tmp_path / "completed.duckdb")
    add_market(con, status="completed", outcome="Down")
    called = False
    def fetcher(_slug):
        nonlocal called
        called = True
        return official()(_slug)
    assert backfill(con, now=NOW, fetcher=fetcher) == []
    assert not called
    assert con.execute("SELECT status,resolved_outcome FROM markets").fetchone() == (
        "completed", "Down")


def test_mapping_mismatch_is_rejected_and_observations_are_unchanged(tmp_path):
    con = connect_db(tmp_path / "mapping.duckdb")
    start, _end = add_market(con)
    con.execute("INSERT INTO bitcoin_events VALUES (?,?,?,?,?,?,?,?)",
                ["btc", start, start, "trade", 100.0, None, None, None])
    con.execute("INSERT INTO polymarket_top_of_book VALUES (?,?,?,?,?,?,?,?,?)",
                ["quote", "market", "up", "Up", "snapshot", .4, .6, start, start])
    before = (con.execute("SELECT * FROM bitcoin_events").fetchall(),
              con.execute("SELECT * FROM polymarket_top_of_book").fetchall())
    results = backfill(con, now=NOW, fetcher=official(tokens=("wrong", "down")))
    after = (con.execute("SELECT * FROM bitcoin_events").fetchall(),
             con.execute("SELECT * FROM polymarket_top_of_book").fetchall())
    assert results[0].action == "unchanged"
    assert "mapping" in results[0].detail
    assert before == after


def test_dry_run_performs_zero_writes(tmp_path):
    con = connect_db(tmp_path / "dry.duckdb")
    add_market(con)
    before = con.execute("SELECT * FROM markets").fetchall()
    results = backfill(con, now=NOW, fetcher=official(), dry_run=True)
    assert results[0].action == "would update"
    assert con.execute("SELECT * FROM markets").fetchall() == before
