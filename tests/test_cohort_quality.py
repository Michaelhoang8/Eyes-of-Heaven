from datetime import datetime, timedelta, timezone

from collector.cohort_quality import (CHECKPOINTS, classify_expected_start,
                                      evaluate_cohort, expected_starts,
                                      market_quality)
from collector.markets import connect_db


BASE = datetime(2026, 8, 6, 12, 5, tzinfo=timezone.utc)


def add_market(con, market_id, start=BASE, minutes=5, status="completed", outcome="Up"):
    end = start + timedelta(minutes=minutes)
    con.execute("INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
        market_id, "event", f"btc-updown-{minutes}m-{int(start.timestamp())}", "question",
        start, end, f"{market_id}-up", f"{market_id}-down", status, start, outcome, start])
    return {"market_id": market_id, "start_time": start, "end_time": end,
            "up_token_id": f"{market_id}-up", "down_token_id": f"{market_id}-down",
            "status": status, "resolved_outcome": outcome}


def add_healthy_data(con, market, *, stale_poly=False, bad_btc=False):
    start, end = market["start_time"], market["end_time"]
    btc_end = 10 if bad_btc else int((end-start).total_seconds())
    for second in range(0, btc_end + 1, 5):
        timestamp = start + timedelta(seconds=second)
        con.execute("INSERT INTO bitcoin_events VALUES (?,?,?,?,?,?,?,?)", [
            f"btc-{market['market_id']}-{second}", timestamp, timestamp, "trade",
            100 + second % 10, None, None, None])
    for seconds in CHECKPOINTS:
        target = end - timedelta(seconds=seconds)
        state_time = target - timedelta(seconds=6 if stale_poly else 1)
        for outcome in ("Up", "Down"):
            token = market[f"{outcome.lower()}_token_id"]
            con.execute("INSERT INTO polymarket_top_of_book VALUES (?,?,?,?,?,?,?,?,?)", [
                f"{market['market_id']}-{seconds}-{outcome}", market["market_id"], token,
                outcome, "snapshot", .4, .6, state_time, state_time])


def test_expected_five_minute_intervals_are_utc_aligned():
    starts = expected_starts(BASE + timedelta(seconds=17), BASE + timedelta(minutes=11), 5)
    assert starts == [BASE + timedelta(minutes=5), BASE + timedelta(minutes=10)]


def test_expected_fifteen_minute_intervals_are_utc_aligned():
    starts = expected_starts(BASE, BASE + timedelta(minutes=35), 15)
    assert starts == [BASE + timedelta(minutes=10), BASE + timedelta(minutes=25)]


def test_missing_market_detection_reports_no_row(tmp_path):
    con = connect_db(tmp_path / "missing.duckdb")
    first = add_market(con, "first", BASE)
    last = add_market(con, "last", BASE + timedelta(minutes=10))
    add_healthy_data(con, first)
    add_healthy_data(con, last)
    _quality, summary, missing = evaluate_cohort(con, BASE - timedelta(seconds=1))
    assert {key: summary[5][key] for key in ("expected", "stored", "missing")} == {
        "expected": 3, "stored": 2, "missing": 1}
    assert [(row["start_time"], row["database_state"]) for row in missing] == [
        (BASE + timedelta(minutes=5), "no market row at all")]


def test_unresolved_completed_market_classification(tmp_path):
    con = connect_db(tmp_path / "unresolved.duckdb")
    market = add_market(con, "unresolved", outcome=None)
    add_healthy_data(con, market)
    quality = market_quality(con, market)
    state = classify_expected_start([market], {"unresolved": quality})
    assert state == "completed market but missing resolution"
    assert not quality["polymarket_calibration_eligible"]


def test_polymarket_and_btc_eligibility_are_separate(tmp_path):
    con = connect_db(tmp_path / "flags.duckdb")
    healthy = add_market(con, "healthy")
    add_healthy_data(con, healthy)
    result = market_quality(con, healthy)
    assert result["polymarket_calibration_eligible"]
    assert result["btc_feature_eligible"]
    assert result["fully_eligible"]

    stale = add_market(con, "stale", BASE + timedelta(minutes=5))
    add_healthy_data(con, stale, stale_poly=True)
    result = market_quality(con, stale)
    assert not result["polymarket_calibration_eligible"]
    assert result["btc_feature_eligible"]
    assert not result["fully_eligible"]

    btc_bad = add_market(con, "btc-bad", BASE + timedelta(minutes=10))
    add_healthy_data(con, btc_bad, bad_btc=True)
    result = market_quality(con, btc_bad)
    assert result["polymarket_calibration_eligible"]
    assert not result["btc_feature_eligible"]
    assert not result["fully_eligible"]
