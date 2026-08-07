from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from collector.calibration import (calibration_bins, calibration_rows,
                                   checkpoint_observation, descriptive_metrics,
                                   outcome_value, probability_bin)
from collector.cohort_quality import CHECKPOINTS
from collector.markets import connect_db


BASE = datetime(2026, 8, 7, 12, tzinfo=timezone.utc)


def add_market(con, market_id, minutes, *, outcome="Up", stale=False):
    end = BASE + timedelta(minutes=minutes)
    con.execute("INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
        market_id, "event", f"btc-updown-{minutes}m-slug", "question", BASE, end,
        f"{market_id}-up", f"{market_id}-down", "completed", BASE, outcome, BASE])
    for seconds in CHECKPOINTS:
        target = end - timedelta(seconds=seconds)
        state = target - timedelta(seconds=6 if stale else 1)
        for label in ("Up", "Down"):
            con.execute("INSERT INTO polymarket_top_of_book VALUES (?,?,?,?,?,?,?,?,?)", [
                f"{market_id}-{seconds}-{label}", market_id, f"{market_id}-{label.lower()}",
                label, "snapshot", .6 if label == "Up" else .2,
                .8 if label == "Up" else .4, state, state])
    return end


def test_up_outcome_encoding():
    assert outcome_value("Up") == 1
    assert outcome_value("Down") == 0
    with pytest.raises(ValueError):
        outcome_value("Unknown")


def test_brier_score_calculation():
    rows = pd.DataFrame({"p_up": [.8, .3], "y": [1, 0]})
    metrics = descriptive_metrics(rows)
    assert metrics["brier"] == pytest.approx((.2**2 + .3**2) / 2)
    assert metrics["mae"] == pytest.approx(.25)


def test_checkpoint_selection_never_looks_forward(tmp_path):
    con = connect_db(tmp_path / "future.duckdb")
    end = add_market(con, "market", 5)
    target = end - timedelta(seconds=60)
    con.execute("INSERT INTO polymarket_top_of_book VALUES (?,?,?,?,?,?,?,?,?)", [
        "future", "market", "market-up", "Up", "snapshot", .9, 1.0,
        target + timedelta(milliseconds=1), target + timedelta(milliseconds=1)])
    selected = checkpoint_observation(
        con, market_id="market", outcome="Up", start=BASE, checkpoint=target)
    assert selected["midpoint"] == pytest.approx(.7)
    assert selected["state_as_of"] <= target


def test_polymarket_ineligible_market_is_excluded(tmp_path):
    con = connect_db(tmp_path / "excluded.duckdb")
    add_market(con, "healthy", 5)
    add_market(con, "stale", 5, stale=True)
    rows = calibration_rows(con, BASE - timedelta(seconds=1))
    assert set(rows.market_id) == {"healthy"}
    assert len(rows) == len(CHECKPOINTS)


def test_interval_types_remain_separate(tmp_path):
    con = connect_db(tmp_path / "intervals.duckdb")
    add_market(con, "five", 5)
    add_market(con, "fifteen", 15, outcome="Down")
    rows = calibration_rows(con, BASE - timedelta(seconds=1))
    assert rows.groupby("interval_minutes").market_id.nunique().to_dict() == {5: 1, 15: 1}
    assert set(rows[rows.interval_minutes == 5].market_id) == {"five"}
    assert set(rows[rows.interval_minutes == 15].market_id) == {"fifteen"}


def test_calibration_bin_boundaries_and_small_sample_label():
    probabilities = [0, .099, .10, .299, .30, .699, .70, .899, .90, 1.0]
    assert [probability_bin(value) for value in probabilities] == [
        "0.00-0.10", "0.00-0.10", "0.10-0.30", "0.10-0.30",
        "0.30-0.70", "0.30-0.70", "0.70-0.90", "0.70-0.90",
        "0.90-1.00", "0.90-1.00"]
    rows = pd.DataFrame({"p_up": probabilities, "y": [0, 0, 0, 0, 1, 1, 1, 1, 1, 1]})
    bins = calibration_bins(rows)
    assert all(item["n"] == 2 for item in bins)
    assert all(item["sample_status"] == "INSUFFICIENT SAMPLE" for item in bins)
