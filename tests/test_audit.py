from datetime import datetime, timedelta, timezone

import pandas as pd

from collector.audit_market import (as_array, determine_verdict, fetch_official,
                                    official_market, research_exclusion_reasons)


def test_official_resolution_retrieval_matches_metadata(monkeypatch):
    payload={"markets":[{"conditionId":"m","outcomes":"[\"Up\",\"Down\"]",
      "clobTokenIds":"[\"u\",\"d\"]","outcomePrices":"[\"1\",\"0\"]"}]}
    class Response:
        def raise_for_status(self): pass
        def json(self): return payload
    monkeypatch.setattr("collector.audit_market.httpx.get",lambda *args,**kwargs:Response())
    event,error=fetch_official("slug"); market=official_market(event,"m")
    labels=as_array(market["outcomes"]); prices=as_array(market["outcomePrices"])
    winner=next(label for label,price in zip(labels,prices) if float(price)>=.999)
    assert error is None and winner=="Up"


def test_zero_polymarket_observations_is_suspicious():
    assert determine_verdict(failures=[],suspicious=[],official_available=True,
      polymarket_event_count=0,token_sanity="INSUFFICIENT DATA")=="SUSPICIOUS"


def test_sufficient_consistent_market_passes():
    assert determine_verdict(failures=[],suspicious=[],official_available=True,
      polymarket_event_count=20,token_sanity="PASS")=="PASS"


def test_concrete_integrity_error_fails():
    finding=("HIGH","token mapping","IDs mismatch","demonstrated mismatch","source","fix")
    assert determine_verdict(failures=[finding],suspicious=[],official_available=True,
      polymarket_event_count=20,token_sanity="PASS")=="FAIL"


def eligibility_inputs(*, ages=None, missing=None, prices=(100.0, 101.0)):
    start = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=5)
    market = {"status": "completed", "resolved_outcome": "Up",
              "up_token_id": "up", "down_token_id": "down"}
    timestamps = [start + timedelta(seconds=1 + index*5) for index in range(60)]
    btc = pd.DataFrame({
        "exchange_timestamp": timestamps,
        "btc_value": [prices[index % len(prices)] for index in range(len(timestamps))],
    })
    ages = ages or {}
    missing = missing or set()
    checkpoints = {}
    for seconds in (60, 30, 20, 10, 5, 3, 2, 1):
        values = {}
        for outcome in ("Up", "Down"):
            age = ages.get(seconds, 1.0)
            quote = None if (seconds, outcome) in missing else (
                end-timedelta(seconds=seconds, milliseconds=100), .4, .6)
            values[outcome] = (quote, None if quote is None else age,
                               None if quote is None or age > 5 else .5)
        checkpoints[seconds] = (None, values)
    return dict(market=market, start=start, end=end, btc=btc,
                checkpoint_results=checkpoints, max_checkpoint_age=5)


def test_stale_checkpoint_pattern_is_not_research_eligible():
    inputs = eligibility_inputs(ages={60: 11.873, 30: 41.873, 20: 51.873,
                                      10: 61.873, 5: 66.873, 3: 68.873,
                                      2: 69.873, 1: 70.873})
    reasons = research_exclusion_reasons(**inputs)
    assert "Polymarket T-30 Up observation stale: 41.873s" in reasons
    assert determine_verdict(failures=[], suspicious=[], official_available=True,
        polymarket_event_count=20, token_sanity="PASS",
        research_eligible=not reasons) == "SUSPICIOUS"


def test_fresh_complete_checkpoints_are_research_eligible():
    assert research_exclusion_reasons(**eligibility_inputs()) == []


def test_missing_checkpoint_is_not_research_eligible():
    reasons = research_exclusion_reasons(**eligibility_inputs(missing={(10, "Down")}))
    assert reasons == ["Polymarket T-10 Down observation missing"]


def test_matching_btc_values_do_not_establish_completeness():
    reasons = research_exclusion_reasons(**eligibility_inputs(prices=(100.0, 100.0)))
    assert "inadequate BTC observation coverage: all 60 observations have the same price" in reasons
