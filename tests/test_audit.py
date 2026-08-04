from collector.audit_market import as_array, determine_verdict, fetch_official, official_market


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
