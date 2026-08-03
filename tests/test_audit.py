from collector.audit_market import as_array, fetch_official, official_market


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
