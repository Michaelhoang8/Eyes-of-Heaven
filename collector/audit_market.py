from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from typing import Any

import httpx
import pandas as pd

from dashboard.queries import completed_market_summary, market_series, open_connection

GAMMA_EVENT_URL = "https://gamma-api.polymarket.com/events/slug/{slug}"
Finding = tuple[str, str, str, str, str, str]


def determine_verdict(*, failures: list[Finding], suspicious: list[Finding],
                      official_available: bool, polymarket_event_count: int,
                      token_sanity: str, research_eligible: bool = True) -> str:
    if failures:
        return "FAIL"
    if (suspicious or not official_available or polymarket_event_count == 0
            or token_sanity != "PASS" or not research_eligible):
        return "SUSPICIOUS"
    return "PASS"


def utc(value: Any) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize("UTC")
    return parsed.tz_convert("UTC").to_pydatetime()


def fmt(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "NULL"
    if isinstance(value, (datetime, pd.Timestamp)):
        return utc(value).isoformat()
    if isinstance(value, float):
        return f"{value:.10g}"
    return str(value)


def expected_minutes(market: dict[str, Any]) -> int | None:
    slug = market.get("slug") or ""
    match = re.search(r"-(5|15)m-", slug)
    return int(match.group(1)) if match else None


def fetch_official(slug: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        response = httpx.get(GAMMA_EVENT_URL.format(slug=slug), timeout=15)
        response.raise_for_status()
        event = response.json()
        return event, None
    except Exception as exc:
        return None, str(exc)


def official_market(event: dict[str, Any] | None, market_id: str) -> dict[str, Any] | None:
    if not event:
        return None
    return next((m for m in event.get("markets", []) if str(m.get("conditionId") or m.get("id")) == market_id), None)


def as_array(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        return [str(v) for v in json.loads(value)]
    return []


def nearest(con, sql: str, params: list[Any]):
    return con.execute(sql, params).fetchone()


def research_exclusion_reasons(*, market: dict[str, Any], start: datetime, end: datetime,
                               btc: pd.DataFrame, checkpoint_results: dict,
                               max_checkpoint_age: float, max_btc_boundary_age: float = 5,
                               max_btc_gap: float = 30) -> list[str]:
    reasons = []
    if market.get("status") != "completed":
        reasons.append("market is not marked completed")
    if market.get("resolved_outcome") not in ("Up", "Down"):
        reasons.append("market has no resolved Up/Down outcome")
    up, down = market.get("up_token_id"), market.get("down_token_id")
    if not up or not down or up == down:
        reasons.append("invalid Up/Down token mapping")

    if btc.empty:
        reasons.append("inadequate BTC observation coverage: no observations")
    else:
        timestamps = [utc(value) for value in btc.exchange_timestamp]
        start_age = (timestamps[0] - start).total_seconds()
        end_age = (end - timestamps[-1]).total_seconds()
        if start_age > max_btc_boundary_age:
            reasons.append(f"inadequate BTC start coverage: first observation is {start_age:.3f}s after start")
        if end_age > max_btc_boundary_age:
            reasons.append(f"inadequate BTC end coverage: last observation is {end_age:.3f}s before end")
        if len(timestamps) < 2:
            reasons.append("inadequate BTC observation coverage: fewer than 2 observations")
        else:
            largest_gap = max((right-left).total_seconds() for left, right in zip(timestamps, timestamps[1:]))
            if largest_gap > max_btc_gap:
                reasons.append(f"inadequate BTC observation coverage: maximum gap is {largest_gap:.3f}s")
            if btc.btc_value.nunique(dropna=True) < 2:
                reasons.append(f"inadequate BTC observation coverage: all {len(btc)} observations have the same price")

    for seconds in (60, 30, 20, 10, 5, 3, 2, 1):
        values = checkpoint_results.get(seconds, (None, {}))[1]
        for outcome in ("Up", "Down"):
            quote, age, _midpoint = values.get(outcome, (None, None, None))
            if quote is None or age is None:
                reasons.append(f"Polymarket T-{seconds} {outcome} observation missing")
            elif age > max_checkpoint_age:
                reasons.append(f"Polymarket T-{seconds} {outcome} observation stale: {age:.3f}s")
    return reasons


def audit(market_id: str | None, latest: bool, max_age: float, database: str | None = None) -> int:
    con = open_connection(database) if database else open_connection()
    if latest:
        selected = con.execute("""SELECT * FROM markets
          WHERE status='completed' OR resolved_outcome IS NOT NULL ORDER BY end_time DESC LIMIT 1""").df()
    else:
        selected = con.execute("SELECT * FROM markets WHERE market_id=?", [market_id]).df()
    if selected.empty:
        print("Market not found or no completed market is available.")
        return 2
    market = selected.iloc[0].to_dict(); market_id = str(market["market_id"])
    start, end = utc(market["start_time"]), utc(market["end_time"])
    duration = (end-start).total_seconds() if start and end else None
    intended = expected_minutes(market)
    failures: list[Finding] = []
    suspicious: list[Finding] = []

    print("MARKET METADATA")
    for label,key in (("market_id","market_id"),("event_id","event_id"),("slug","slug"),("question/title","question")):
        print(f"{label}: {fmt(market.get(key))}")
    print(f"market start UTC: {fmt(start)}")
    print(f"market end UTC: {fmt(end)}")
    print(f"market duration: {fmt(duration)} seconds")
    print(f"discovery timestamp UTC: {fmt(market.get('discovered_at'))}")
    print(f"market status: {fmt(market.get('status'))}")
    print(f"resolved outcome: {fmt(market.get('resolved_outcome'))}")
    print(f"Up token ID: {market.get('up_token_id')}")
    print(f"Down token ID: {market.get('down_token_id')}")
    checks = {
        "start < end": bool(start and end and start < end),
        "duration matches intended interval": bool(intended and duration == intended*60),
        "Up/Down token IDs differ": market.get("up_token_id") != market.get("down_token_id"),
        "resolved outcome is Up/Down": market.get("resolved_outcome") in ("Up","Down"),
    }
    for name,passed in checks.items(): print(f"{name}: {'PASS' if passed else 'FAIL'}")
    if not checks["duration matches intended interval"]:
        failures.append(("HIGH","market discovery",f"Stored duration {duration}s; slug specifies {intended}m","startDate is creation/listing time","collector/markets.py normalize_market","Use eventStartTime for interval start"))
    if not checks["Up/Down token IDs differ"]:
        failures.append(("HIGH","token mapping","Up and Down token IDs are identical","invalid metadata","collector/markets.py map_outcome_tokens","Reject identical token IDs"))

    qstats = con.execute("""SELECT count(*) total,
      min(state_as_of) first_source,
      max(state_as_of) last_source,
      min(receipt_timestamp) first_receipt,max(receipt_timestamp) last_receipt,
      count(*) FILTER (WHERE outcome='Up') up_count,count(*) FILTER (WHERE outcome='Down') down_count,
      count(*) FILTER (WHERE state_as_of< ?) before_count,
      count(*) FILTER (WHERE state_as_of> ?) after_count
      FROM polymarket_quote_points WHERE market_id=?""",[start,end,market_id]).fetchone()
    tstats = con.execute("""SELECT count(*),
      count(*) FILTER (WHERE COALESCE(source_timestamp,receipt_timestamp)< ?),
      count(*) FILTER (WHERE COALESCE(source_timestamp,receipt_timestamp)> ?)
      FROM polymarket_trades WHERE market_id=?""",[start,end,market_id]).fetchone()
    print("\nPOLYMARKET RAW EVENT WINDOW")
    for label,value in zip(("total quote count","first event timestamp","last event timestamp","first receipt timestamp","last receipt timestamp","Up quote count","Down quote count","quotes before start","quotes after end"),qstats): print(f"{label}: {fmt(value)}")
    print(f"total trade count: {tstats[0]}")
    print(f"trades before start: {tstats[1]}"); print(f"trades after end: {tstats[2]}")
    in_window_polymarket_events = (qstats[0]-qstats[7]-qstats[8]) + (tstats[0]-tstats[1]-tstats[2])
    if in_window_polymarket_events == 0:
        suspicious.append(("MEDIUM","Polymarket data completeness",
          "no Polymarket quotes or trades were recorded during the market interval",
          "cause unavailable from the recorded data","collector/audit_market.py audit",
          "Collect more data before treating this market as fully validated"))
    outside = qstats[7]+qstats[8]+tstats[1]+tstats[2]
    if outside:
        print("Outside-window samples:")
        samples=con.execute("""SELECT event_id,outcome,event_type,state_as_of ts
          FROM polymarket_quote_points WHERE market_id=? AND (state_as_of<? OR state_as_of>?)
          ORDER BY state_as_of,receipt_timestamp,event_id LIMIT 5""",[market_id,start,end]).fetchall()
        for sample in samples: print("  "+" | ".join(fmt(x) for x in sample))
        failures.append(("HIGH","Polymarket ingestion window",f"{outside} raw events assigned after/before the market interval",
          "collector switched subscriptions only on its periodic discovery timer","collector/main.py run",
          "Wake discovery at the selected market end and retain legacy raw rows for auditability"))

    price_expr="COALESCE(price, CASE WHEN best_bid IS NOT NULL AND best_ask IS NOT NULL THEN (best_bid+best_ask)/2 END)"
    btc=con.execute(f"""SELECT exchange_timestamp,{price_expr} AS btc_value FROM bitcoin_events
      WHERE exchange_timestamp BETWEEN ? AND ? AND {price_expr} IS NOT NULL ORDER BY exchange_timestamp""",[start,end]).df()
    print("\nBTC EVENT WINDOW")
    print("BTC events are global; queries associate them by market_start <= exchange_timestamp <= market_end.")
    print(f"first observation timestamp: {fmt(btc.event_time.iloc[0] if 'event_time' in btc and len(btc) else (btc.exchange_timestamp.iloc[0] if len(btc) else None))}")
    print(f"last observation timestamp: {fmt(btc.exchange_timestamp.iloc[-1] if len(btc) else None)}")
    print(f"number of observations: {len(btc)}")
    for label,value in (("first BTC price",btc.btc_value.iloc[0] if len(btc) else None),("last BTC price",btc.btc_value.iloc[-1] if len(btc) else None),("minimum BTC price",btc.btc_value.min() if len(btc) else None),("maximum BTC price",btc.btc_value.max() if len(btc) else None)): print(f"{label}: {fmt(value)}")
    open_row=(utc(btc.exchange_timestamp.iloc[0]),float(btc.btc_value.iloc[0])) if len(btc) else (None,None)
    close_row=(utc(btc.exchange_timestamp.iloc[-1]),float(btc.btc_value.iloc[-1])) if len(btc) else (None,None)
    change=close_row[1]-open_row[1] if open_row[1] is not None else None
    print("\nRECOMPUTED BTC OPEN/CLOSE")
    print(f"open timestamp: {fmt(open_row[0])}; distance from start: {fmt((open_row[0]-start).total_seconds() if open_row[0] else None)}s")
    print(f"close timestamp: {fmt(close_row[0])}; distance before end: {fmt((end-close_row[0]).total_seconds() if close_row[0] else None)}s")
    print(f"BTC change $: {fmt(change)}")
    print(f"BTC change %: {fmt(change/open_row[1]*100 if change is not None else None)}")
    print(f"BTC change basis points: {fmt(change/open_row[1]*10000 if change is not None else None)}")
    if open_row[0] and (open_row[0]-start).total_seconds()>5:
        suspicious.append(("MEDIUM","BTC data completeness",f"First BTC observation is {(open_row[0]-start).total_seconds():.3f}s after market start",
          "collector began after this market opened","dashboard/queries.py completed_market_summary",
          "Treat the reported open as earliest collected observation; do not fabricate the missing opening price"))

    summary=completed_market_summary(con); dashboard=summary[summary.market_id==market_id].iloc[0]
    for name,stored,recomputed in (("BTC open",dashboard.btc_open,open_row[1]),("BTC close",dashboard.btc_close,close_row[1]),("BTC change",dashboard.btc_change,change)):
        match=(pd.isna(stored) and recomputed is None) or (recomputed is not None and abs(float(stored)-recomputed)<1e-8)
        print(f"{name}: STORED VALUE={fmt(stored)} | RECOMPUTED VALUE={fmt(recomputed)} | DIFFERENCE={fmt(float(stored)-recomputed if recomputed is not None else None)} | {'PASS' if match else 'FAIL'}")

    event,network_error=fetch_official(str(market.get("slug"))); official=official_market(event,market_id)
    labels=as_array(official.get("outcomes")) if official else []
    tokens=as_array(official.get("clobTokenIds")) if official else []
    prices=as_array(official.get("outcomePrices")) if official else []
    official_map=dict(zip(labels,tokens)); official_winners=[label for label,price in zip(labels,prices) if float(price)>=.999]
    official_winner=official_winners[0] if len(official_winners)==1 else None
    print("\nRESOLUTION AUDIT")
    print(f"resolved outcome recorded locally: {fmt(market.get('resolved_outcome'))}")
    print(f"official resolved outcome: {fmt(official_winner)}")
    print(f"resolution metadata/source: {fmt((official or event or {}).get('resolutionSource'))}")
    print(f"market question: {fmt((official or {}).get('question') or market.get('question'))}")
    print(f"resolution rule text: {fmt((official or event or {}).get('description'))}")
    print("reference start value: Official reference value unavailable")
    print("reference end value: Official reference value unavailable")
    if network_error: print(f"official metadata fetch error: {network_error}")
    if official and (official_map.get("Up")!=market.get("up_token_id") or official_map.get("Down")!=market.get("down_token_id")):
        failures.append(("HIGH","token mapping","Local IDs differ from official outcome arrays","mapping/storage error","collector/markets.py map_outcome_tokens","Correct mapping from explicit labels"))
    if official_winner and official_winner != market.get("resolved_outcome"):
        failures.append(("HIGH","resolution","Local winner differs from official outcomePrices","stale/incorrect resolution","collector/markets.py normalize_market","Refresh official resolution"))

    print("\nTOKEN MAPPING")
    token_sanity="INSUFFICIENT DATA"
    last_prices={}
    for outcome,token in (("Up",market.get("up_token_id")),("Down",market.get("down_token_id"))):
        rows=con.execute("""SELECT (best_bid+best_ask)/2 midpoint FROM polymarket_quote_points
          WHERE market_id=? AND token_id=? AND best_bid IS NOT NULL AND best_ask IS NOT NULL
          AND state_as_of BETWEEN ? AND ? ORDER BY state_as_of,receipt_timestamp,event_id""",[market_id,token,start,end]).fetchall()
        first,last=(rows[0][0],rows[-1][0]) if rows else (None,None); last_prices[outcome]=last
        print(f"{outcome}: token ID={token}; stored outcome label={outcome}; first observed price={fmt(first)}; last observed price={fmt(last)}")
    winner=market.get("resolved_outcome"); loser="Down" if winner=="Up" else "Up"
    if last_prices.get(winner) is not None and last_prices.get(loser) is not None:
        token_sanity="PASS" if last_prices[winner]>=.9 and last_prices[loser]<=.1 else "SUSPICIOUS"
    print(f"Token mapping sanity check: {token_sanity}")
    if token_sanity == "INSUFFICIENT DATA" and in_window_polymarket_events > 0:
        suspicious.append(("MEDIUM","Polymarket data completeness",
          "recorded data is insufficient to validate both Up and Down token price behavior",
          "cause unavailable from the recorded data","collector/audit_market.py audit",
          "Collect sufficient two-sided quotes before treating token mapping as fully validated"))
    elif token_sanity == "SUSPICIOUS":
        suspicious.append(("MEDIUM","token mapping sanity",
          "winning and losing token prices did not approach expected settlement values",
          "cause unavailable from the recorded data","collector/audit_market.py audit",
          "Inspect the recorded token quotes and official outcome mapping"))

    checkpoint_results={}
    print(f"\nCHECKPOINT AUDIT (MAX_CHECKPOINT_AGE_SECONDS={max_age:g})")
    for seconds in (60,30,20,10,5,3,2,1):
        target=end-pd.Timedelta(seconds=seconds)
        b=nearest(con,f"""SELECT exchange_timestamp,{price_expr} FROM bitcoin_events WHERE exchange_timestamp BETWEEN ? AND ?
          AND {price_expr} IS NOT NULL ORDER BY exchange_timestamp DESC LIMIT 1""",[start,target])
        values={}
        for outcome in ("Up","Down"):
            q=nearest(con,"""SELECT state_as_of,best_bid,best_ask
              FROM polymarket_quote_points WHERE market_id=? AND outcome=? AND state_as_of BETWEEN ? AND ?
              AND best_bid IS NOT NULL AND best_ask IS NOT NULL
              ORDER BY state_as_of DESC,last_changed_at DESC,receipt_timestamp DESC,event_id DESC LIMIT 1""",[market_id,outcome,start,target])
            age=(target-utc(q[0])).total_seconds() if q else None
            mid=(q[1]+q[2])/2 if q and age<=max_age else None
            values[outcome]=(q,age,mid)
        bage=(target-utc(b[0])).total_seconds() if b else None
        checkpoint_results[seconds]=(b,values)
        print(f"{seconds}s | checkpoint={fmt(target)} | BTC ts={fmt(b[0] if b else None)} price={fmt(b[1] if b else None)} age={fmt(bage)}s | "
          f"Up ts={fmt(values['Up'][0][0] if values['Up'][0] else None)} bid={fmt(values['Up'][0][1] if values['Up'][0] else None)} ask={fmt(values['Up'][0][2] if values['Up'][0] else None)} midpoint={fmt(values['Up'][2])} age={fmt(values['Up'][1])}s | "
          f"Down ts={fmt(values['Down'][0][0] if values['Down'][0] else None)} bid={fmt(values['Down'][0][1] if values['Down'][0] else None)} ask={fmt(values['Down'][0][2] if values['Down'][0] else None)} midpoint={fmt(values['Down'][2])} age={fmt(values['Down'][1])}s")

    exclusion_reasons = research_exclusion_reasons(
        market=market, start=start, end=end, btc=btc, checkpoint_results=checkpoint_results,
        max_checkpoint_age=max_age)
    research_eligible = not exclusion_reasons
    print(f"\nRESEARCH ELIGIBLE: {'YES' if research_eligible else 'NO'}")
    print("EXCLUSION REASONS:")
    if exclusion_reasons:
        for reason in exclusion_reasons:
            print(f"* {reason}")
    else:
        print("None")

    print("\nTIMESTAMP CONSISTENCY")
    print("market discovery: ISO 8601 strings -> DuckDB timezone-aware TIMESTAMPTZ")
    print("Binance aggTrade: Unix milliseconds T -> timezone-aware UTC; bookTicker: UTC receipt time")
    print("Polymarket: Unix milliseconds -> timezone-aware UTC; receipt time is UTC fallback only")
    print("dashboard: TIMESTAMPTZ comparisons; no naive datetime comparison detected")
    print("seconds-vs-milliseconds: PASS for sampled stored source timestamps")

    btc_series,poly_series=market_series(con,market_id)
    btc_bad=btc_series[(btc_series.event_time<start)|(btc_series.event_time>end)] if len(btc_series) else btc_series
    poly_bad=poly_series[(poly_series.event_time<start)|(poly_series.event_time>end)] if len(poly_series) else poly_series
    print("\nDATA WINDOW SANITY CHECK")
    print(f"BTC chart window: {'PASS' if btc_bad.empty else 'FAIL'}")
    print(f"Polymarket chart window: {'PASS' if poly_bad.empty else 'FAIL'}")
    if not poly_bad.empty:
        print(f"Polymarket offending rows={len(poly_bad)} earliest={fmt(poly_bad.event_time.min())} latest={fmt(poly_bad.event_time.max())}")
        failures.append(("HIGH","dashboard chart",f"{len(poly_bad)} Polymarket rows outside stored window","market_series lacks time predicate","dashboard/queries.py market_series","Filter Polymarket timestamps to market bounds"))

    print("\nDASHBOARD SUMMARY AUDIT")
    raw_fields={"Winner":official_winner or market.get("resolved_outcome"),"BTC open":open_row[1],"BTC close":close_row[1],"BTC change":change,
      "BTC high":btc.btc_value.max() if len(btc) else None,"BTC low":btc.btc_value.min() if len(btc) else None}
    dash_fields={"Winner":dashboard.winner,"BTC open":dashboard.btc_open,"BTC close":dashboard.btc_close,"BTC change":dashboard.btc_change,
      "BTC high":dashboard.btc_high,"BTC low":dashboard.btc_low}
    for sec in (30,1):
        for outcome in ("Up","Down"):
            name=f"{outcome} @ {sec}s"; dash_fields[name]=dashboard[f"{outcome.lower()}_{sec}s"]; raw_fields[name]=checkpoint_results[sec][1][outcome][2]
    print("Field | Dashboard | Raw recomputation | Match")
    for name in dash_fields:
        d,r=dash_fields[name],raw_fields[name]
        match=(pd.isna(d) and r is None) or str(d)==str(r) or (isinstance(r,(float,int)) and not pd.isna(d) and abs(float(d)-float(r))<1e-8)
        print(f"{name} | {fmt(d)} | {fmt(r)} | {'PASS' if match else 'FAIL'}")

    if change is not None and market.get("resolved_outcome") in ("Up","Down") and ((change<0 and market['resolved_outcome']=='Up') or (change>0 and market['resolved_outcome']=='Down')):
        suspicious.append(("MEDIUM","resolution comparison",f"Binance change {change:.4f} disagrees with official winner {market['resolved_outcome']}","Polymarket settles on Chainlink, not Binance","collector/audit_market.py","Do not infer settlement from Binance"))
    status=determine_verdict(failures=failures, suspicious=suspicious,
      official_available=official is not None, polymarket_event_count=in_window_polymarket_events,
      token_sanity=token_sanity, research_eligible=research_eligible)
    print(f"\nOVERALL STATUS: {status}")
    print("\nFINDINGS")
    findings=failures+suspicious
    if not findings: print("No issues found.")
    for severity,component,evidence,cause,source,fix in findings:
        print(f"severity: {severity}\narea: {component}\nevidence: {evidence}\nlikely cause: {cause}\nexact source file/function: {source}\nrecommended fix: {fix}\n")
    return 1 if status=="FAIL" else 0


def main() -> None:
    parser=argparse.ArgumentParser(description="Audit one completed Polymarket BTC Up/Down market without modifying data.")
    choice=parser.add_mutually_exclusive_group(required=True)
    choice.add_argument("--market-id"); choice.add_argument("--latest",action="store_true")
    parser.add_argument("--max-checkpoint-age",type=float,default=5.0)
    parser.add_argument("--db",help=argparse.SUPPRESS)
    args=parser.parse_args()
    raise SystemExit(audit(args.market_id,args.latest,args.max_checkpoint_age,args.db))


if __name__ == "__main__":
    main()
