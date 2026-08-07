from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import duckdb

from collector.audit_market import as_array, fetch_official, official_market
from collector.markets import DEFAULT_DB, parse_utc, utcnow


@dataclass
class BackfillResult:
    market_id: str
    slug: str
    action: str
    outcome: str | None
    detail: str


def candidate_markets(con: duckdb.DuckDBPyConnection, *, since: datetime | None = None,
                      market_id: str | None = None,
                      now: datetime | None = None) -> list[dict[str, Any]]:
    clauses = ["end_time < ?", "(status != 'completed' OR resolved_outcome IS NULL)"]
    params: list[Any] = [now or datetime.now(timezone.utc)]
    if since:
        clauses.append("start_time > ?")
        params.append(since)
    if market_id:
        clauses.append("market_id = ?")
        params.append(market_id)
    frame = con.execute(
        "SELECT * FROM markets WHERE " + " AND ".join(clauses) + " ORDER BY start_time,market_id",
        params).df()
    return [row.to_dict() for _, row in frame.iterrows()]


def official_resolution(market: dict[str, Any],
                        fetcher: Callable[[str], tuple[dict[str, Any] | None, str | None]] = fetch_official
                        ) -> tuple[str | None, str]:
    slug = str(market.get("slug") or "")
    if not slug:
        return None, "stored market has no slug"
    event, error = fetcher(slug)
    if error:
        return None, f"official metadata request failed: {error}"
    if not event:
        return None, "official event metadata unavailable"
    stored_event_id = str(market.get("event_id") or "")
    official_event_id = str(event.get("id") or "")
    if stored_event_id and official_event_id and stored_event_id != official_event_id:
        return None, "official event ID does not match stored event ID"
    official = official_market(event, str(market["market_id"]))
    if not official:
        return None, "exact stored market ID absent from official event metadata"
    official_slug = str(official.get("slug") or "")
    if official_slug and official_slug != slug:
        return None, "official market slug does not match stored slug"

    labels = as_array(official.get("outcomes"))
    tokens = as_array(official.get("clobTokenIds"))
    if len(labels) != len(tokens):
        return None, "official outcome/token mapping is incomplete"
    token_map = {label.strip().lower(): token for label, token in zip(labels, tokens)}
    if (token_map.get("up") != str(market.get("up_token_id"))
            or token_map.get("down") != str(market.get("down_token_id"))):
        return None, "official Up/Down token mapping does not match stored mapping"
    if not bool(official.get("closed")):
        return None, "official market is not closed"
    prices = as_array(official.get("outcomePrices"))
    if len(prices) != len(labels):
        return None, "official resolution prices are unavailable"
    try:
        winners = [label.strip().title() for label, price in zip(labels, prices)
                   if float(price) >= .999]
    except (TypeError, ValueError):
        return None, "official resolution prices are invalid"
    if len(winners) != 1 or winners[0] not in ("Up", "Down"):
        return None, "official Up/Down outcome is unresolved or ambiguous"
    return winners[0], "official closed market has an unambiguous outcome"


def backfill(con: duckdb.DuckDBPyConnection, *, since: datetime | None = None,
             market_id: str | None = None, dry_run: bool = False,
             now: datetime | None = None,
             fetcher: Callable[[str], tuple[dict[str, Any] | None, str | None]] = fetch_official,
             clock: Callable[[], datetime] = utcnow) -> list[BackfillResult]:
    results = []
    for market in candidate_markets(con, since=since, market_id=market_id, now=now):
        outcome, detail = official_resolution(market, fetcher)
        if outcome is None:
            results.append(BackfillResult(str(market["market_id"]), str(market.get("slug") or ""),
                                          "unchanged", None, detail))
            continue
        if not dry_run:
            con.execute("""UPDATE markets SET status='completed',resolved_outcome=?,updated_at=?
                           WHERE market_id=? AND end_time<?
                           AND (status!='completed' OR resolved_outcome IS NULL)""",
                        [outcome, clock(), market["market_id"], now or datetime.now(timezone.utc)])
        results.append(BackfillResult(str(market["market_id"]), str(market.get("slug") or ""),
                                      "would update" if dry_run else "updated", outcome, detail))
    return results


def print_results(results: list[BackfillResult], *, dry_run: bool) -> None:
    print("RESOLUTION BACKFILL")
    print(f"Mode: {'DRY RUN' if dry_run else 'UPDATE'}")
    print(f"Candidates: {len(results)}")
    available = sum(result.outcome in ("Up", "Down") for result in results)
    changed = sum(result.action in ("updated", "would update") for result in results)
    print(f"Official outcomes available: {available}")
    print(f"{'Would update' if dry_run else 'Updated'}: {changed}")
    print(f"Unchanged: {len(results)-changed}")
    for result in results:
        print(f"{result.market_id} | {result.slug} | {result.action} | "
              f"{result.outcome or 'NULL'} | {result.detail}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill official Gamma resolutions for ended incomplete markets.")
    parser.add_argument("--since", help="Exclusive market-start boundary in UTC")
    parser.add_argument("--market-id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help=argparse.SUPPRESS)
    args = parser.parse_args()
    since = parse_utc(args.since) if args.since else None
    con = duckdb.connect(str(args.db), read_only=args.dry_run)
    try:
        results = backfill(con, since=since, market_id=args.market_id, dry_run=args.dry_run)
    finally:
        con.close()
    print_results(results, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
