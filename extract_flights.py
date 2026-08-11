"""EXTRACT (flights): pull arrivals into each market's airport from the
OpenSky Network API and land them as immutable dated CSV files.

Runs three ways with the same code:
  - Locally:        python3 extract_flights.py
  - Backfill:       python3 extract_flights.py --backfill 7
  - AWS Lambda:     handler = extract_flights.lambda_handler

Auth: OpenSky OAuth2 client credentials (create a free API client on your
OpenSky account page). Set OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET.
Anonymous access is attempted if unset, but heavily rate-limited.

Output mode: OUTPUT_MODE=s3 (needs S3_BUCKET) or local files under data/.
Design notes (the corners, handled):
  - OpenSky flight data lags: we pull a trailing window ending LAG_HOURS
    ago, and windows overlap between runs; staging deduplicates.
  - 404 from the arrivals endpoint means "no flights found" -> empty, not error.
  - 429 -> single retry after the polite wait, then give up this run
    (the next scheduled run covers the window; overlap + dedupe = no loss).
  - Callsigns are stripped of commas so raw CSVs stay clean.
"""

import csv
import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from config import MARKETS, FLIGHTS_COLUMNS

TOKEN_URL = ("https://auth.opensky-network.org/auth/realms/opensky-network"
             "/protocol/openid-connect/token")
ARRIVALS_URL = "https://opensky-network.org/api/flights/arrival"

LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "3"))
LAG_HOURS = int(os.environ.get("LAG_HOURS", "1"))
OUTPUT_MODE = os.environ.get("OUTPUT_MODE", "local")   # "local" or "s3"
S3_BUCKET = os.environ.get("S3_BUCKET", "")
DATA_DIR = os.environ.get("DATA_DIR", "data")


def get_token():
    cid = os.environ.get("OPENSKY_CLIENT_ID")
    secret = os.environ.get("OPENSKY_CLIENT_SECRET")
    if not cid or not secret:
        print("No OpenSky credentials set -> trying anonymous (low limits).")
        return None
    body = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": cid,
        "client_secret": secret,
    }).encode()
    req = urllib.request.Request(
        TOKEN_URL, data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())["access_token"]


def fetch_arrivals(icao, begin, end, token):
    """One call to the arrivals endpoint. Returns a list (possibly empty)."""
    qs = urllib.parse.urlencode({"airport": icao, "begin": begin, "end": end})
    req = urllib.request.Request(f"{ARRIVALS_URL}?{qs}")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    for attempt in (1, 2):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return []                       # no flights in window
            if e.code == 429 and attempt == 1:
                print(f"  429 rate-limited on {icao}; waiting 60s once...")
                time.sleep(60)
                continue
            if e.code == 401:
                raise SystemExit(
                    "401 Unauthorized from OpenSky. Create an API client on "
                    "your OpenSky account page and set OPENSKY_CLIENT_ID / "
                    "OPENSKY_CLIENT_SECRET.")
            raise
    return []


def rows_for(market, flights, extracted_at):
    tz = ZoneInfo(MARKETS[market]["tz"])
    rows = []
    for f in flights:
        last_seen = f.get("lastSeen")           # arrival time, unix seconds
        if not last_seen:
            continue
        ts_utc = datetime.fromtimestamp(last_seen, tz=timezone.utc)
        ts_loc = ts_utc.astimezone(tz)
        rows.append({
            "market": market,
            "icao24": f.get("icao24", ""),
            "callsign": (f.get("callsign") or "").strip().replace(",", ""),
            "departure_airport": f.get("estDepartureAirport") or "",
            "arrival_ts_utc": ts_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "arrival_date_local": ts_loc.strftime("%Y-%m-%d"),
            "arrival_hour_local": ts_loc.hour,
            "extracted_at_utc": extracted_at,
        })
    return rows


def write_rows(rows, kind, market, stamp):
    """Group rows by local date and write one headerless CSV per date."""
    by_date = {}
    for r in rows:
        by_date.setdefault(r["arrival_date_local"], []).append(r)
    for d, rs in by_date.items():
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=FLIGHTS_COLUMNS)
        for r in rs:
            w.writerow(r)
        key = f"raw/{kind}/dt={d}/{kind}_{market}_{stamp}.csv"
        payload = buf.getvalue()
        if OUTPUT_MODE == "s3":
            import boto3
            boto3.client("s3").put_object(
                Bucket=S3_BUCKET, Key=key, Body=payload.encode())
            print(f"  s3://{S3_BUCKET}/{key}  ({len(rs)} rows)")
        else:
            path = os.path.join(DATA_DIR, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as fh:
                fh.write(payload)
            print(f"  {path}  ({len(rs)} rows)")


def run(begin=None, end=None):
    now = datetime.now(timezone.utc)
    end = end or int((now - timedelta(hours=LAG_HOURS)).timestamp())
    begin = begin or int(
        (now - timedelta(hours=LAG_HOURS + LOOKBACK_HOURS)).timestamp())
    token = get_token()
    extracted_at = now.strftime("%Y-%m-%d %H:%M:%S")
    stamp = str(end)
    total = 0
    for market, cfg in MARKETS.items():
        flights = fetch_arrivals(cfg["icao"], begin, end, token)
        rows = rows_for(market, flights, extracted_at)
        print(f"{market}: {len(rows)} arrivals in window.")
        if rows:
            write_rows(rows, "flights", market, stamp)
        total += len(rows)
        time.sleep(2)                            # be polite between markets
    print(f"Done. {total} arrival rows landed.")


def backfill(days):
    """Pull each of the previous N days as its own 1-day window."""
    for i in range(days, 0, -1):
        day = datetime.now(timezone.utc).date() - timedelta(days=i)
        b = int(datetime(day.year, day.month, day.day,
                         tzinfo=timezone.utc).timestamp())
        e = b + 86400
        print(f"--- Backfilling {day} ---")
        run(begin=b, end=e)
        time.sleep(5)


def lambda_handler(event, context):
    run()
    return {"ok": True}


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--backfill":
        backfill(int(sys.argv[2]))
    else:
        run()
