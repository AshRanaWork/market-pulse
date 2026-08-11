"""EXTRACT (weather): pull hourly weather for each market from Open-Meteo
(free, no key) and land it as immutable dated CSVs. Same three run modes
as the flights extractor. past_days=2 creates overlap on purpose; staging
dedupes by keeping the freshest extraction per market-hour.
"""

import csv
import io
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from config import MARKETS, WEATHER_COLUMNS

API = "https://api.open-meteo.com/v1/forecast"
OUTPUT_MODE = os.environ.get("OUTPUT_MODE", "local")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
DATA_DIR = os.environ.get("DATA_DIR", "data")


def fetch(market, cfg):
    qs = urllib.parse.urlencode({
        "latitude": cfg["lat"], "longitude": cfg["lon"],
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "temperature_unit": "fahrenheit",
        "timezone": cfg["tz"],
        "past_days": 2, "forecast_days": 1,
    })
    with urllib.request.urlopen(f"{API}?{qs}", timeout=60) as r:
        return json.loads(r.read())["hourly"]


def run():
    now = datetime.now(timezone.utc)
    extracted_at = now.strftime("%Y-%m-%d %H:%M:%S")
    stamp = str(int(now.timestamp()))
    for market, cfg in MARKETS.items():
        h = fetch(market, cfg)
        by_date = {}
        for i, ts in enumerate(h["time"]):          # "2026-07-17T14:00"
            date_local, hour = ts[:10], int(ts[11:13])
            by_date.setdefault(date_local, []).append({
                "market": market,
                "observed_at_local": ts.replace("T", " ") + ":00",
                "observed_date_local": date_local,
                "hour_local": hour,
                "temp_f": h["temperature_2m"][i],
                "precip_mm": h["precipitation"][i],
                "wind_kmh": h["wind_speed_10m"][i],
                "extracted_at_utc": extracted_at,
            })
        for d, rows in by_date.items():
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=WEATHER_COLUMNS)
            for r in rows:
                w.writerow(r)
            key = f"raw/weather/dt={d}/weather_{market}_{stamp}.csv"
            if OUTPUT_MODE == "s3":
                import boto3
                boto3.client("s3").put_object(
                    Bucket=S3_BUCKET, Key=key, Body=buf.getvalue().encode())
                print(f"  s3://{S3_BUCKET}/{key} ({len(rows)} rows)")
            else:
                path = os.path.join(DATA_DIR, key)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as fh:
                    fh.write(buf.getvalue())
                print(f"  {path} ({len(rows)} rows)")
    print("Weather extraction done.")


def lambda_handler(event, context):
    run()
    return {"ok": True}


if __name__ == "__main__":
    run()
