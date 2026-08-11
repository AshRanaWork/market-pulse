# Market Pulse

A multi-market hospitality demand signal pipeline. Live flight arrivals
(LAS + LAX), weather, and calendar signals -> a daily demand-pressure
rating (NORMAL / ELEVATED / HIGH) with automatic email alerts.

**Stack:** AWS (S3, Lambda, EventBridge, Glue, Athena, SNS, CloudWatch,
IAM) - dbt - DuckDB (local dev) - GitHub Actions - Python.

## Architecture

```
 OpenSky API ──▶ Lambda (hourly) ──┐
 Open-Meteo  ──▶ Lambda (daily)  ──┼──▶ S3 lake (bronze, dt= partitions)
                                   │        │ Glue catalog (projection)
 EventBridge schedules ────────────┘        ▼
                              Athena ◀── dbt (GitHub Actions daily):
                                          staging (silver) + marts (gold)
                                          + data quality tests
                                               │
                                          SNS alert on HIGH
```

Bronze = immutable raw files. Silver = typed, deduplicated staging views.
Gold = `mart_daily_demand_signals` and `mart_hourly_arrival_curve`.
Adding a market = one entry in `config.py`.

## The signal

`demand_pressure` is a transparent rules score: arrival momentum vs the
trailing 7-day average, weekend/holiday flags, curated major events, and
weather extremes. Explainable by design until occupancy ground truth
exists to validate a model against.

## Honest limitations

- A leading-indicator signal, not a validated occupancy forecast.
- OpenSky coverage has gaps and lag; overlapping pulls + dedup handle it,
  and warmup days are labeled WARMUP rather than guessed.
- The events file is manually curated (and stated as such).

## Roadmap

- At-scale mirror of the transforms in PySpark + Delta Lake (Databricks).
- Same config-driven pattern extended to entertainment release tracking
  (box office momentum), my other domain.
