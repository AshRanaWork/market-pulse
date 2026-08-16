# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Validation: hard assertions on the Delta tables
# MAGIC
# MAGIC The Spark port must behave exactly like the dbt/Athena pipeline.
# MAGIC Every check here raises on failure; a clean run prints ALL CHECKS PASSED.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "market_pulse"


def run_checks(spark):
    spark.sql(f"USE {CATALOG}.{SCHEMA}")
    sf = spark.table("silver_flights")
    swx = spark.table("silver_weather")
    gold = spark.table("gold_daily_demand_signals")
    checks = []

    def check(name, ok, detail=""):
        checks.append((name, ok, detail))
        print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail else ""))

    # 1. Silver dedupe: natural keys are unique.
    dupes = (sf.groupBy("market", "icao24", "arrival_ts_utc")
             .count().filter("count > 1").count())
    check("silver_flights natural key unique", dupes == 0, f"{dupes} dupes")

    dupes = (swx.groupBy("market", "observed_at_local")
             .count().filter("count > 1").count())
    check("silver_weather natural key unique", dupes == 0, f"{dupes} dupes")

    # 2. Gold grain: one row per market-day.
    dupes = gold.groupBy("daily_key").count().filter("count > 1").count()
    check("gold daily_key unique", dupes == 0, f"{dupes} dupes")

    # 3. Labels are from the accepted set, none null.
    bad = gold.filter(
        ~F.col("demand_pressure").isin("NORMAL", "ELEVATED", "HIGH", "WARMUP")
        | F.col("demand_pressure").isNull()).count()
    check("demand_pressure accepted values", bad == 0, f"{bad} bad")

    # 4. WARMUP is exactly the first 7 days per market, live from day 8.
    ranked = gold.withColumn(
        "day_rank", F.row_number().over(
            __import__("pyspark").sql.window.Window
            .partitionBy("market").orderBy("arrival_date_local")))
    early_not_warmup = ranked.filter(
        (F.col("day_rank") <= 7) &
        (F.col("demand_pressure") != "WARMUP")).count()
    late_warmup = ranked.filter(
        (F.col("day_rank") > 7) &
        (F.col("demand_pressure") == "WARMUP")).count()
    check("warmup covers exactly first 7 days",
          early_not_warmup == 0 and late_warmup == 0,
          f"{early_not_warmup} early, {late_warmup} late")

    # 5. Score reconciles with drivers: sum of (+n) in the string == score.
    pts = F.expr(r"""
        aggregate(
          transform(
            regexp_extract_all(score_drivers, '\\(\\+(\\d)\\)', 1),
            x -> cast(x as int)),
          0, (a, b) -> a + b)
    """)
    mism = (gold.withColumn("driver_pts", pts)
            .filter(F.col("driver_pts") != F.col("pressure_score")).count())
    check("score_drivers reconciles to pressure_score", mism == 0,
          f"{mism} mismatched rows")

    # 6. Interpretation present on every row.
    nulls = gold.filter(F.col("interpretation").isNull()).count()
    check("interpretation never null", nulls == 0, f"{nulls} null")

    failed = [c for c in checks if not c[1]]
    assert not failed, f"{len(failed)} checks failed: {[c[0] for c in failed]}"
    print(f"\nALL {len(checks)} CHECKS PASSED")


if __name__ == "__main__":
    run_checks(spark)
