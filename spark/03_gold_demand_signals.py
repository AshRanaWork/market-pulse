# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: daily demand signals
# MAGIC
# MAGIC Port of dbt's `mart_daily_demand_signals`, including `score_drivers`
# MAGIC (which rules fired, with points) and `interpretation` (what the signal
# MAGIC means; deliberately a leading indicator, never a pricing recommendation).
# MAGIC Rules-based v1 by design: every point is explainable.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "market_pulse"

INTERPRETATIONS = {
    "WARMUP":
        "Baseline still building. The signal needs a full 7-day trailing "
        "window before it means anything. No action.",
    "HIGH":
        "Inbound volume and conditions are well above trend for this market. "
        "This pattern typically precedes elevated same-day and next-day "
        "demand. Worth reviewing rate and remaining inventory before the "
        "evening arrival peak, alongside your own booking pace.",
    "ELEVATED":
        "Running above baseline, but not decisively. Treat as a watch item: "
        "check whether your own booking pace is tracking the same direction "
        "before changing anything.",
    "NORMAL":
        "Inbound volume is in line with the trailing week. Nothing here "
        "argues for a change to rate or staffing.",
}


def gold_demand_signals(silver_flights, silver_weather, events, holidays):
    daily_arrivals = (silver_flights
        .groupBy("market", "arrival_date_local")
        .agg(F.count("*").alias("arrivals")))

    daily_weather = (silver_weather
        .groupBy("market", "observed_date_local")
        .agg(F.max("temp_f").alias("temp_max_f"),
             F.min("temp_f").alias("temp_min_f"),
             F.round(F.sum("precip_mm"), 1).alias("precip_total_mm"),
             F.round(F.max("wind_kmh"), 1).alias("wind_max_kmh")))

    trail = (Window.partitionBy("market").orderBy("arrival_date_local")
             .rowsBetween(-7, -1))
    with_trailing = (daily_arrivals
        .withColumn("trailing_7day_avg", F.avg("arrivals").over(trail))
        .withColumn("days_in_window", F.count("arrivals").over(trail)))

    ev = events.select(
        "market",
        F.col("event_date").cast("date").alias("event_date"),
        "event_name")
    hol = holidays.select(
        F.col("holiday_date").cast("date").alias("holiday_date"))

    j = (with_trailing.alias("t")
        .join(daily_weather.alias("w"),
              (F.col("t.market") == F.col("w.market")) &
              (F.col("t.arrival_date_local") == F.col("w.observed_date_local")),
              "left")
        .join(ev.alias("e"),
              (F.col("t.market") == F.col("e.market")) &
              (F.col("t.arrival_date_local") == F.col("e.event_date")),
              "left")
        .join(hol.alias("h"),
              F.col("t.arrival_date_local") == F.col("h.holiday_date"),
              "left")
        .select("t.market", "t.arrival_date_local", "t.arrivals",
                "t.trailing_7day_avg", "t.days_in_window",
                "w.temp_max_f", "w.temp_min_f",
                "w.precip_total_mm", "w.wind_max_kmh",
                "e.event_name",
                F.col("e.event_date").isNotNull().cast("int")
                    .alias("is_major_event"),
                F.col("h.holiday_date").isNotNull().cast("int")
                    .alias("is_holiday")))

    j = (j
        .withColumn("arrivals_vs_7day_avg_pct",
            F.when(F.col("trailing_7day_avg") > 0,
                   F.round((F.col("arrivals") - F.col("trailing_7day_avg"))
                           / F.col("trailing_7day_avg") * 100, 1)))
        # Spark dayofweek(): 1 = Sunday ... 7 = Saturday
        .withColumn("is_weekend",
            F.dayofweek("arrival_date_local").isin(1, 7).cast("int"))
        .withColumn("is_extreme_heat",
            (F.col("temp_max_f") >= 108).cast("int"))
        .withColumn("is_pool_weather",
            (F.col("temp_max_f").between(85, 104) &
             (F.coalesce("precip_total_mm", F.lit(0.0)) < 0.5)).cast("int"))
        .withColumn("is_rain",
            (F.coalesce("precip_total_mm", F.lit(0.0)) >= 2).cast("int"))
        .fillna(0, ["is_extreme_heat", "is_pool_weather", "is_rain"]))

    arrivals_pts = (F.when(F.col("arrivals_vs_7day_avg_pct") >= 20, 2)
                     .when(F.col("arrivals_vs_7day_avg_pct") >= 10, 1)
                     .otherwise(0))
    weather_pts = F.when((F.col("is_extreme_heat") == 1) |
                         (F.col("is_rain") == 1), 1).otherwise(0)

    scored = j.withColumn(
        "pressure_score",
        arrivals_pts + F.col("is_weekend") + F.col("is_holiday")
        + F.col("is_major_event") * 2 + weather_pts)

    labeled = scored.withColumn(
        "demand_pressure",
        F.when(F.col("days_in_window") < 7, "WARMUP")
         .when(F.col("pressure_score") >= 4, "HIGH")
         .when(F.col("pressure_score") >= 2, "ELEVATED")
         .otherwise("NORMAL"))

    pct_str = F.col("arrivals_vs_7day_avg_pct").cast("string")
    drivers = F.concat(
        F.when(F.col("arrivals_vs_7day_avg_pct") >= 20,
               F.concat(F.lit("arrivals +"), pct_str,
                        F.lit("% vs 7-day (+2); ")))
         .when(F.col("arrivals_vs_7day_avg_pct") >= 10,
               F.concat(F.lit("arrivals +"), pct_str,
                        F.lit("% vs 7-day (+1); ")))
         .otherwise(F.lit("")),
        F.when(F.col("is_weekend") == 1, "weekend (+1); ").otherwise(""),
        F.when(F.col("is_holiday") == 1, "holiday (+1); ").otherwise(""),
        F.when(F.col("is_major_event") == 1,
               F.concat(F.lit("event: "),
                        F.coalesce("event_name", F.lit("unnamed")),
                        F.lit(" (+2); ")))
         .otherwise(F.lit("")),
        F.when(F.col("is_extreme_heat") == 1, "extreme heat (+1); ")
         .when(F.col("is_rain") == 1, "rain (+1); ")
         .otherwise(""),
    )
    labeled = labeled.withColumn(
        "score_drivers",
        F.when(F.length(drivers) == 0, "no rules fired")
         .otherwise(F.regexp_replace(drivers, "; $", "")))

    interp = F.create_map(
        *[x for k, v in INTERPRETATIONS.items() for x in (F.lit(k), F.lit(v))])
    labeled = labeled.withColumn(
        "interpretation", interp[F.col("demand_pressure")])

    return labeled.select(
        F.concat_ws("|", "market",
                    F.col("arrival_date_local").cast("string"))
            .alias("daily_key"),
        "market", "arrival_date_local", "arrivals",
        F.round("trailing_7day_avg", 1).alias("trailing_7day_avg"),
        "arrivals_vs_7day_avg_pct",
        "temp_max_f", "temp_min_f", "precip_total_mm", "wind_max_kmh",
        "is_weekend", "is_holiday", "is_major_event", "event_name",
        "is_extreme_heat", "is_pool_weather", "is_rain",
        "pressure_score", "demand_pressure", "score_drivers",
        "interpretation")


if __name__ == "__main__":
    spark.sql(f"USE {CATALOG}.{SCHEMA}")
    gold = gold_demand_signals(
        spark.table("silver_flights"), spark.table("silver_weather"),
        spark.table("events"), spark.table("holidays"))
    (gold.write.format("delta").mode("overwrite")
     .saveAsTable("gold_daily_demand_signals"))
    (spark.table("gold_daily_demand_signals")
     .orderBy(F.desc("arrival_date_local"), "market")
     .select("market", "arrival_date_local", "arrivals",
             "pressure_score", "demand_pressure", "score_drivers")
     .show(10, truncate=60))
