# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: typed, deduplicated Delta tables
# MAGIC
# MAGIC Mirrors dbt's `stg_flights` / `stg_weather` exactly. Extraction windows
# MAGIC overlap on purpose (to survive OpenSky lag), so the same flight can land
# MAGIC more than once; the freshest extraction wins per natural key.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "market_pulse"


def silver_flights(bronze):
    typed = bronze.select(
        "market",
        "icao24",
        F.nullif(F.trim("callsign"), F.lit("")).alias("callsign"),
        F.nullif("departure_airport", F.lit("")).alias("departure_airport"),
        F.col("arrival_ts_utc").cast("timestamp").alias("arrival_ts_utc"),
        F.col("arrival_date_local").cast("date").alias("arrival_date_local"),
        F.col("arrival_hour_local").cast("int").alias("arrival_hour_local"),
        F.col("extracted_at_utc").cast("timestamp").alias("extracted_at_utc"),
    )
    w = Window.partitionBy("market", "icao24", "arrival_ts_utc") \
              .orderBy(F.col("extracted_at_utc").desc())
    return (typed.withColumn("rn", F.row_number().over(w))
            .filter("rn = 1")
            .drop("rn", "extracted_at_utc"))


def silver_weather(bronze):
    typed = bronze.select(
        "market",
        F.col("observed_at_local").cast("timestamp").alias("observed_at_local"),
        F.col("observed_date_local").cast("date").alias("observed_date_local"),
        F.col("hour_local").cast("int").alias("hour_local"),
        F.col("temp_f").cast("double").alias("temp_f"),
        F.col("precip_mm").cast("double").alias("precip_mm"),
        F.col("wind_kmh").cast("double").alias("wind_kmh"),
        F.col("extracted_at_utc").cast("timestamp").alias("extracted_at_utc"),
    )
    w = Window.partitionBy("market", "observed_at_local") \
              .orderBy(F.col("extracted_at_utc").desc())
    return (typed.withColumn("rn", F.row_number().over(w))
            .filter("rn = 1")
            .drop("rn", "extracted_at_utc"))


if __name__ == "__main__":
    spark.sql(f"USE {CATALOG}.{SCHEMA}")

    sf = silver_flights(spark.table("bronze_flights"))
    swx = silver_weather(spark.table("bronze_weather"))

    sf.write.format("delta").mode("overwrite").saveAsTable("silver_flights")
    swx.write.format("delta").mode("overwrite").saveAsTable("silver_weather")

    b, s = spark.table("bronze_flights").count(), sf.count()
    print(f"flights: {b} bronze -> {s} silver "
          f"({b - s} duplicate extractions removed)")
    b, s = spark.table("bronze_weather").count(), swx.count()
    print(f"weather: {b} bronze -> {s} silver "
          f"({b - s} duplicate extractions removed)")
