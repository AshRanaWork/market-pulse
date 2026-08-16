# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: land raw CSVs as Delta tables
# MAGIC
# MAGIC Reads the uploaded `databricks_upload.zip` from the Volume, unpacks it,
# MAGIC and writes `bronze_flights` / `bronze_weather` Delta tables with the
# MAGIC `dt` partition value recovered from each file's path. Bronze is a
# MAGIC faithful copy of raw: no dedupe, no typing beyond strings.

# COMMAND ----------

import shutil
import zipfile
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG = "workspace"          # Free Edition default catalog
SCHEMA = "market_pulse"
VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw_upload"
# Extract INTO the volume: on serverless compute, executors cannot read
# the driver's /tmp, but every node can read Unity Catalog volume paths.
WORK = f"{VOLUME}/extracted"

# Raw CSVs are headerless by design; these ARE the schema (see config.py).
FLIGHTS_SCHEMA = StructType([
    StructField(c, StringType()) for c in [
        "market", "icao24", "callsign", "departure_airport",
        "arrival_ts_utc", "arrival_date_local", "arrival_hour_local",
        "extracted_at_utc"]
])
WEATHER_SCHEMA = StructType([
    StructField(c, StringType()) for c in [
        "market", "observed_at_local", "observed_date_local", "hour_local",
        "temp_f", "precip_mm", "wind_kmh", "extracted_at_utc"]
])


def unpack(volume_dir: str, work_dir: str) -> Path:
    work = Path(work_dir)
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    zips = sorted(Path(volume_dir).glob("*.zip"))
    assert zips, f"No zip found in {volume_dir}; upload databricks_upload.zip"
    with zipfile.ZipFile(zips[-1]) as zf:
        zf.extractall(work)
    n = len(list(work.rglob("*.csv")))
    print(f"Unpacked {zips[-1].name}: {n} csv files -> {work}")
    return work


def read_raw(spark, path_glob: str, schema: StructType):
    """Read headerless CSVs and recover the dt partition from the path."""
    df = (spark.read.schema(schema).option("header", "false")
          .csv(path_glob)
          .withColumn("dt", F.regexp_extract(
              F.input_file_name(), r"dt=(\d{4}-\d{2}-\d{2})", 1)))
    return df


if __name__ == "__main__":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"USE {CATALOG}.{SCHEMA}")

    work = unpack(VOLUME, WORK)

    bronze_flights = read_raw(
        spark, f"{work}/raw/flights/dt=*/*.csv", FLIGHTS_SCHEMA)
    bronze_weather = read_raw(
        spark, f"{work}/raw/weather/dt=*/*.csv", WEATHER_SCHEMA)

    (bronze_flights.write.format("delta").mode("overwrite")
     .partitionBy("dt").saveAsTable("bronze_flights"))
    (bronze_weather.write.format("delta").mode("overwrite")
     .partitionBy("dt").saveAsTable("bronze_weather"))

    # Seeds land as plain Delta tables (they have headers).
    for seed in ("events", "holidays"):
        df = (spark.read.option("header", "true")
              .csv(f"{work}/seeds/{seed}.csv"))
        df.write.format("delta").mode("overwrite").saveAsTable(seed)

    print("bronze_flights:", spark.table("bronze_flights").count())
    print("bronze_weather:", spark.table("bronze_weather").count())
    print("events:", spark.table("events").count(),
          "| holidays:", spark.table("holidays").count())
