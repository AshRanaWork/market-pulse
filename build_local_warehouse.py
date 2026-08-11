"""LOAD (Phase A, local): rebuild a DuckDB warehouse from ALL landed raw
files. Full rebuild = idempotent by construction. Columns are loaded as
VARCHAR to mirror the Athena raw tables exactly; dbt staging does the
typing, so the SAME dbt models run locally and in the cloud."""

import duckdb

from config import FLIGHTS_COLUMNS, WEATHER_COLUMNS

con = duckdb.connect("warehouse.duckdb")

def load(table, path_glob, columns):
    cols = ", ".join(f"'{c}': 'VARCHAR'" for c in columns)
    con.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_csv('{path_glob}',
                               header=false, columns={{{cols}}})
    """)
    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{table}: {n:,} rows")

load("raw_flights", "data/raw/flights/*/*.csv", FLIGHTS_COLUMNS)
load("raw_weather", "data/raw/weather/*/*.csv", WEATHER_COLUMNS)
con.close()
print("Local warehouse rebuilt: warehouse.duckdb")
