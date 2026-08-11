-- Run these in the Athena query editor (replace YOUR_BUCKET, 3 places).
-- Partition projection means Athena discovers dt=YYYY-MM-DD folders
-- automatically: no MSCK REPAIR, no Glue crawlers, no daily upkeep.

CREATE DATABASE IF NOT EXISTS market_pulse;

CREATE EXTERNAL TABLE IF NOT EXISTS market_pulse.raw_flights (
    market             string,
    icao24             string,
    callsign           string,
    departure_airport  string,
    arrival_ts_utc     string,
    arrival_date_local string,
    arrival_hour_local string,
    extracted_at_utc   string
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://YOUR_BUCKET/raw/flights/'
TBLPROPERTIES (
    'projection.enabled'      = 'true',
    'projection.dt.type'      = 'date',
    'projection.dt.range'     = '2026-01-01,NOW',
    'projection.dt.format'    = 'yyyy-MM-dd',
    'storage.location.template' = 's3://YOUR_BUCKET/raw/flights/dt=${dt}/'
);

CREATE EXTERNAL TABLE IF NOT EXISTS market_pulse.raw_weather (
    market              string,
    observed_at_local   string,
    observed_date_local string,
    hour_local          string,
    temp_f              string,
    precip_mm           string,
    wind_kmh            string,
    extracted_at_utc    string
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://YOUR_BUCKET/raw/weather/'
TBLPROPERTIES (
    'projection.enabled'      = 'true',
    'projection.dt.type'      = 'date',
    'projection.dt.range'     = '2026-01-01,NOW',
    'projection.dt.format'    = 'yyyy-MM-dd',
    'storage.location.template' = 's3://YOUR_BUCKET/raw/weather/dt=${dt}/'
);
