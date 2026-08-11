-- SILVER: typed, deduplicated arrivals.
-- Extraction windows overlap on purpose (to survive OpenSky lag),
-- so the same flight can be landed more than once. We keep the
-- freshest extraction per (market, aircraft, arrival time).

with src as (
    select * from {{ source('raw', 'raw_flights') }}
),

typed as (
    select
        market,
        icao24,
        nullif(trim(callsign), '')                as callsign,
        nullif(departure_airport, '')             as departure_airport,
        cast(arrival_ts_utc as timestamp)         as arrival_ts_utc,
        cast(arrival_date_local as date)          as arrival_date_local,
        cast(arrival_hour_local as integer)       as arrival_hour_local,
        cast(extracted_at_utc as timestamp)       as extracted_at_utc
    from src
),

deduped as (
    select *,
        row_number() over (
            partition by market, icao24, arrival_ts_utc
            order by extracted_at_utc desc
        ) as rn
    from typed
)

select market, icao24, callsign, departure_airport,
       arrival_ts_utc, arrival_date_local, arrival_hour_local
from deduped
where rn = 1
