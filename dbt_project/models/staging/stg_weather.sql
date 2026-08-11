-- SILVER: typed, deduplicated hourly weather (overlapping pulls are
-- expected; freshest extraction wins per market-hour).

with src as (
    select * from {{ source('raw', 'raw_weather') }}
),

typed as (
    select
        market,
        cast(observed_at_local as timestamp)  as observed_at_local,
        cast(observed_date_local as date)     as observed_date_local,
        cast(hour_local as integer)           as hour_local,
        cast(temp_f as double)                as temp_f,
        cast(precip_mm as double)             as precip_mm,
        cast(wind_kmh as double)              as wind_kmh,
        cast(extracted_at_utc as timestamp)   as extracted_at_utc
    from src
),

deduped as (
    select *,
        row_number() over (
            partition by market, observed_at_local
            order by extracted_at_utc desc
        ) as rn
    from typed
)

select market, observed_at_local, observed_date_local,
       hour_local, temp_f, precip_mm, wind_kmh
from deduped
where rn = 1
