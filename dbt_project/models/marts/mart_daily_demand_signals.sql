-- GOLD: one row per market per day. The demand-pressure signal an
-- operations manager would act on. Rules-based v1 by design: every
-- point in the score is explainable, which beats a black box until
-- there is occupancy ground truth to validate against.

with daily_arrivals as (
    select market, arrival_date_local, count(*) as arrivals
    from {{ ref('stg_flights') }}
    group by market, arrival_date_local
),

daily_weather as (
    select
        market,
        observed_date_local,
        max(temp_f)                     as temp_max_f,
        min(temp_f)                     as temp_min_f,
        round(sum(precip_mm), 1)        as precip_total_mm,
        round(max(wind_kmh), 1)         as wind_max_kmh
    from {{ ref('stg_weather') }}
    group by market, observed_date_local
),

with_trailing as (
    select
        a.market,
        a.arrival_date_local,
        a.arrivals,
        avg(a.arrivals) over (
            partition by a.market
            order by a.arrival_date_local
            rows between 7 preceding and 1 preceding
        ) as trailing_7day_avg,
        count(a.arrivals) over (
            partition by a.market
            order by a.arrival_date_local
            rows between 7 preceding and 1 preceding
        ) as days_in_window
    from daily_arrivals a
),

joined as (
    select
        t.market,
        t.arrival_date_local,
        t.arrivals,
        t.trailing_7day_avg,
        t.days_in_window,
        case when t.trailing_7day_avg > 0
             then round((t.arrivals - t.trailing_7day_avg)
                        / t.trailing_7day_avg * 100, 1)
        end                                        as arrivals_vs_7day_avg_pct,
        w.temp_max_f, w.temp_min_f,
        w.precip_total_mm, w.wind_max_kmh,
        case when {{ is_weekend('t.arrival_date_local') }}
             then 1 else 0 end                     as is_weekend,
        case when h.holiday_date is not null
             then 1 else 0 end                     as is_holiday,
        case when e.event_date is not null
             then 1 else 0 end                     as is_major_event,
        e.event_name,
        case when w.temp_max_f >= 108 then 1 else 0 end as is_extreme_heat,
        case when w.temp_max_f between 85 and 104
              and coalesce(w.precip_total_mm, 0) < 0.5
             then 1 else 0 end                     as is_pool_weather,
        case when coalesce(w.precip_total_mm, 0) >= 2
             then 1 else 0 end                     as is_rain
    from with_trailing t
    left join daily_weather w
        on t.market = w.market
       and t.arrival_date_local = w.observed_date_local
    left join {{ ref('events') }} e
        on t.market = e.market
       and t.arrival_date_local = cast(e.event_date as date)
    left join {{ ref('holidays') }} h
        on t.arrival_date_local = cast(h.holiday_date as date)
),

scored as (
    select *,
        (case when arrivals_vs_7day_avg_pct >= 20 then 2
              when arrivals_vs_7day_avg_pct >= 10 then 1
              else 0 end)
        + is_weekend
        + is_holiday
        + (is_major_event * 2)
        + (case when is_extreme_heat = 1 or is_rain = 1
                then 1 else 0 end)                 as pressure_score
    from joined
)

select
    market || '|' || cast(arrival_date_local as varchar) as daily_key,
    market,
    arrival_date_local,
    arrivals,
    round(trailing_7day_avg, 1)     as trailing_7day_avg,
    arrivals_vs_7day_avg_pct,
    temp_max_f, temp_min_f, precip_total_mm, wind_max_kmh,
    is_weekend, is_holiday, is_major_event, event_name,
    is_extreme_heat, is_pool_weather, is_rain,
    pressure_score,
    case
        when days_in_window < 7 then 'WARMUP'
        when pressure_score >= 4       then 'HIGH'
        when pressure_score >= 2       then 'ELEVATED'
        else 'NORMAL'
    end as demand_pressure
from scored
