-- GOLD: one row per market per day. The demand-pressure signal an
-- operations manager would act on. Rules-based v1 by design: every
-- point in the score is explainable, which beats a black box until
-- there is occupancy ground truth to validate against.
--
-- score_drivers   spells out exactly which rules fired and for how many
--                 points, so the number is auditable rather than opaque.
-- interpretation  states what the signal means and where to look. It
--                 deliberately stops short of a pricing recommendation:
--                 this feed has no occupancy, rate, or booking-pace data,
--                 so it is a leading indicator, not a decision.

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
),

labeled as (
    select *,
        case
            when days_in_window < 7 then 'WARMUP'
            when pressure_score >= 4       then 'HIGH'
            when pressure_score >= 2       then 'ELEVATED'
            else 'NORMAL'
        end as demand_pressure,

        -- Every rule that contributed a point, with its contribution.
        -- Empty string when nothing fired, which is itself informative.
        rtrim(
            (case when arrivals_vs_7day_avg_pct >= 20
                  then 'arrivals +' || cast(arrivals_vs_7day_avg_pct as varchar) || '% vs 7-day (+2); '
                  when arrivals_vs_7day_avg_pct >= 10
                  then 'arrivals +' || cast(arrivals_vs_7day_avg_pct as varchar) || '% vs 7-day (+1); '
                  else '' end)
            || (case when is_weekend = 1 then 'weekend (+1); ' else '' end)
            || (case when is_holiday = 1 then 'holiday (+1); ' else '' end)
            || (case when is_major_event = 1
                     then 'event: ' || coalesce(event_name, 'unnamed') || ' (+2); '
                     else '' end)
            || (case when is_extreme_heat = 1 then 'extreme heat (+1); '
                     when is_rain = 1 then 'rain (+1); '
                     else '' end)
        , '; ') as score_drivers
    from scored
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
    demand_pressure,
    case when score_drivers = '' then 'no rules fired' else score_drivers end
        as score_drivers,
    case demand_pressure
        when 'WARMUP' then
            'Baseline still building. The signal needs a full 7-day trailing '
            || 'window before it means anything. No action.'
        when 'HIGH' then
            'Inbound volume and conditions are well above trend for this market. '
            || 'This pattern typically precedes elevated same-day and next-day '
            || 'demand. Worth reviewing rate and remaining inventory before the '
            || 'evening arrival peak, alongside your own booking pace.'
        when 'ELEVATED' then
            'Running above baseline, but not decisively. Treat as a watch item: '
            || 'check whether your own booking pace is tracking the same '
            || 'direction before changing anything.'
        else
            'Inbound volume is in line with the trailing week. Nothing here '
            || 'argues for a change to rate or staffing.'
    end as interpretation
from labeled
