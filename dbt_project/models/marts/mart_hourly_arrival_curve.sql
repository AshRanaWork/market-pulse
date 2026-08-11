-- GOLD: arrivals by hour of day per market. The shape an ops manager
-- uses to time front-desk, valet, and shuttle staffing peaks.

select
    market,
    arrival_date_local,
    arrival_hour_local,
    count(*) as arrivals
from {{ ref('stg_flights') }}
group by market, arrival_date_local, arrival_hour_local
