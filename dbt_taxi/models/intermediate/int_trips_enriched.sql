with trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

enriched as (
    select
        *,

        -- tip behavior
        case
            when tip_amount > 0 then true
            else false
        end as is_tipped,

        tip_amount / nullif(fare_amount, 0) as tip_rate,

        -- time of day buckets
        case
            when pickup_hour between 7 and 9   then 'morning_rush'
            when pickup_hour between 10 and 15 then 'midday'
            when pickup_hour between 16 and 19 then 'evening_rush'
            when pickup_hour between 20 and 23 then 'night'
            else 'overnight'
        end as time_of_day,

        -- weekend flag (Spark: 1=Sunday, 7=Saturday)
        case
            when pickup_dayofweek in (1, 7) then true
            else false
        end as is_weekend,

        -- fare per mile
        fare_amount / nullif(trip_distance, 0) as fare_per_mile

    from trips
)

select * from enriched