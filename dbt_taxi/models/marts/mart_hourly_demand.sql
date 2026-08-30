with trips as (
    select * from {{ ref('int_trips_enriched') }}
),

aggregated as (
    select
        pickup_hour,
        pickup_dayofweek,
        time_of_day,
        is_weekend,

        -- demand
        count(*)                          as trip_count,

        -- revenue
        round(sum(fare_amount), 2)        as total_fare,
        round(avg(fare_amount), 2)        as avg_fare,
        round(avg(tip_rate) * 100, 2)     as avg_tip_pct,

        -- trip characteristics
        round(avg(trip_distance), 2)      as avg_distance,
        round(avg(trip_duration_min), 2)  as avg_duration_min,
        round(avg(fare_per_mile), 2)      as avg_fare_per_mile,

        -- passenger stats
        round(avg(passenger_count), 2)    as avg_passengers

    from trips
    group by
        pickup_hour,
        pickup_dayofweek,
        time_of_day,
        is_weekend
)

select * from aggregated
order by pickup_dayofweek, pickup_hour