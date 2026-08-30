with source as (
    select * from raw_trips
),

renamed as (
    select
        -- identifiers
        vendor_id,
        rate_code,
        payment_type,

        -- timestamps
        pickup_datetime,
        dropoff_datetime,

        -- coordinates (cast from varchar to double)
        cast(pickup_latitude as double)   as pickup_latitude,
        cast(pickup_longitude as double)  as pickup_longitude,
        cast(dropoff_latitude as double)  as dropoff_latitude,
        cast(dropoff_longitude as double) as dropoff_longitude,

        -- trip facts
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,

        -- engineered features (already computed by PySpark)
        trip_duration_min,
        pickup_hour,
        pickup_dayofweek

    from source
    where
        fare_amount > 0
        and trip_distance > 0
        and trip_duration_min > 0
        and pickup_latitude is not null
        and pickup_longitude is not null
)

select * from renamed