-- Filtered and enriched taxi trips.
--
-- Filters (thresholds defined in dbt_project.yml vars):
--   fare_amount, trip_distance, passenger_count, location IDs — in inner query
--   trip_duration_min — in outer query (computed field, needs both timestamps first)
--
-- union_by_name=true on the parquet source handles schema drift: congestion_surcharge
-- and airport_fee were added at different points in time and are absent in older files.

SELECT *
FROM (
    SELECT
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        CAST(passenger_count AS INTEGER)    AS passenger_count,
        trip_distance,
        CAST(PULocationID AS INTEGER)       AS PULocationID,
        CAST(DOLocationID AS INTEGER)       AS DOLocationID,
        RatecodeID,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        COALESCE(congestion_surcharge, 0.0) AS congestion_surcharge,
        COALESCE(airport_fee, 0.0)          AS airport_fee,

        CAST(EXTRACT(HOUR FROM tpep_pickup_datetime) AS INTEGER)    AS pickup_hour,
        CAST(tpep_pickup_datetime AS DATE)::VARCHAR                  AS pickup_date,
        CAST(EXTRACT(YEAR FROM tpep_pickup_datetime) AS INTEGER)    AS pickup_year,

        ROUND(
            date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0,
            1
        ) AS trip_duration_min,

        ROUND(
            CASE WHEN fare_amount > 0
                THEN (tip_amount / fare_amount) * 100.0
                ELSE 0.0
            END,
            1
        ) AS tip_pct

    FROM {{ source('raw', 'raw_trips') }}
    WHERE
        fare_amount      >  {{ var('filter_fare_min') }}
        AND fare_amount  <= {{ var('filter_fare_max') }}
        AND trip_distance >  {{ var('filter_distance_min') }}
        AND trip_distance <= {{ var('filter_distance_max') }}
        AND passenger_count >= {{ var('filter_passenger_min') }}
        AND passenger_count <= {{ var('filter_passenger_max') }}
        AND tpep_pickup_datetime  IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
        AND PULocationID IS NOT NULL
        AND DOLocationID IS NOT NULL
)
WHERE trip_duration_min >= {{ var('filter_duration_min_minutes') }}
  AND trip_duration_min <= {{ var('filter_duration_max_minutes') }}
