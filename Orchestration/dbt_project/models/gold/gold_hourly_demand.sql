-- Trip demand aggregated by hour of day (0-23).
-- Always produces exactly 24 rows for 2 years of NYC data.

SELECT
    pickup_hour,
    COUNT(*)                            AS trip_count,
    ROUND(AVG(trip_distance),     2)    AS avg_distance_miles,
    ROUND(AVG(total_amount),      2)    AS avg_revenue,
    ROUND(AVG(trip_duration_min), 2)    AS avg_duration_min
FROM {{ ref('clean_trips') }}
GROUP BY pickup_hour
ORDER BY pickup_hour
