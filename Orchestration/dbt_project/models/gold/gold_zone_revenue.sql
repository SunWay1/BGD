-- Revenue and trip metrics aggregated by pickup zone.
-- INNER JOIN drops LocationIDs 264/265 ('Unknown') which have no zone entry.

SELECT
    t.PULocationID                     AS location_id,
    z.Zone                             AS pickup_zone,
    z.Borough                          AS borough,
    z.is_airport,
    COUNT(*)                           AS trip_count,
    ROUND(SUM(t.total_amount),    2)   AS total_revenue,
    ROUND(AVG(t.fare_amount),     2)   AS avg_fare,
    ROUND(AVG(t.tip_pct),         2)   AS avg_tip_pct,
    ROUND(AVG(t.trip_distance),   2)   AS avg_distance_miles,
    ROUND(AVG(t.trip_duration_min), 2) AS avg_duration_min
FROM {{ ref('clean_trips') }} t
INNER JOIN {{ ref('clean_zones') }} z ON t.PULocationID = z.LocationID
GROUP BY t.PULocationID, z.Zone, z.Borough, z.is_airport
ORDER BY total_revenue DESC
