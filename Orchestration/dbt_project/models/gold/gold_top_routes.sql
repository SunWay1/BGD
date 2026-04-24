-- Top-N most popular pickup→dropoff zone pairs.
-- Routes with fewer than gold_top_routes_min_trips trips are excluded.

SELECT
    t.PULocationID                  AS pu_location_id,
    t.DOLocationID                  AS do_location_id,
    pu.Zone                         AS pickup_zone,
    do_zone.Zone                    AS dropoff_zone,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(t.fare_amount),   2)  AS avg_fare,
    ROUND(AVG(t.trip_distance), 2)  AS avg_distance_miles
FROM {{ ref('clean_trips') }} t
INNER JOIN {{ ref('clean_zones') }} pu      ON t.PULocationID = pu.LocationID
INNER JOIN {{ ref('clean_zones') }} do_zone ON t.DOLocationID = do_zone.LocationID
GROUP BY t.PULocationID, t.DOLocationID, pu.Zone, do_zone.Zone
HAVING COUNT(*) >= {{ var('gold_top_routes_min_trips') }}
ORDER BY trip_count DESC
LIMIT {{ var('gold_top_routes_limit') }}
