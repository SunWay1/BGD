-- Normalized taxi zones.
--
-- Changes from raw:
--   Borough = 'EWR' is renamed to 'New Jersey' (EWR is an IATA airport code, not a borough).
--   is_airport flag added for the three NYC-area airports.

SELECT
    CAST(LocationID AS INTEGER)                                                  AS LocationID,
    CASE WHEN Borough = 'EWR' THEN 'New Jersey' ELSE Borough END                AS Borough,
    Zone,
    service_zone,
    Zone IN ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')              AS is_airport
FROM {{ source('raw', 'raw_zones') }}
ORDER BY LocationID
