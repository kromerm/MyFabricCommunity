-- models/silver_yellow_taxi_trips.sql
-- Silver layer transformation for NYC yellow cab trip data.
-- Filters bad rows, adds derived columns, and joins the zone lookup
-- to resolve PULocationID / DOLocationID to human-readable boroughs and zones.

{{
  config(
    materialized='incremental',
    unique_key=['VendorID', 'tpep_pickup_datetime'],
    on_schema_change='sync_all_columns'
  )
}}

WITH bronze AS (
    SELECT *
    FROM {{ source('bronze', 'yellow_tripdata') }}
    WHERE fare_amount   > 0
      AND trip_distance > 0
      AND tpep_pickup_datetime < tpep_dropoff_datetime
    {% if is_incremental() %}
      AND tpep_pickup_datetime >= DATEADD(month, -1, GETDATE())
    {% endif %}
),

zone_lookup AS (
    SELECT LocationID, Borough, Zone
    FROM {{ source('bronze', 'taxi_zone_lookup') }}
),

enriched AS (
    SELECT
        b.*,
        DATEDIFF(minute,
                 b.tpep_pickup_datetime,
                 b.tpep_dropoff_datetime)         AS trip_duration_minutes,
        CASE WHEN b.fare_amount > 0
             THEN b.tip_amount / b.fare_amount * 100.0
             ELSE 0 END                           AS tip_pct,
        pu.Borough                                AS pickup_borough,
        pu.Zone                                    AS pickup_zone,
        do_.Borough                               AS dropoff_borough,
        do_.Zone                                  AS dropoff_zone
    FROM bronze        b
    LEFT JOIN zone_lookup pu  ON b.PULocationID = pu.LocationID
    LEFT JOIN zone_lookup do_ ON b.DOLocationID = do_.LocationID
)

SELECT * FROM enriched
