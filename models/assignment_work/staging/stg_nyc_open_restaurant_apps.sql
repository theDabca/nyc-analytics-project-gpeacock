-- Clean and standardize Open Restaurant Applications data
-- One row per application

WITH source AS (
   SELECT * FROM {{ source('raw', 'source_nyc_open_restaurant_apps') }}
),

cleaned AS (
   SELECT
       -- Get all columns from source, except ones we're transforming below
       * EXCEPT (
           objectid,
           time_of_submission,
           restaurant_name,
           legal_business_name,
           bulding_number,
           borough,
           zip,
           latitude,
           longitude,
           approved_for_sidewalk_seating,
           approved_for_roadway_seating
       ),

       -- Identifiers
       CAST(objectid AS STRING) AS application_id,

       -- Date/Time
       CAST(time_of_submission AS TIMESTAMP) AS time_of_submission,

       -- Business Details
       UPPER(TRIM(CAST(restaurant_name AS STRING))) AS restaurant_name,
       UPPER(TRIM(CAST(legal_business_name AS STRING))) AS legal_business_name,

       -- Location - Fix the source data typo for building number
       CAST(bulding_number AS STRING) AS building_number,

       -- Location - Standardize borough
       CASE
           WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
           WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
           WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
           WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
           WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
           ELSE 'UNKNOWN'
       END AS borough,

       -- Location - Basic zip code cleaning
       CASE
           WHEN LENGTH(CAST(zip AS STRING)) = 5 THEN CAST(zip AS STRING)
           ELSE NULL
       END AS zip_code,

       CAST(latitude AS FLOAT64) AS latitude,
       CAST(longitude AS FLOAT64) AS longitude,

       -- Seating approvals
       UPPER(TRIM(CAST(approved_for_sidewalk_seating AS STRING))) AS approved_for_sidewalk_seating,
       UPPER(TRIM(CAST(approved_for_roadway_seating AS STRING))) AS approved_for_roadway_seating,

       -- Metadata
       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source

   -- Filters
   WHERE objectid IS NOT NULL
   AND time_of_submission IS NOT NULL

   -- Deduplicate
   QUALIFY ROW_NUMBER() OVER (PARTITION BY objectid ORDER BY time_of_submission DESC) = 1
)

SELECT * FROM cleaned