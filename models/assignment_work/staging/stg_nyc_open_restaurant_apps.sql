-- Clean and standardize NYC Open Restaurant Applications (Historic)
-- One row per restaurant application

WITH source AS (
   SELECT * FROM {{ source('raw', 'source_nyc_open_restaurant_apps') }}
),

cleaned AS (
   SELECT
       -- 1. EXCEPT list now matches your screenshot names exactly
       * EXCEPT (
           objectid,
           restaurant_name,
           legal_business_name,
           borough,
           zip,
           time_of_submission,           -- Fix: This is the name in your screenshot
           seating_interest_sidewalk,    -- Fix: Matches screenshot
           approved_for_sidewalk_seating,
           approved_for_roadway_seating,
           sla_serial_number,
           sla_license_type
       ),

       -- Identifier
       CAST(objectid AS STRING) AS application_id,

       -- Restaurant Info
       TRIM(CAST(restaurant_name AS STRING)) AS restaurant_name,
       TRIM(CAST(legal_business_name AS STRING)) AS legal_business_name,

       -- Borough Standardization
       CASE
           WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
           WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
           WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
           WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
           WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
           ELSE 'UNKNOWN'
       END AS borough,

       -- Zip Code Cleaning (using raw 'zip' field from your screenshot)
       CASE
           WHEN LENGTH(CAST(zip AS STRING)) = 5 THEN CAST(zip AS STRING)
           ELSE NULL
       END AS zip_code,

       -- Seating Info (using the specific sidewalk field from your screenshot)
       UPPER(TRIM(CAST(seating_interest_sidewalk AS STRING))) AS seating_interest,
       CAST(approved_for_sidewalk_seating AS STRING) AS sidewalk_seating_flag,
       CAST(approved_for_roadway_seating AS STRING) AS roadway_seating_flag,

       -- SLA Info
       CAST(sla_serial_number AS STRING) AS sla_serial_number,
       CAST(sla_license_type AS STRING) AS sla_license_type,

       -- Date Transformation (using raw 'time_of_submission' field from your screenshot)
       CAST(time_of_submission AS TIMESTAMP) AS submission_timestamp,

       -- Metadata
       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source

   -- 2. Ensure filters use the correct raw column names from BigQuery
   WHERE objectid IS NOT NULL
     AND time_of_submission IS NOT NULL 
     AND borough IS NOT NULL

   -- 3. Deduplicate using the correct timestamp field
   QUALIFY ROW_NUMBER() OVER (
       PARTITION BY objectid
       ORDER BY time_of_submission DESC   
   ) = 1
)

SELECT * FROM cleaned