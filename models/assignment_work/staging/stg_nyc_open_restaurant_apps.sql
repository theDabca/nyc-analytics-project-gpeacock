-- Clean and standardize NYC Open Restaurant Applications data
-- One row per restaurant application

WITH source AS (
   SELECT * FROM {{ source('raw', 'open_restaurant_applications') }}
),

cleaned AS (
   SELECT
       -- Keep other columns except ones we clean
       * EXCEPT (
           objectid,
           restaurant_name,
           legal_business_name,
           borough,
           zip_code,
           seating_interest,
           sidewalk_dimensions,
           roadway_dimensions,
           approved_for_sidewalk_seating,
           approved_for_roadway_seating,
           sla_serial_number,
           sla_license_type,
           application_status,
           application_type,
           submission_timestamp
       ),

       -- Identifier
       CAST(objectid AS STRING) AS application_id,

       -- Restaurant Info
       TRIM(CAST(restaurant_name AS STRING)) AS restaurant_name,
       TRIM(CAST(legal_business_name AS STRING)) AS legal_business_name,

       -- Borough standardization (reuse your logic 👍)
       CASE
           WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
           WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
           WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
           WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
           WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
           ELSE 'UNKNOWN'
       END AS borough,

       -- Zip Code Cleaning
       CASE
           WHEN LENGTH(CAST(zip_code AS STRING)) = 5 THEN CAST(zip_code AS STRING)
           ELSE NULL
       END AS zip_code,

       -- Application Details
       UPPER(TRIM(CAST(application_status AS STRING))) AS application_status,
       UPPER(TRIM(CAST(application_type AS STRING))) AS application_type,

       -- Seating Info
       UPPER(TRIM(CAST(seating_interest AS STRING))) AS seating_interest,
       CAST(approved_for_sidewalk_seating AS STRING) AS sidewalk_seating_flag,
       CAST(approved_for_roadway_seating AS STRING) AS roadway_seating_flag,

       -- SLA Info
       CAST(sla_serial_number AS STRING) AS sla_serial_number,
       CAST(sla_license_type AS STRING) AS sla_license_type,

       -- Dates
       CAST(submission_timestamp AS TIMESTAMP) AS submission_timestamp,

       -- Metadata
       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source

   -- Filters
   WHERE objectid IS NOT NULL
     AND submission_timestamp IS NOT NULL
     AND borough IS NOT NULL

   -- Deduplicate (latest application per ID)
   QUALIFY ROW_NUMBER() OVER (
       PARTITION BY objectid
       ORDER BY submission_timestamp DESC
   ) = 1
)

SELECT * FROM cleaned
