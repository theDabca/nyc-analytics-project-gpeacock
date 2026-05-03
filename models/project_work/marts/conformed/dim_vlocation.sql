{{ config(materialized='table') }}

with locations as (

    select distinct
        borough,
        zip_code
    from {{ ref('stg_nyc_311_service_request_history') }}

    union distinct

    select distinct
        borough,
        zip_code
    from {{ ref('stg_nyc_service_mvcollision') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['borough','zip_code']) }} as location_sk,
        borough,
        zip_code
    from locations

)

select * from final