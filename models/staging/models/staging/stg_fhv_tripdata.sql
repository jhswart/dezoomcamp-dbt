{{ config(materialized='view') }}

select 
    -- identifiers
    dispatching_base_num,
    cast(SR_Flag as integer) as sr_flag,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    Affiliated_base_number as affiliated_base_number

from {{ source('staging', 'fhv_tripdata_non_partitoned') }}
where EXTRACT(YEAR FROM CAST(pickup_datetime as datetime)) = 2019 OR EXTRACT(YEAR FROM CAST(dropOff_datetime as datetime)) = 2019

