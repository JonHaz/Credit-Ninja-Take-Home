{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}


    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        created_at AS customer_created_at,
        CURRENT_TIMESTAMP() AS record_loaded_at
    FROM {{ ref('stg_customers') }}

