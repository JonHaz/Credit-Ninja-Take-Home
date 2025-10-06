{{ config(materialized='table') }}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at
FROM {{ source('bronze', 'customers') }}
