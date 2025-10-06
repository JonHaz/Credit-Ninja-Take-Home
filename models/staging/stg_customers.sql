{{ config(materialized='table') }}

SELECT
    customer_id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    LOWER(email) AS email,
    created_at
FROM {{ source('bronze', 'customers') }}
