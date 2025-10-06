{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    created_at AS customer_created_at
FROM source
