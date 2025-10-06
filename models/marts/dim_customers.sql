{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        customer_created_at,
        record_loaded_at
    FROM {{ ref('stg_customers') }}
),

deduped AS (
    -- Keep only the latest record per customer_id (if source has duplicates)
    SELECT
    customer_id,
    ANY_VALUE(first_name) AS first_name,
    ANY_VALUE(last_name) AS last_name,
    ANY_VALUE(email) AS email,
    MAX(customer_created_at) AS customer_created_at,
    MAX(record_loaded_at) AS record_loaded_at
    FROM source
    GROUP BY customer_id
)

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    customer_created_at,
    record_loaded_at,
    CURRENT_TIMESTAMP() AS record_updated_at
FROM deduped

{% if is_incremental() %}
  -- Only update or insert customers with new or changed data
  WHERE customer_id IN (
      SELECT customer_id
      FROM {{ ref('stg_customers') }}
      WHERE created_at > (SELECT COALESCE(MAX(customer_created_at), '1900-01-01') FROM {{ this }})
  )
{% endif %}
