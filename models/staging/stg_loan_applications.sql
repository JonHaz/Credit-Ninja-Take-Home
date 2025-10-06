{{
    config(
        materialized='incremental',
        unique_key='loan_id',
        incremental_strategy='merge'
    )
}}

SELECT
    application_id,
    customer_id,
    CAST(application_date AS DATE) AS application_date,
    CAST(loan_amount_requested AS DECIMAL(10,2)) AS loan_amount_requested,
    status,
    updated_at,
    CURRENT_TIMESTAMP() AS record_loaded_at
FROM {{ source('snowflake', 'loan_applications') }}
