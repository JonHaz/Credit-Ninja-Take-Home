{{ config(materialized='table') }}

SELECT
    application_id,
    customer_id,
    CAST(application_date AS DATE) AS application_date,
    CAST(loan_amount_requested AS DECIMAL(10,2)) AS loan_amount_requested,
    status,
    updated_at
FROM {{ source('snowflake', 'loan_applications') }}
