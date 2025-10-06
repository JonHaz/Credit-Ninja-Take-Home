{{
    config(
        materialized='incremental',
        unique_key='loan_id',
        incremental_strategy='merge'
    )
}}

SELECT
    loan_id,
    application_id,
    customer_id,
    CAST(loan_amount AS DECIMAL(10,2)) AS loan_amount,
    CAST(interest_rate AS DECIMAL(5,2)) AS interest_rate,
    CAST(start_date AS DATE) AS start_date,
    CAST(end_date AS DATE) AS end_date,
    status,
    CURRENT_TIMESTAMP() AS record_loaded_at
FROM {{ source('snowflake', 'loans') }}


