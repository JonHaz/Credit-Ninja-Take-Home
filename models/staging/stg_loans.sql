{{ config(materialized='table') }}

SELECT
    loan_id,
    application_id,
    customer_id,
    CAST(loan_amount AS DECIMAL(10,2)) AS loan_amount,
    CAST(interest_rate AS DECIMAL(5,2)) AS interest_rate,
    start_date,
    end_date,
    status
FROM {{ source('bronze', 'loans') }}
