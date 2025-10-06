{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge'
    )
}}
    
SELECT
    payment_id,
    loan_id,
    customer_id,
    CAST(payment_amount AS DECIMAL(10,2)) AS payment_amount,
    payment_date,
    payment_type
FROM {{ source('snowflake', 'payments') }}
