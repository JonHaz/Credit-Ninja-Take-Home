{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge'
    )
}}

WITH payments AS (
    SELECT
        payment_id,
        loan_id,
        customer_id,
        CAST(payment_amount AS DECIMAL(10,2)) AS payment_amount,
        payment_date,
        payment_type
    FROM {{ ref('stg_payments') }}
),

joined AS (
    SELECT
        p.payment_id,
        p.loan_id,
        p.customer_id,
        p.payment_amount,
        p.payment_date,
        p.payment_type,
        l.status AS loan_status,
        l.interest_rate,
        l.start_date,
        l.end_date,
        c.first_name,
        c.last_name,
        c.email
    FROM payments p
    LEFT JOIN {{ ref('stg_loans') }} l
        ON p.loan_id = l.loan_id
    LEFT JOIN {{ ref('stg_customers') }} c
        ON p.customer_id = c.customer_id
),

enhanced AS (
    SELECT
        j.payment_id,
        j.loan_id,
        j.customer_id,
        j.payment_amount,
        j.payment_date,
        j.payment_type,
        j.loan_status,
        j.interest_rate,
        j.start_date,
        j.end_date,
        j.first_name,
        j.last_name,
        j.email,
        CASE 
            WHEN j.payment_date > j.end_date THEN TRUE ELSE FALSE
        END AS is_late_payment,
        DATE_TRUNC('month', j.payment_date) AS payment_month
    FROM joined j
)

SELECT
    ROW_NUMBER() OVER (ORDER BY payment_id) AS payment_sk,
    payment_id,
    loan_id,
    customer_id,
    payment_amount,
    payment_date,
    payment_month,
    payment_type,
    is_late_payment,
    loan_status,
    interest_rate,
    start_date,
    end_date
FROM enhanced
