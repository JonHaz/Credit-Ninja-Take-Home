{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge'
    )
}}

-- Pull the latest payment record per payment_id
WITH payments AS (
    SELECT
        payment_id,
        loan_id,
        customer_id,
        CAST(payment_amount AS DECIMAL(10,2)) AS payment_amount,
        payment_date,
        payment_type,
        record_loaded_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY record_loaded_at DESC) AS rn
        FROM {{ ref('stg_payments') }}
    )
    WHERE rn = 1
),

-- Pull the latest loan record per loan_id
loans AS (
    SELECT
        loan_id,
        customer_id,
        interest_rate,
        start_date,
        end_date,
        status AS loan_status,
        record_loaded_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY record_loaded_at DESC) AS rn
        FROM {{ ref('stg_loans') }}
    )
    WHERE rn = 1
),

-- Pull the latest customer record per customer_id
customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        record_loaded_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY record_loaded_at DESC) AS rn
        FROM {{ ref('stg_customers') }}
    )
    WHERE rn = 1
),

-- Join payments with loans and customers
joined AS (
    SELECT
        p.payment_id,
        p.loan_id,
        p.customer_id,
        p.payment_amount,
        p.payment_date,
        p.payment_type,
        l.loan_status,
        l.interest_rate,
        l.start_date,
        l.end_date,
        c.first_name,
        c.last_name,
        c.email,
        GREATEST(p.record_loaded_at, l.record_loaded_at, c.record_loaded_at) AS record_loaded_at
    FROM payments p
    LEFT JOIN loans l
        ON p.loan_id = l.loan_id
    LEFT JOIN customers c
        ON p.customer_id = c.customer_id
)

-- Final model: derive additional metrics
SELECT
    ROW_NUMBER() OVER (ORDER BY payment_id) AS payment_sk,
    payment_id,
    loan_id,
    customer_id,
    payment_amount,
    payment_date,
    DATE_TRUNC('month', payment_date) AS payment_month,
    payment_type,
    CASE 
        WHEN payment_date > end_date THEN TRUE ELSE FALSE
    END AS is_late_payment,
    loan_status,
    interest_rate,
    start_date,
    end_date,
    record_loaded_at,
    CURRENT_TIMESTAMP() AS record_updated_at
FROM joined

{% if is_incremental() %}
  WHERE record_loaded_at > (
      SELECT COALESCE(MAX(record_loaded_at), '1900-01-01') FROM {{ this }}
  )
{% endif %}
