{{
    config(
        materialized='incremental',
        unique_key='loan_id',
        incremental_strategy='merge'
    )
}}

-- Pull the latest loan record per loan_id
WITH loans AS (
    SELECT
        loan_id,
        application_id,
        customer_id,
        loan_amount,
        interest_rate,
        start_date,
        end_date,
        status,
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

-- Join loans and customers
joined AS (
    SELECT
        l.loan_id,
        l.application_id,
        l.customer_id,
        l.loan_amount,
        l.interest_rate,
        l.start_date,
        l.end_date,
        DATEDIFF(month, l.start_date, l.end_date) AS loan_term_months,
        DATE_TRUNC('month', l.start_date) AS disbursed_month,
        l.status AS loan_status,
        c.first_name,
        c.last_name,
        c.email,
        GREATEST(l.record_loaded_at, c.record_loaded_at) AS record_loaded_at
    FROM loans l
    LEFT JOIN customers c
        ON l.customer_id = c.customer_id
)

SELECT
    ROW_NUMBER() OVER (ORDER BY loan_id) AS loan_sk,
    loan_id,
    application_id,
    customer_id,
    first_name,
    last_name,
    email,
    loan_amount,
    interest_rate,
    start_date,
    end_date,
    loan_term_months,
    disbursed_month,
    loan_status,
    record_loaded_at,
    CURRENT_TIMESTAMP() AS record_updated_at
FROM joined

{% if is_incremental() %}
  WHERE record_loaded_at > (
      SELECT COALESCE(MAX(record_loaded_at), '1900-01-01') FROM {{ this }}
  )
{% endif %}
