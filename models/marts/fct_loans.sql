{{
    config(
        materialized='incremental',
        unique_key='loan_id',
        incremental_strategy='merge'
    )
}}

WITH loans AS (
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
    FROM {{ ref('stg_loans') }}
),

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
        l.status,
        c.first_name,
        c.last_name,
        c.email,
        CURRENT_TIMESTAMP() AS record_loaded_at
    FROM loans l
    LEFT JOIN {{ ref('stg_customers') }} c
        ON l.customer_id = c.customer_id
),

deduped AS (
    -- Keep only the most recent record per loan_id (if duplicates exist)
    SELECT
        loan_id,
        FIRST_VALUE(application_id) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS application_id,
        FIRST_VALUE(customer_id) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS customer_id,
        FIRST_VALUE(loan_amount) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS loan_amount,
        FIRST_VALUE(interest_rate) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS interest_rate,
        FIRST_VALUE(start_date) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS start_date,
        FIRST_VALUE(end_date) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS end_date,
        FIRST_VALUE(loan_term_months) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS loan_term_months,
        FIRST_VALUE(disbursed_month) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS disbursed_month,
        FIRST_VALUE(status) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS status,
        FIRST_VALUE(first_name) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS first_name,
        FIRST_VALUE(last_name) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS last_name,
        FIRST_VALUE(email) OVER (PARTITION BY loan_id ORDER BY start_date DESC) AS email,
        MAX(record_loaded_at) AS record_loaded_at
    FROM joined
    GROUP BY loan_id
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
    status AS loan_status,
    record_loaded_at,
    CURRENT_TIMESTAMP() AS record_updated_at
FROM deduped

{% if is_incremental() %}
  -- Only merge new or recently updated loans
  WHERE loan_id IN (
      SELECT loan_id
      FROM {{ ref('stg_loans') }}
      WHERE start_date > (SELECT COALESCE(MAX(start_date), '1900-01-01') FROM {{ this }})
  )
{% endif %}
