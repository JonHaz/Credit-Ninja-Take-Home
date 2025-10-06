{{ config(materialized='table') }}

-- This model generates a date dimension covering the full loan/payment timeline.

WITH date_spine AS (

    -- Define a reasonable date range: 10 years back and 5 years forward.
    SELECT
        SEQUENCE(
            DATEADD(year, -10, CURRENT_DATE()),
            DATEADD(year, 5, CURRENT_DATE()),
            INTERVAL 1 day
        ) AS date_array
),

unnested AS (
    SELECT EXPLODE(date_array) AS date_day
    FROM date_spine
)

SELECT
    date_day                                   AS date_actual,
    YEAR(date_day)                             AS year,
    MONTH(date_day)                            AS month,
    DAY(date_day)                              AS day,
    TO_CHAR(date_day, 'YYYY-MM-DD')            AS date_id,  
    TO_CHAR(date_day, 'YYYY-MM')               AS month_id,
    TO_CHAR(date_day, 'Month')                 AS month_name,
    QUARTER(date_day)                          AS quarter,
    TO_CHAR(date_day, 'YYYY-"Q"Q')             AS quarter_id,
    WEEKOFYEAR(date_day)                       AS week_of_year,
    DAYOFWEEK(date_day)                        AS day_of_week,
    CASE
        WHEN DAYOFWEEK(date_day) IN (6,7) THEN TRUE ELSE FALSE
    END                                        AS is_weekend
FROM unnested
ORDER BY date_actual
