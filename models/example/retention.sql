{{ config(
    materialized='table'
) }}

WITH base_data AS (
    -- Reference your original table that has the absolute counts
    SELECT
        customer_id,
        DATE_TRUNC('month', first_order_date) AS cohort_month,
        TIMESTAMP_DIFF(order_date, first_order_date, MONTH) AS months_since_first_order
    FROM
        {{ ref('table-cohort-analysis.sql') }}  -- Replace 'new_orders' with the actual name of your original table/model
),
cohort_sizes AS (
    -- Calculate the size of each cohort
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_size
    FROM
        base_data
    WHERE
        months_since_first_order = 0  -- Only consider the first month to get the initial cohort size
    GROUP BY
        cohort_month
),
active_customers AS (
    -- Calculate the number of active customers in each month for each cohort
    SELECT
        cohort_month,
        months_since_first_order,
        COUNT(DISTINCT customer_id) AS active_customers
    FROM
        base_data
    GROUP BY
        cohort_month,
        months_since_first_order
)
SELECT
    ac.cohort_month,
    ac.months_since_first_order,
    ac.active_customers,
    cs.cohort_size,
    ROUND((ac.active_customers::FLOAT / cs.cohort_size) * 100, 2) AS retention_percentage
FROM
    active_customers AS ac
JOIN
    cohort_sizes AS cs ON ac.cohort_month = cs.cohort_month
ORDER BY
    ac.cohort_month,
    ac.months_since_first_order;
