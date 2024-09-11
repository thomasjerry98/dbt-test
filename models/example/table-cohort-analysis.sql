{{ config(
    materialized='incremental'
) }}

-- Combine both CTEs in one WITH block
WITH customer_first_order AS (
    -- Step 1: Calculate the first-ever order date for each customer
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date  -- First purchase date for each customer
    FROM
        `raw.orders`
    GROUP BY
        customer_id
),
new_orders AS (
    -- Step 2: Select new orders and join with the first order date
    SELECT
        o.customer_id,
        f.first_order_date,  -- Always use the customer's first order date
        o.order_date,
        TIMESTAMP_DIFF(o.order_date, f.first_order_date, MONTH) AS months_since_first_order  -- Calculate months since first order
    FROM
        `raw.orders` AS o
    JOIN
        customer_first_order AS f
    ON
        o.customer_id = f.customer_id
    
    {% if is_incremental() %}
    -- Only select new records during incremental runs
    WHERE o.order_date >= (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
)

-- Final SELECT statement to return the results
SELECT * FROM new_orders



