{{ config(
    materialized='incremental'
) }}

with new_orders as (
    select
        customer_id,
        min(order_date) over(partition by customer_id) as first_order_date,
        order_date,
        timestamp_diff(order_date, min(order_date) over(partition by customer_id), month) as months_since_first_order
    from `raw.orders`
    
    {% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
    {% endif %}
)

select * from new_orders


