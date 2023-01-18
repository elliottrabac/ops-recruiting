{{config(materialized = 'table')}}
WITH paid_orders as (select Orders.ID as order_id,
        Orders.USER_ID    as customer_id,
        Orders.ORDER_DATE AS order_placed_at, 
            Orders.STATUS AS order_status,
        p.total_amount_paid,
        p.payment_finalized_date, 
        C.FIRST_NAME    as customer_first_name,
            C.LAST_NAME as customer_last_name
    FROM `growth-ops-recruiting`.analytics.orders as Orders
    left join (select ORDERID as order_id, max(CREATED) as payment_finalized_date, sum(AMOUNT) as total_amount_paid
from `growth-ops-recruiting`.stripe.payment
where STATUS <> 'fail'
group by 1) p ON orders.ID = p.order_id
left join `growth-ops-recruiting`.analytics.customers C on orders.USER_ID = C.ID ),

life_time_value as (select customer_id, sum(total_amount_paid) ltv from paid_orders group by customer_id),
 
customer_orders 
    as (select C.ID as customer_id
        , min(ORDER_DATE) as first_order_date
        , max(ORDER_DATE) as most_recent_order_date
        , count(ORDERS.ID) AS number_of_orders
    from `growth-ops-recruiting`.analytics.customers C 
    left join `growth-ops-recruiting`.analytics.orders as Orders
    on orders.USER_ID = C.ID 
    group by 1)

select
    p.*,
    customer_first_name || ' ' || customer_last_name as customer_full_name,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
    CASE WHEN c.first_order_date = p.order_placed_at
    THEN 'new' ELSE 'return' END as nvsr,
    ltv.ltv,
    c.first_order_date as fdos
    FROM paid_orders p
    left join customer_orders as c USING (customer_id)
    left join life_time_value as ltv on p.customer_id = ltv.customer_id
    ORDER BY order_id