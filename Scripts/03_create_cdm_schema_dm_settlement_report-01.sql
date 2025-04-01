SELECT 
    do2.restaurant_id,
    r.restaurant_name,
    dt.date, 
    COUNT(distinct do2.order_key) AS orders_count,
    SUM(fps.total_sum) AS orders_total_sum,
    SUM(fps.bonus_payment) AS orders_bonus_payment_sum,
    SUM(fps.bonus_grant) AS orders_bonus_granted_sum,
    SUM(fps.total_sum) * 0.25 AS order_processing_fee,
    SUM(fps.total_sum) - (SUM(fps.total_sum) * 0.25) - SUM(fps.bonus_payment) AS restaurant_reward_sum
FROM dds.dm_orders do2
left join dds.dm_timestamps dt on dt.id = do2.timestamp_id
LEFT JOIN dds.fct_product_sales fps ON fps.order_id = do2.id
LEFT JOIN dds.dm_restaurants r ON r.id = do2.restaurant_id
WHERE do2.order_status = 'CLOSED'
GROUP BY do2.restaurant_id, r.restaurant_name, dt.date
ORDER BY dt.date DESC, do2.restaurant_id --, r.restaurant_name
ON CONFLICT (restaurant_id, settlement_date)
DO UPDATE SET 
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
;