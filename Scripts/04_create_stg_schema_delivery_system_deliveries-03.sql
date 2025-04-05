--- CANCELED ORDER

SELECT id, order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum
FROM stg.delivery_system_deliveries
WHERE delivery_system_deliveries.order_id = '67daaf7fe0c4572708960647'
;