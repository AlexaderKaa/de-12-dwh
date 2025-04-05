SELECT id, order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum
FROM stg.delivery_system_deliveries sds
WHERE sds.order_id = '67f03a665b8a2e7e84b74510'
;

SELECT *
FROM dds.dm_orders ddo
WHERE ddo.order_key IN 
(
'67f03a665b8a2e7e84b74510'
)
;

SELECT * FROM dds.dm_timestamps ddt WHERE ddt.id = 15135  ;