CREATE TABLE IF NOT EXISTS dds.fct_order_delivery (
	id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
	id_order INT NOT NULL,
	ts_id_order INT NOT NULL,
	id_delivery TEXT NOT NULL,
	courier_id TEXT NOT NULL,
	courier_name TEXT NOT NULL,
	courier_rate INT NOT NULL,
	order_sum INT NOT NULL,
	order_tip_sum INT NOT NULL,
	CONSTRAINT fct_order_delivery_pkey PRIMARY KEY (id)
);


/*
SELECT 
  ddo.id   AS id_order
  , ddt.id AS ts_id_order
  , ddd.id AS id_delivery
  , ddc.courier_id
  , ddc.courier_name
  , ddd.courier_rate
  , ddd.order_sum
  , ddd.order_tip_sum
FROM dds.dm_orders AS ddo
JOIN dds.dm_timestamps AS ddt ON ddt.id = ddo.timestamp_id
JOIN dds.dm_deliveries AS ddd ON ddd.order_id = ddo.order_key 
JOIN dds.dm_couriers   AS ddc ON ddc.courier_id = ddd.courier_id
*/