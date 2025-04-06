CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
	id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
	order_id TEXT NOT NULL,
	delivery_id TEXT NOT NULL,
	courier_id TEXT NOT NULL,
	courier_rate INT NOT NULL,
	order_sum INT NOT NULL,
	order_tip_sum INT NOT NULL,
	CONSTRAINT delivery_system_delivery_id_key UNIQUE (delivery_id),
	CONSTRAINT delivery_system_delivery_pkey PRIMARY KEY (id)
);


--SELECT * FROM stg.delivery_system_deliveries;