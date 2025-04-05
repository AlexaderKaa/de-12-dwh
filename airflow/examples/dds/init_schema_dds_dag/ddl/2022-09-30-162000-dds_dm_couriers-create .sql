CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
	courier_id varchar NOT NULL,
	courier_name text NOT NULL,
	CONSTRAINT delivery_system_courier_id_key UNIQUE (courier_id),
	CONSTRAINT delivery_system_courier_pkey PRIMARY KEY (id)
);


--SELECT * FROM stg.delivery_system_couriers;