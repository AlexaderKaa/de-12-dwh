CREATE TABLE IF NOT EXISTS stg.delivery_system_couriers (
	id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
	object_id varchar NOT NULL,
	object_name text NOT NULL,
	CONSTRAINT delivery_system_couriers_object_id_key UNIQUE (object_id),
	CONSTRAINT delivery_system_couriers_pkey PRIMARY KEY (id)
);


--SELECT * FROM stg.delivery_system_couriers;