SELECT id, object_id, object_name
FROM stg.delivery_system_restaurants;

-- DELETE FROM stg.delivery_system_restaurants;


CREATE TABLE IF NOT EXISTS stg.delivery_system_restaurants (
	id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
	object_id varchar NOT NULL,
	object_name text NOT NULL,
	CONSTRAINT delivery_system_restaurants_object_id_key UNIQUE (object_id),
	CONSTRAINT delivery_system_restaurants_pkey PRIMARY KEY (id)
);

SELECT id, workflow_key, workflow_settings
FROM stg.srv_wf_settings;

-- select setval('delivery_system_restaurants_id_seq', -1)

-- UPDATE stg.srv_wf_settings SET workflow_settings = '{"last_loaded_id": 0}' WHERE id = 3562;
