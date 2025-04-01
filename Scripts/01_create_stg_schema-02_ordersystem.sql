SELECT id, object_id, object_value, update_ts
FROM stg.ordersystem_orders;

 
-- Создание таблицы для хранения данных о пользователях
CREATE TABLE stg.ordersystem_users (
    id serial PRIMARY KEY,               -- Уникальный идентификатор записи типа serial
    object_id varchar NOT NULL,          -- Уникальный идентификатор документа из MongoDB
    object_value text NOT NULL,          -- Хранение всего документа из MongoDB
    update_ts timestamp NOT NULL         -- Время обновления объекта из MongoDB
);
 

-- Создание таблицы для хранения данных о пользователях
CREATE TABLE stg.ordersystem_users (
    id serial PRIMARY KEY,               -- Уникальный идентификатор записи типа serial
    object_id varchar NOT NULL,          -- Уникальный идентификатор документа из MongoDB
    object_value text NOT NULL,          -- Хранение всего документа из MongoDB
    update_ts timestamp NOT NULL         -- Время обновления объекта из MongoDB
);
 

CREATE TABLE stg.ordersystem_users (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	user_name text NOT NULL,
	user_login text NOT NULL,
	update_ts timestamp NOT NULL
	--,CONSTRAINT ordersystem_users_pkey1 PRIMARY KEY (id)
);

ALTER TABLE stg.ordersystem_users
ADD CONSTRAINT unique_object_id UNIQUE (object_id);


CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR NOT NULL UNIQUE,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);