-- Создание таблицы для хранения данных о заказах
/*
CREATE TABLE IF NOT EXISTS  stg.ordersystem_orders (
    id serial PRIMARY KEY,               -- Уникальный идентификатор записи типа serial
    object_id varchar NOT NULL,          -- Уникальный идентификатор документа из MongoDB
    object_value text NOT NULL,          -- Хранение всего документа из MongoDB
    update_ts timestamp NOT NULL         -- Время обновления объекта из MongoDB
);
*/

CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
	id SERIAL PRIMARY KEY,
	object_id TEXT NOT NULL,
	restaurant_id TEXT NOT NULL,
	users_id TEXT NOT NULL,
	order_items JSONB NOT NULL,
	bonus_payment NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (bonus_payment >= 0),
	cost NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (cost >= 0),
	payment NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (payment >= 0),
	bonus_grant NUMERIC(19, 5)	DEFAULT 0 NOT NULL CHECK (bonus_grant >= 0),
	statuses JSONB NOT NULL,
	final_status TEXT NOT NULL,
	update_ts TIMESTAMP NOT NULL
);