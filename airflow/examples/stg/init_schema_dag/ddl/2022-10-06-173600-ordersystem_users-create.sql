-- Создание таблицы для хранения данных о пользователях
/*
CREATE TABLE IF NOT EXISTS  stg.ordersystem_users (
    id serial PRIMARY KEY,               -- Уникальный идентификатор записи типа serial
    object_id varchar NOT NULL,          -- Уникальный идентификатор документа из MongoDB
    object_value text NOT NULL,          -- Хранение всего документа из MongoDB
    update_ts timestamp NOT NULL         -- Время обновления объекта из MongoDB
);
*/

CREATE TABLE IF NOT EXISTS  stg.ordersystem_users (
    id serial PRIMARY KEY,               -- Уникальный идентификатор записи типа serial
    object_id varchar NOT NULL,          -- Уникальный идентификатор документа из MongoDB
    user_name text NOT NULL,          	
	user_login text NOT NULL,          	
    update_ts timestamp NOT NULL         -- Время обновления объекта из MongoDB
);

/*
CREATE TABLE IF NOT EXISTS
stg.ordersystem_users (
id SERIAL PRIMARY KEY,
object_id TEXT NOT NULL,
user_name TEXT NOT NULL,
user_login TEXT NOT NULL
UNIQUE,
update_ts TIMESTAMP NOT NULL
);*/