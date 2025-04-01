CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    bonus_percent NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (bonus_percent >= 0),
    min_payment_threshold NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (min_payment_threshold >= 0)
);


CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);


-- Создание таблицы bonussystem_users
CREATE TABLE stg.bonussystem_users (
    id integer NOT NULL,  -- Оригинальный id, без генерации новых
    order_user_id text NOT NULL,
    CONSTRAINT bonussystem_users_pkey PRIMARY KEY (id)
);


-- Создание таблицы bonussystem_events
CREATE TABLE stg.bonussystem_events (
    id integer NOT NULL,  -- Оригинальный id, без генерации новых
    event_ts timestamp NOT NULL,
    event_type varchar NOT NULL,
    event_value text NOT NULL,
    CONSTRAINT bonussystem_events_pkey PRIMARY KEY (id)
);