SELECT 
    table_name,
    column_name,
    data_type,
    character_maximum_length,
    column_default,
    is_nullable
FROM 
    information_schema.columns
WHERE 
    table_schema = 'public'
ORDER BY 
    table_name, column_name;
    
Код:    HL54o2dXuo
---------------------------------------------
 
SELECT DISTINCT
    product->>'product_name' AS product_name
FROM
    outbox,
    jsonb_array_elements(event_value::jsonb->'product_payments') AS product
WHERE
    event_value::jsonb->'product_payments' IS NOT NULL
    AND product->>'product_name' IS NOT NULL;
    
Код: AqwV4d9rlh
---------------------------------------------
 
CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id serial NOT NULL,
    restaurant_id varchar NOT NULL,
    restaurant_name varchar NOT NULL,
    settlement_date date NOT NULL,
    orders_count integer NOT NULL,
    orders_total_sum numeric(14, 2) NOT NULL,
    orders_bonus_payment_sum numeric(14, 2) NOT NULL,
    orders_bonus_granted_sum numeric(14, 2) NOT NULL,
    order_processing_fee numeric(14, 2) NOT NULL,
    restaurant_reward_sum numeric(14, 2) NOT NULL,
    PRIMARY KEY (id)
);
 
Двигайтесь дальше! Ваш код: xjnEv7JXWN
 
---------------------------------------------
 
CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id serial NOT NULL PRIMARY KEY,
    restaurant_id varchar NOT NULL,
    restaurant_name varchar NOT NULL,
    settlement_date date NOT NULL,
    orders_count integer NOT NULL,
    orders_total_sum numeric(14, 2) NOT NULL,
    orders_bonus_payment_sum numeric(14, 2) NOT NULL,
    orders_bonus_granted_sum numeric(14, 2) NOT NULL,
    order_processing_fee numeric(14, 2) NOT NULL,
    restaurant_reward_sum numeric(14, 2) NOT NULL
);
 
Двигайтесь дальше! Ваш код: 5zF17A2Gzb
 
---------------------------------------------
 
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_settlement_date_check CHECK (EXTRACT(YEAR FROM settlement_date) >= 2022 AND EXTRACT(YEAR FROM settlement_date) < 2500);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_settlement_date_check CHECK (settlement_date >= '2022-01-01' AND settlement_date < '2500-01-01');
 
код прислали: ZBfLxNLryk
 
---------------------------------------------
-- Добавляем значение по умолчанию
ALTER TABLE cdm.dm_settlement_report
ALTER COLUMN orders_count SET DEFAULT 0;
 
ALTER TABLE cdm.dm_settlement_report
ALTER COLUMN orders_total_sum SET DEFAULT 0;
 
ALTER TABLE cdm.dm_settlement_report
ALTER COLUMN orders_bonus_payment_sum SET DEFAULT 0;
 
ALTER TABLE cdm.dm_settlement_report
ALTER COLUMN orders_bonus_granted_sum SET DEFAULT 0;
 
ALTER TABLE cdm.dm_settlement_report
ALTER COLUMN order_processing_fee SET DEFAULT 0;
 
ALTER TABLE cdm.dm_settlement_report
ALTER COLUMN restaurant_reward_sum SET DEFAULT 0;
 
-- Добавляем ограничения CHECK
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT orders_count_check CHECK (orders_count >= 0);
 
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT orders_total_sum_check CHECK (orders_total_sum >= 0);
 
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT orders_bonus_payment_sum_check CHECK (orders_bonus_payment_sum >= 0);
 
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT orders_bonus_granted_sum_check CHECK (orders_bonus_granted_sum >= 0);
 
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT order_processing_fee_check CHECK (order_processing_fee >= 0);
 
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT restaurant_reward_sum_check CHECK (restaurant_reward_sum >= 0);
 
Двигайтесь дальше! Ваш код: qUpu6QoZs9
 
--------------------------------------------------
 
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT unique_restaurant_period UNIQUE (restaurant_id, settlement_date);
SELECT restaurant_id, settlement_date, COUNT(*)
FROM cdm.dm_settlement_report
GROUP BY restaurant_id, settlement_date
HAVING COUNT(*) > 1;
 
Двигайтесь дальше! Ваш код: YLalElZtMP
 
--------------------------------------
 
CREATE SCHEMA IF NOT EXISTS stg;
 
-- Создание таблицы bonussystem_users
CREATE TABLE stg.bonussystem_users (
    id integer NOT NULL,  -- Оригинальный id, без генерации новых
    order_user_id text NOT NULL,
    CONSTRAINT bonussystem_users_pkey PRIMARY KEY (id)
);
 
-- Создание таблицы bonussystem_ranks
CREATE TABLE stg.bonussystem_ranks (
    id integer NOT NULL,  -- Оригинальный id, без генерации новых
    "name" varchar(2048) NOT NULL,
    bonus_percent numeric(19, 5) DEFAULT 0 NOT NULL CHECK (bonus_percent >= 0),
    min_payment_threshold numeric(19, 5) DEFAULT 0 NOT NULL CHECK (min_payment_threshold >= 0),
    CONSTRAINT bonussystem_ranks_pkey PRIMARY KEY (id)
);
 
-- Создание таблицы bonussystem_events
CREATE TABLE stg.bonussystem_events (
    id integer NOT NULL,  -- Оригинальный id, без генерации новых
    event_ts timestamp NOT NULL,
    event_type varchar NOT NULL,
    event_value text NOT NULL,
    CONSTRAINT bonussystem_events_pkey PRIMARY KEY (id)
);
 
-- Создание индекса на поле event_ts
CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);
 
Двигайтесь дальше! Ваш код: S3Hlgxf3Vd
 
--------------------------------------
 
-- Создание таблицы для хранения данных о заказах
CREATE TABLE stg.ordersystem_orders (
    id serial PRIMARY KEY,               -- Уникальный идентификатор записи типа serial
    object_id varchar NOT NULL,          -- Уникальный идентификатор документа из MongoDB
    object_value text NOT NULL,          -- Хранение всего документа из MongoDB
    update_ts timestamp NOT NULL         -- Время обновления объекта из MongoDB
);
 
-- Создание таблицы для хранения данных о ресторанах
CREATE TABLE stg.ordersystem_restaurants (
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
 
Двигайтесь дальше! Ваш код: OIoYDT7RQC
 
--------------------------------------
 
-- Создание таблицы для хранения данных о заказах
DROP table stg.ordersystem_orders;
CREATE TABLE IF NOT exists stg.ordersystem_orders (
    id serial PRIMARY KEY,               -- Уникальный идентификатор записи типа serial
    object_id varchar NOT NULL,          -- Уникальный идентификатор документа из MongoDB
    object_value text NOT NULL,          -- Хранение всего документа из MongoDB
    update_ts timestamp NOT NULL,        -- Время обновления объекта из MongoDB
    CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id)  -- Уникальное ограничение на object_id
);
 
-- Создание таблицы для хранения данных о ресторанах
DROP table stg.ordersystem_restaurants;
CREATE TABLE IF NOT exists stg.ordersystem_restaurants (
    id serial PRIMARY KEY,               -- Уникальный идентификатор записи типа serial
    object_id varchar NOT NULL,          -- Уникальный идентификатор документа из MongoDB
    object_value text NOT NULL,          -- Хранение всего документа из MongoDB
    update_ts timestamp NOT NULL,        -- Время обновления объекта из MongoDB
    CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id)  -- Уникальное ограничение на object_id
);
 
-- Создание таблицы для хранения данных о пользователях
DROP table stg.ordersystem_users;
CREATE TABLE IF NOT exists stg.ordersystem_users (
    id serial PRIMARY KEY,               -- Уникальный идентификатор записи типа serial
    object_id varchar NOT NULL,          -- Уникальный идентификатор документа из MongoDB
    object_value text NOT NULL,          -- Хранение всего документа из MongoDB
    update_ts timestamp NOT NULL,        -- Время обновления объекта из MongoDB
    CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id)  -- Уникальное ограничение на object_id
);
 
Двигайтесь дальше! Ваш код: Ve7J48uY2K
 
--------------------------------------
 
UPDATE clients
SET login = 'arthur_dent'
WHERE client_id = 42;
 
Двигайтесь дальше! Ваш код: S3Hlgxf3Vd
 
--------------------------------------
 
-- Удалите внешний ключ из sales
ALTER TABLE sales
    DROP CONSTRAINT sales_products_product_id_fk;
 
-- Удалите первичный ключ из products
ALTER TABLE products
    DROP CONSTRAINT products_pk;
 
-- Добавьте новое поле id для суррогатного ключа в products
ALTER TABLE products
    ADD COLUMN id SERIAL NOT NULL;
 
-- Сделайте данное поле первичным ключом
ALTER TABLE products
    ADD CONSTRAINT products_pk PRIMARY KEY (id);
 
-- Добавьте дату начала действия записи в products
ALTER TABLE products
    ADD COLUMN valid_from timestamptz NOT NULL DEFAULT NOW();
 
-- Добавьте дату окончания действия записи в products
ALTER TABLE products
    ADD COLUMN valid_to timestamptz;
 
-- Добавьте новый внешний ключ sales_products_id_fk в sales
ALTER TABLE sales
    ADD CONSTRAINT sales_products_id_fk FOREIGN KEY (product_id) REFERENCES products (id);
    
    
Двигайтесь дальше! Ваш код: OIoYDT7RQC
 
--------------------------------------
 
CREATE SCHEMA IF NOT EXISTS dds;
 
Двигайтесь дальше! Ваш код: 7lcOGpj4ZL
 
--------------------------------------
 
CREATE TABLE dds.dm_users
(
    id SERIAL PRIMARY KEY,              -- Суррогатный первичный ключ
    user_id varchar NOT NULL,           -- Идентификатор пользователя из MongoDB
    user_name varchar NOT NULL,            -- Имя пользователя
    user_login varchar NOT NULL             -- Логин пользователя
);
 
Двигайтесь дальше! Ваш код: g0MSs4ez0v
 
--------------------------------------
 
CREATE TABLE dds.dm_restaurants
(
    id SERIAL PRIMARY KEY,               -- Суррогатный первичный ключ
    restaurant_id VARCHAR NOT NULL, -- Идентификатор ресторана
    restaurant_name VARCHAR NOT NULL,-- Название ресторана
    active_from TIMESTAMP NOT NULL,      -- Дата начала действия записи
    active_to TIMESTAMP NOT NULL          -- Дата окончания действия записи
);
 
Двигайтесь дальше! Ваш код: h5xVUDD1Wm
 
--------------------------------------
 
CREATE TABLE dds.dm_products
(
    id SERIAL PRIMARY KEY,                        -- Суррогатный первичный ключ
    product_id VARCHAR NOT NULL,           -- Идентификатор продукта
    product_name VARCHAR NOT NULL,         -- Название продукта
    product_price NUMERIC(14, 2) DEFAULT 0 NOT NULL 
        CHECK (product_price >= 0),             -- Цена продукта с ограничением на значение
    restaurant_id integer NOT NULL,        -- Идентификатор ресторана
    active_from TIMESTAMP NOT NULL,              -- Дата начала действия записи
    active_to TIMESTAMP NOT NULL                 -- Дата окончания действия записи
);
 
Двигайтесь дальше! Ваш код: yCYrkJMm2Z
 
--------------------------------------
 
ALTER TABLE dds.dm_restaurants
   ADD CONSTRAINT unique_restaurant_id UNIQUE (restaurant_id);
   
ALTER TABLE dds.dm_products
   ADD CONSTRAINT dm_products_restaurant_id_fkey
   FOREIGN KEY (restaurant_id)
   REFERENCES dds.dm_restaurants (id);
   
Двигайтесь дальше! Ваш код: rgNx3z8aKm
 
--------------------------------------
 
CREATE TABLE dds.dm_timestamps (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP NOT NULL,
    year SMALLINT CHECK (year >= 2022 AND year < 2500) NOT NULL,
    month SMALLINT CHECK (month >= 1 AND month <= 12) NOT NULL,
    day SMALLINT CHECK (day >= 1 AND day <= 31) NOT NULL,
    time TIME NOT NULL,
    date DATE NOT NULL
);
 
Двигайтесь дальше! Ваш код: tIVtQGEgoL
 
--------------------------------------
 
CREATE TABLE dds.dm_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    restaurant_id INTEGER NOT NULL,
    timestamp_id INTEGER NOT NULL,
    order_key VARCHAR NOT NULL UNIQUE,
    order_status VARCHAR NOT NULL
);
 
Двигайтесь дальше! Ваш код: wgfELwIQ0E
 
--------------------------------------
 
ALTER TABLE dds.dm_orders
ADD CONSTRAINT fk_user
FOREIGN KEY (user_id) REFERENCES dds.dm_users(id);
 
ALTER TABLE dds.dm_orders
ADD CONSTRAINT fk_restaurant
FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);
 
ALTER TABLE dds.dm_orders
ADD CONSTRAINT fk_timestamp
FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id);
 
Двигайтесь дальше! Ваш код: FyKdrGMogs
 
--------------------------------------
CREATE TABLE dds.fct_product_sales (
    id SERIAL PRIMARY KEY, -- Суррогатный первичный ключ
    product_id INTEGER NOT NULL, -- Идентификатор продукта (внешний ключ)
    order_id INTEGER NOT NULL,   -- Идентификатор заказа (внешний ключ)
    count INTEGER NOT NULL DEFAULT 0 CHECK (count >= 0), -- Количество продукта с проверкой
    price NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (price >= 0), -- Цена продукта с проверкой
    total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (total_sum >= 0), -- Общая сумма с проверкой
    bonus_payment NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (bonus_payment >= 0), -- Оплата бонусами с проверкой
    bonus_grant NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (bonus_grant >= 0) -- Начисленные бонусы с проверкой
);
 
Двигайтесь дальше! Ваш код: udZMKdxadM
 
--------------------------------------
 
ALTER TABLE dds.fct_product_sales
ADD CONSTRAINT fk_product
FOREIGN KEY (product_id) REFERENCES dds.dm_products(id);
 
ALTER TABLE dds.fct_product_sales
ADD CONSTRAINT fk_order
FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);
 
Двигайтесь дальше! Ваш код: NVyO1i2Dr5
 
-----------------------------------------
 
INSERT INTO cdm.dm_settlement_report (
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
SELECT 
    do2.restaurant_id,
    r.restaurant_name,
    dt.date, -- Укажите желаемую дату для отчетного периода если необходимо
    COUNT(distinct do2.order_key) AS orders_count,
    SUM(fps.total_sum) AS orders_total_sum,
    SUM(fps.bonus_payment) AS orders_bonus_payment_sum,
    SUM(fps.bonus_grant) AS orders_bonus_granted_sum,
    SUM(fps.total_sum) * 0.25 AS order_processing_fee,
    SUM(fps.total_sum) - (SUM(fps.total_sum) * 0.25) - SUM(fps.bonus_payment) AS restaurant_reward_sum
FROM dds.dm_orders do2
left join dds.dm_timestamps dt on dt.id = do2.timestamp_id
LEFT JOIN dds.fct_product_sales fps ON fps.order_id = do2.id
LEFT JOIN dds.dm_restaurants r ON r.id = do2.restaurant_id
WHERE do2.order_status = 'CLOSED'
GROUP BY do2.restaurant_id, r.restaurant_name, dt.date
ON CONFLICT (restaurant_id, settlement_date)
DO UPDATE SET 
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
    
Двигайтесь дальше! Ваш код: mgXgcqQzFv
 
-----------------------------------------
 
Двигайтесь дальше! Ваш код: gESZ89Tpop