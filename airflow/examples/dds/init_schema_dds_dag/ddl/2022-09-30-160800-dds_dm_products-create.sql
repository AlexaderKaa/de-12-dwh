CREATE TABLE IF NOT EXISTS dds.dm_products
(
    id SERIAL PRIMARY KEY,                        -- Суррогатный первичный ключ
    product_id VARCHAR NOT NULL,                  -- Идентификатор продукта
    product_name VARCHAR NOT NULL,                -- Название продукта
    product_price NUMERIC(14, 2) DEFAULT 0 NOT NULL 
        CHECK (product_price >= 0),               -- Цена продукта с ограничением на значение
    restaurant_id INTEGER NOT NULL,               -- Идентификатор ресторана
    active_from TIMESTAMP NOT NULL,               -- Дата начала действия записи
    active_to TIMESTAMP NOT NULL,                 -- Дата окончания действия записи
    CONSTRAINT dm_products_restaurant_id_fkey
        FOREIGN KEY (restaurant_id)
        REFERENCES dds.dm_restaurants (id)       -- Внешний ключ на таблицу ресторанов
);

/*
CREATE TABLE IF NOT EXISTS dds.dm_products
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

ALTER TABLE dds.dm_products
   ADD CONSTRAINT dm_products_restaurant_id_fkey
   FOREIGN KEY (restaurant_id)
   REFERENCES dds.dm_restaurants (id);
*/