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

ALTER TABLE dds.dm_products
   ADD CONSTRAINT dm_products_restaurant_id_fkey
   FOREIGN KEY (restaurant_id)
   REFERENCES dds.dm_restaurants (id);
   