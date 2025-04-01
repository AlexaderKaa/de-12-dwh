                SELECT
                    o.order_key AS order_key,
                    dp.product_id AS product_id,
                    pr.total_sum,
                    pr.count,
                    pr.price,
                    pr.bonus_payment,
                    pr.bonus_grant
                FROM dds.fct_product_sales AS pr
                    INNER JOIN dds.dm_orders AS o
                        ON pr.order_id = o.id
                    INNER JOIN dds.dm_timestamps AS t
                        ON o.timestamp_id = t.id
                    INNER JOIN dds.dm_products AS dp
                        ON pr.product_id = dp.id
                --WHERE ts::date BETWEEN (now() AT TIME ZONE 'utc')::date - 2 AND (now() AT TIME ZONE 'utc')::date - 1
                ORDER BY ts DESC
                ;
                
CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
    id SERIAL PRIMARY KEY, -- Суррогатный первичный ключ
    product_id INTEGER NOT NULL, -- Идентификатор продукта (внешний ключ)
    order_id INTEGER NOT NULL,   -- Идентификатор заказа (внешний ключ)
    count INTEGER NOT NULL DEFAULT 0 CHECK (count >= 0), -- Количество продукта с проверкой
    price NUMERIC(19, 5) NOT NULL DEFAULT 0 CHECK (price >= 0), -- Цена продукта с проверкой
    total_sum NUMERIC(19, 5) NOT NULL DEFAULT 0 CHECK (total_sum >= 0), -- Общая сумма с проверкой
    bonus_payment NUMERIC(19, 5) NOT NULL DEFAULT 0 CHECK (bonus_payment >= 0), -- Оплата бонусами с проверкой
    bonus_grant NUMERIC(19, 5) NOT NULL DEFAULT 0 CHECK (bonus_grant >= 0), -- Начисленные бонусы с проверкой
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dds.dm_products(id), -- Внешний ключ на продукт
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id) -- Внешний ключ на заказ
);

--ALTER TABLE dds.fct_product_sales ADD CONSTRAINT unique_order_product UNIQUE (order_id, product_id);
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT unique_product UNIQUE (product_id);

ALTER TABLE dds.fct_product_sales ADD CONSTRAINT unique_ordert UNIQUE (order_id);

TRUNCATE TABLE dds.fct_product_sales;