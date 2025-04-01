CREATE TABLE IF NOT EXISTS dds.dm_restaurants
(
    id SERIAL PRIMARY KEY,               -- Суррогатный первичный ключ
    restaurant_id VARCHAR NOT NULL, -- Идентификатор ресторана
    restaurant_name VARCHAR NOT NULL,-- Название ресторана
    active_from TIMESTAMP NOT NULL,      -- Дата начала действия записи
    active_to TIMESTAMP NOT NULL          -- Дата окончания действия записи
);