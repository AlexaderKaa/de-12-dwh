CREATE TABLE IF NOT EXISTS dds.dm_users
(
    id SERIAL PRIMARY KEY,              -- Суррогатный первичный ключ
    user_id varchar NOT NULL,           -- Идентификатор пользователя из MongoDB
    user_name varchar NOT NULL,            -- Имя пользователя
    user_login varchar NOT NULL             -- Логин пользователя
);