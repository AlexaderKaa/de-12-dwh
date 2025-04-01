SELECT id, object_id, object_value, update_ts
FROM stg.ordersystem_orders;

--DROP table stg.ordersystem_orders;
CREATE TABLE IF NOT exists stg.ordersystem_orders (
    id serial PRIMARY KEY,               -- Уникальный идентификатор записи типа serial
    object_id varchar NOT NULL,          -- Уникальный идентификатор документа из MongoDB
    object_value text NOT NULL,          -- Хранение всего документа из MongoDB
    update_ts timestamp NOT NULL,        -- Время обновления объекта из MongoDB
    CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id)  -- Уникальное ограничение на object_id
);



SELECT COUNT(*) FROM stg.ordersystem_orders;

SELECT TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp, ou.*  FROM stg.ordersystem_orders ou WHERE ou.id = 9232 OR ou.object_id  = '67dac5bf46876637744af3e9';

SELECT  t.object_id , COUNT(*) FROM stg.ordersystem_orders t GROUP BY t.object_id HAVING COUNT(*)>1;

/* Проверка заполнения таблицы `dds.dm_orders` Проверяющий запрос: */
                    SELECT
                        o.order_key AS order_key,
                        o.order_status AS order_status,
                        r.restaurant_id AS restaurant_id,
                        to_char(ts."date"::timestamptz(0), 'Mon-dd-YYYY,HH24:mi:ss') AS ts_date,
                        du.user_id AS user_id
                    FROM dds.dm_orders AS o
                        INNER JOIN dds.dm_restaurants AS r
                            ON o.restaurant_id = r.id
                        INNER JOIN dds.dm_timestamps AS ts
                            ON o.timestamp_id = ts.id
                        INNER JOIN dds.dm_users AS du
                            ON o.user_id = du.id
                    WHERE
                        ts.ts::date BETWEEN (now() AT TIME ZONE 'utc')::date - 2 AND (now() AT TIME ZONE 'utc')::date - 1