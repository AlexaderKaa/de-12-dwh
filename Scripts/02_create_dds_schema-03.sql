 
INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
WITH ord AS (
SELECT 
    (SELECT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id') AS user_id,  -- Извлечение user_id
    (SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id') AS restaurant_id,
    (SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp) AS timestamp_id,  -- Извлечение timestamp_id
    ou.object_id AS order_key,
    ou.object_value::JSONB->>'final_status' AS order_status
FROM 
    stg.ordersystem_orders ou
    )
    SELECT COUNT(*) FROM ord
    ;


INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
SELECT 
    (SELECT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id'),  -- Извлечение user_id
    (SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id'),
    (SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp),  -- Извлечение timestamp_id
    ou.object_id AS order_key,
    ou.object_value::JSONB->>'final_status' AS order_status
FROM 
    stg.ordersystem_orders ou


INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
SELECT 
    (SELECT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id') AS user_id,  -- Извлечение user_id
    --1 AS restaurant_id,
    (SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id') AS restaurant_id,
    (SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp) AS timestamp_id,  -- Извлечение timestamp_id
    --Извлечение timestamp_id
    ou.object_id AS order_key,
    ou.object_value::JSONB->>'final_status' AS order_status
FROM 
    stg.ordersystem_orders ou

SELECT id, ts, "year", "month", "day", "time", "date" FROM dds.dm_timestamps ORDER BY id;
    
SELECT 
DISTINCT
--du.id AS id_user
--dr.id AS id_restaurant
dts.id AS id_timestamp
,dts.ts AS ts_ts
,TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp
   --object_value::JSONB->'user'->>'id' AS user_id
   /* (SELECT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id') AS user_id  -- Извлечение user_id
    (SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id'),
    (SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp) AS timestamp_id,  -- Извлечение timestamp_id
    --Извлечение timestamp_id
    ou.object_id AS order_key,
    ou.object_value::JSONB->>'final_status' AS order_status*/
FROM stg.ordersystem_orders ou
JOIN dds.dm_users du ON object_value::JSONB->'user'->>'id'= du.user_id  
--JOIN dds.dm_restaurants dr ON ou.object_value::JSONB->'restaurant'->>'id' = dr.restaurant_id 
JOIN dds.dm_timestamps dts ON dts.ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp
ORDER BY 1
;

TRUNCATE TABLE dds.dm_orders ;

SELECT DISTINCT
    (SELECT DISTINCT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id') AS user_id  -- Извлечение user_id
    --1 AS restaurant_id,
    --,(SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id') AS restaurant_id,
    --,(SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp) AS timestamp_id,  -- Извлечение timestamp_id
    --Извлечение timestamp_id
    --,ou.object_id AS order_key,
    --,ou.object_value::JSONB->>'final_status' AS order_status
FROM 
    stg.ordersystem_orders ou
;
    