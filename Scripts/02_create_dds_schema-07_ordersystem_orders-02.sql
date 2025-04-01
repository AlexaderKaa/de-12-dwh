SELECT id, user_id, restaurant_id, timestamp_id, order_key, order_status
FROM dds.dm_orders;


                    SELECT 
                      id                     
                      ,(SELECT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id') AS user_id
                      ,(SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id') AS restaurant_id
                      ,(SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp) AS timestamp_id
                      ,ou.object_id AS order_key
                      ,ou.object_value::JSONB->>'final_status' AS order_status
                    FROM 
                    stg.ordersystem_orders ou
                    ;

                    INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
                    SELECT 
                       (SELECT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id') AS user_id
                      ,(SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id') AS restaurant_id
                      ,(SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp) AS timestamp_id
                      ,ou.object_id AS order_key
                      ,ou.object_value::JSONB->>'final_status' AS order_status
                    FROM 
                    stg.ordersystem_orders ou
                                        ON CONFLICT (ts) DO UPDATE
                    --ON CONFLICT (id) DO UPDATE
                    SET 
                        ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time
                    



                    INSERT INTO dds.dm_orders (user_id , restaurant_id, timestamp_id, order_key, order_status)
                    SELECT 
                       (SELECT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id') AS user_id
                      ,(SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id') AS restaurant_id
                      ,(SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp) AS timestamp_id
                      ,ou.object_id AS order_key
                      ,ou.object_value::JSONB->>'final_status' AS order_status
                    FROM 
                    stg.ordersystem_orders ou
                    --WHERE TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp IS NOT NULL

                    
SELECT COUNT(*) FROM dds.dm_timestamps;

SELECT  t.ts , COUNT(*) FROM dds.dm_timestamps t GROUP BY t.ts HAVING COUNT(*)>1;
SELECT  * FROM dds.dm_timestamps t WHERE t.ts IN ('2025-03-15 21:10:22.000','2025-03-15 20:50:22.000','2025-03-15 20:55:28.000');

WITH dm_ord  AS (
                    SELECT 
                       (SELECT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id') AS user_id
                      ,(SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id') AS restaurant_id
                      ,(SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp) AS timestamp_id
                      ,ou.object_id AS order_key
                      ,ou.object_value::JSONB->>'final_status' AS order_status
                    FROM 
                    stg.ordersystem_orders ou
                 ) 
SELECT COUNT(*) FROM dm_ord;

SELECT COUNT(*) FROM stg.ordersystem_orders;

SELECT DISTINCT object_id FROM stg.ordersystem_orders;
SELECT DISTINCT object_value FROM stg.ordersystem_orders;


INSERT INTO dds.dm_orders (order_key, order_status, restaurant_id, timestamp_id, user_id)
SELECT DISTINCT 
    o.object_id AS order_key,
    o.object_value::json->>'final_status' AS order_status,
    r.id AS restaurant_id,
    t.id AS timestamp_id
  --  u.id AS user_id
FROM stg.ordersystem_orders o
CROSS JOIN LATERAL (
    SELECT (jsonb_path_query_first(o.object_value::jsonb, '$.statuses[*] ? (@.status == "CLOSED" || @.status == "CANCELLED").dttm') #>> '{}')::TIMESTAMP AS ts
) AS extracted_ts
JOIN dds.dm_timestamps t  ON t.ts = extracted_ts.ts
--LEFT JOIN dds.dm_restaurants r ON r.restaurant_id = (o.object_value::json->'restaurant'->'id'->>'$oid')::VARCHAR
--LEFT JOIN dds.dm_restaurants r ON r.restaurant_id = (o.object_value::jsonb->'restaurant'->'id')::VARCHAR
JOIN dds.dm_restaurants r ON r.restaurant_id = o.object_value::JSONB->'restaurant'->>'id'
--JOIN dds.dm_users u       ON u.user_id = (o.object_value::json->'user'->'id'/*->>'$oid'*/)::VARCHAR
JOIN dds.dm_users u       ON u.user_id = o.object_value::jsonb->'user'->>'id'
--WHERE r.id IS NOT NULL
WHERE NOT EXISTS (
    SELECT 1 
    FROM dds.dm_orders d
    WHERE d.order_key = o.object_id
    AND d.restaurant_id = r.id
    AND d.timestamp_id = t.id
    AND d.user_id = u.id
);