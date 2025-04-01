SELECT id, workflow_key, workflow_settings
FROM dds.srv_wf_settings;

--DELETE FROM  dds.srv_wf_settings WHERE id = 30;

--UPDATE dds.srv_wf_settings SET workflow_settings = ' ' WHERE id = 25;

--UPDATE dds.srv_wf_settings SET workflow_settings= '{"last_loaded_id": 0}' WHERE id = 41;
--UPDATE dds.srv_wf_settings SET workflow_settings= '{"last_loaded_id": 0}' WHERE id = 16;
--UPDATE dds.srv_wf_settings SET workflow_settings= '{"last_loaded_id": 0}' WHERE id = 264;

--DELETE FROM dds.dm_timestamps ;


--DELETE FROM dds.dm_products ;


                        SELECT 
                           --ROW_NUMBER() OVER (ORDER BY menu_item->>'_id') AS id
                          --,COALESCE(currval('dds.dm_products_id_seq'), nextval('dds.dm_products_id_seq')) AS id
                          COALESCE(currval('dds.dm_products_id_seq'), nextval('dds.dm_products_id_seq')) AS id
                          ,menu_item->>'_id' AS product_id
                          ,menu_item->>'name' AS product_name
                          ,(menu_item->>'price')::numeric AS product_price
                          ,r.id AS restaurant_id
                          --,TO_CHAR(r.update_ts, 'YYYY-MM-DD HH24:MI:SS') AS active_from
                          ,r.update_ts::timestamp  AS active_from
                          ,'2099-12-31 00:00:00.000'::timestamp AS active_to
                        FROM 
                        stg.ordersystem_restaurants r,
                        jsonb_array_elements(r.object_value::jsonb->'menu') AS menu_item
                        --WHERE id > %(threshold)s
                        --ORDER BY id ASC
                        ORDER BY menu_item->>'_id' ASC
;


                       WITH menu_items AS (
                                            SELECT 
                                              menu_item->>'_id' AS product_id,
                                              menu_item->>'name' AS product_name,
                                              (menu_item->>'price')::numeric AS product_price,
                                              r.id AS restaurant_id,
                                              r.update_ts::timestamp AS active_from,
                                              '2099-12-31 00:00:00.000'::timestamp AS active_to
                                            FROM 
                                            stg.ordersystem_restaurants r,
                                            jsonb_array_elements(r.object_value::jsonb->'menu') AS menu_item
                                            )
                        SELECT 
                          nextval('dds.dm_products_id_seq') AS id,
                          product_id,
                          product_name,
                          product_price,
                          restaurant_id,
                          active_from,
                          active_to
                        FROM 
                          menu_items
                        WHERE id > %(threshold)s
                        ORDER BY 
                          product_id;
                       
--   FROM DAG
                        SELECT 
                           --ROW_NUMBER() OVER (ORDER BY menu_item->>'_id') AS id
                           nextval('dds.dm_products_id_seq') AS id
                          ,menu_item->>'_id' AS product_id
                          ,menu_item->>'name' AS product_name
                          ,(menu_item->>'price')::numeric AS product_price
                          ,r.id AS restaurant_id
                          --,TO_CHAR(r.update_ts, 'YYYY-MM-DD HH24:MI:SS') AS active_from
                          ,r.update_ts::timestamp  AS active_from
                          ,'2099-12-31 00:00:00.000'::timestamp AS active_to
                        FROM 
                        stg.ordersystem_restaurants r,
                        jsonb_array_elements(r.object_value::jsonb->'menu') AS menu_item
                       -- WHERE id > %(threshold)s
                        --ORDER BY id ASC
                        ORDER BY menu_item->>'_id' ASC