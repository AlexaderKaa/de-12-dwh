SELECT currval('dds.dm_products_id_seq'), "last_value",  log_cnt, is_called
FROM dds.dm_products_id_seq;

select setval('dds.dm_products_id_seq', (select min(dds.dm_products.id) from dds.dm_products))

select setval('dds.dm_products_id_seq', 1)


                        SELECT 
                           --ROW_NUMBER() OVER (ORDER BY menu_item->>'_id') AS id
                          --,COALESCE(currval('dds.dm_products_id_seq'), nextval('dds.dm_products_id_seq')) AS id
                          COALESCE(nextval('dds.dm_products_id_seq')-1, (SELECT MAX(id) FROM dds.dm_products)) AS id
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
