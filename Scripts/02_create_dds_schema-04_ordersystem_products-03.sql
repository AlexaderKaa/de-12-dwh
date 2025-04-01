SELECT id, product_id, product_name, product_price, restaurant_id, active_from, active_to
FROM dds.dm_products;

/*
В STG данные о заказе лежат в формате JSON. Поэтому логика шага может выглядеть таким образом:
Прочитать данные из таблицы в STG.
Преобразовать JSON к объекту.
Найти версию ресторана по id и update_ts.
Вставить объекты с учётом историчности SCD2.
*/

INSERT INTO dds.dm_products (product_id, product_name, product_price, restaurant_id, active_from, active_to)
SELECT 
    menu_item->>'_id' AS product_id,
    menu_item->>'name' AS product_name,
    (menu_item->>'price')::numeric AS product_price,
    r.id AS restaurant_id,  -- Здесь нужно найти соответствующий restaurant_id
    r.update_ts AS active_from,
    '2099-12-31 00:00:00.000'::timestamp AS active_to
FROM 
    stg.ordersystem_restaurants r,
    jsonb_array_elements(r.object_value::jsonb->'menu') AS menu_item
    ;

SELECT 
                          --(SELECT "last_value" FROM dds.dm_products_id_seq) AS id,
						(select setval('dds.dm_products_id_seq', (select min(dds.dm_products.id) from dds.dm_products))) AS id
                        ,nextval('dds.dm_products_id_seq') AS id2
						--ROW_NUMBER() OVER (ORDER BY menu_item->>'_id') AS id
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
ORDER BY 
    product_id;  -- или другой критерий сортировки, если необходимо


SELECT "last_value" FROM dds.dm_products_id_seq
select setval('dds.dm_products_id_seq', (select max(dds.dm_products.id) from dds.dm_products));
