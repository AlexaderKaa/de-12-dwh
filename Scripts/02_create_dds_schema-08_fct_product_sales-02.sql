SELECT id, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant
FROM dds.fct_product_sales;

--TRUNCATE dds.fct_product_sales;
--select setval('dds.fct_product_sales_id_seq', 1)

SELECT *
FROM information_schema.table_constraints
WHERE table_name = 'fct_product_sales' AND constraint_type IN ('UNIQUE', 'PRIMARY KEY');

INSERT INTO dds.fct_product_sales SELECT * FROM (
                 WITH bs_ev AS 
                   (
                     SELECT
                       id AS bs_id -- event_ts, event_type, event_value
                       ,(s.event_value::jsonb ->> 'order_id') AS bs_order_id  
                       ,(jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'product_id') AS bs_product_id
                       ,(jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'quantity')::numeric AS bs_cnt
                       ,(jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'price')::numeric AS bs_price
                       ,(jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'bonus_payment')::numeric AS bonus_payment
                       ,(jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'bonus_grant')::numeric AS bonus_grant
                     FROM stg.bonussystem_events s
                     WHERE s.event_type = 'bonus_transaction'
                    )
                   SELECT DISTINCT ON (p.id, o.id)
                   --SELECT DISTINCT
                      --bs_id AS id
                     p.id AS product_id 
                     ,o.id AS order_id
                     ,bs_cnt AS count
                     ,bs_price AS price
                     ,bs_price * bs_cnt AS total_sum
                     ,bonus_payment
                     ,bonus_grant
                  FROM bs_ev
                  JOIN dds.dm_orders o ON o.order_key = bs_order_id
                  JOIN dds.dm_products p ON p.product_id = bs_product_id ) fct_sales
                  --ON CONFLICT (product_id, order_id) DO UPDATE
                  ON CONFLICT (id) DO UPDATE
                    SET 
                        product_id = EXCLUDED.product_id,
                        order_id = EXCLUDED.order_id,
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant
                        ;




WITH bs_ev AS (
    SELECT
        (s.event_value::jsonb ->> 'order_id') AS bs_order_id,
        (jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'product_id') AS bs_product_id,
        (jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'quantity')::numeric AS bs_cnt,
        (jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'price')::numeric AS bs_price,
        (jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'bonus_payment')::numeric AS bonus_payment,
        (jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'bonus_grant')::numeric AS bonus_grant
    FROM stg.bonussystem_events s
    WHERE s.event_type = 'bonus_transaction'
)
SELECT
    p.id AS product_id,
    o.id AS order_id,
    COUNT(*) AS count
FROM bs_ev
JOIN dds.dm_orders o ON o.order_key = bs_order_id
JOIN dds.dm_products p ON p.product_id = bs_product_id
GROUP BY p.id, o.id
HAVING COUNT(*) > 1;


INSERT INTO dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
SELECT DISTINCT ON (product_id, order_id)
   product_id,
   order_id,
   count,
   price,
   total_sum,
   bonus_payment,
   bonus_grant
FROM (
    WITH bs_ev AS (
        SELECT
            id AS bs_id,
            (s.event_value::jsonb ->> 'order_id') AS bs_order_id,
            (jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'product_id') AS bs_product_id,
            (jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'quantity')::numeric AS bs_cnt,
            (jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'price')::numeric AS bs_price,
            (jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'bonus_payment')::numeric AS bonus_payment,
            (jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'bonus_grant')::numeric AS bonus_grant
        FROM stg.bonussystem_events s
        WHERE s.event_type = 'bonus_transaction'
    )
    SELECT
        p.id AS product_id,
        o.id AS order_id,
        bs_cnt AS count,
        bs_price AS price,
        bs_price * bs_cnt AS total_sum,
        bonus_payment,
        bonus_grant
    FROM bs_ev
    JOIN dds.dm_orders o ON o.order_key = bs_order_id
    JOIN dds.dm_products p ON p.product_id = bs_product_id
) fct_sales
ORDER BY product_id, order_id
ON CONFLICT (id) DO UPDATE
SET 
    product_id = EXCLUDED.product_id,
    order_id = EXCLUDED.order_id,
    count = EXCLUDED.count,
    price = EXCLUDED.price,
    total_sum = EXCLUDED.total_sum,
    bonus_payment = EXCLUDED.bonus_payment,
    bonus_grant = EXCLUDED.bonus_grant;













