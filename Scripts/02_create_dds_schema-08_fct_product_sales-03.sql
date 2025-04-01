SELECT DISTINCT ON (product_id, order_id)
					 --COALESCE(nextval('dds.fct_product_sales_id_seq')-1, (SELECT MAX(id) FROM dds.fct_product_sales)) AS id,
					(SELECT MAX(id) FROM dds.fct_product_sales) AS id,
					COALESCE((SELECT MAX(id) FROM dds.fct_product_sales), nextval('dds.fct_product_sales_id_seq')-1) AS id2,
                     product_id,
                     order_id,
                     count,
                     price,
                     total_sum,
                     bonus_payment,
                     bonus_grant
                   FROM (
                         WITH bs_ev AS 
                         (
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