WITH bs_ev AS 
(
SELECT 
  --,id event_ts, event_type, event_value
  (s.event_value::jsonb ->> 'order_id') AS bs_order_id  
  ,(jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'product_id') AS bs_product_id
  ,(jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'quantity')::numeric AS bs_cnt
  ,(jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'price')::numeric AS bs_price
  ,(jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'bonus_payment') AS bonus_payment
  ,(jsonb_array_elements(s.event_value::jsonb -> 'product_payments') ->> 'bonus_grant') AS bonus_grant
  FROM stg.bonussystem_events s
--JOIN dds.dm_products p ON dp.product_id = bs_ev.object_value::JSONB->'user'->>'id'
WHERE s.event_type = 'bonus_transaction'
)
SELECT 
  bs_product_id AS product_id
  ,bs_order_id AS order_key
  ,bs_cnt AS count
  ,bs_price AS price
  ,bs_price * bs_cnt AS total_sum
  ,bonus_payment
  ,bonus_grant
FROM bs_ev
JOIN dds.dm_orders o ON o.order_key = bs_order_id
JOIN dds.dm_products p ON p.product_id = bs_product_id 
;



{
"SELECT id, event_ts, event_type, event_value\nFROM stg.bonussystem_events": [
	{
		"id" : 750001,
		"event_ts" : "2025-03-07T18:10:23.001Z",
		"event_type" : "bonus_transaction",
		"event_value" : "
{\"user_id\": 46, \"order_id\": \"67cb60be61def47b6de90cfc\", \"order_date\": \"2025-03-07 21:10:22\", \"product_payments\": 
[{\"price\": 50, \"quantity\": 1, \"product_id\": \"2d614774b2d57fd0650a3129\", \"bonus_grant\": 0, \"product_cost\": 50, \"product_name\": \"Соус Сметана\", \"bonus_payment\": 50}, 
{\"price\": 560, \"quantity\": 1, \"product_id\": \"84544c10b6a1de9310cd0462\", \"bonus_grant\": 2, \"product_cost\": 560, \"product_name\": \"Оджахури с говядиной\", \"bonus_payment\": 544.0}, 
{\"price\": 240, \"quantity\": 3, \"product_id\": \"a8d54fb4abc3d87722d9ea26\", \"bonus_grant\": 72, \"product_cost\": 720, \"product_name\": \"Хинкали с сыром сулугуни 3 шт\", \"bonus_payment\": 0.0}, 
{\"price\": 300, \"quantity\": 2, \"product_id\": \"22e74a698d81d8b5275e2ba7\", \"bonus_grant\": 60, \"product_cost\": 600, \"product_name\": \"Борщ\", \"bonus_payment\": 0.0}, 
{\"price\": 100, \"quantity\": 4, \"product_id\": \"45c34a7eaea04e757faee967\", \"bonus_grant\": 40, \"product_cost\": 400, \"product_name\": \"Соус Наршараб\", \"bonus_payment\": 0.0}, 
{\"price\": 270, \"quantity\": 2, \"product_id\": \"564b4a2bb6e699b15549069c\", \"bonus_grant\": 54, \"product_cost\": 540, \"product_name\": \"Хинкали с говядиной и свининой 3 шт\", \"bonus_payment\": 0.0}, 
{\"price\": 450, \"quantity\": 3, \"product_id\": \"845d42f8af7705492d2d576a\", \"bonus_grant\": 135, \"product_cost\": 1350, \"product_name\": \"Хинкали с креветкой Том Ям 3 шт\", \"bonus_payment\": 0.0}, 
{\"price\": 270, \"quantity\": 4, \"product_id\": \"1c3744b29df6178f396fb4bd\", \"bonus_grant\": 108, \"product_cost\": 1080, \"product_name\": \"Хинкали с бараниной 3 шт\", \"bonus_payment\": 0.0}, 
{\"price\": 300, \"quantity\": 4, \"product_id\": \"dcfd472aa0f182837473d42d\", \"bonus_grant\": 120, \"product_cost\": 1200, \"product_name\": \"Куриный суп с лапшой\", \"bonus_payment\": 0.0}]}"
	},

/*
SELECT 
DISTINCT event_type
FROM stg.bonussystem_events bs_ev
*/
