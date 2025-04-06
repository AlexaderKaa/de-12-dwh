/*
 Описание задачи
Вам необходимо построить витрину, содержащую информацию о выплатах курьерам.
Состав витрины:
id — идентификатор записи.

dds.dm_couriers
courier_id — ID курьера, которому перечисляем.
courier_name — Ф. И. О. курьера.

dds.dm_timestamps
settlement_year — год отчёта.
settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.

stg.delivery_system_deliveries -->> dds.dm_deliveries (id, order_id(order_key), delivery_id(key), courier_id(courier_key), rate, sum, tip_sum)
dds.dm_orders + dds.dm_deliveries + dds.dm_timestamps = 
dds.fct_order_delivery 
(
id
, order_ts_id -- from dds.dm_timestamps joined on dds.dm_orders
, order_id -- from dds.dm_orders
, delivery_id , courier_id,  rate, order_sum, order_tip_sum -- from dds.dm_deliveries
)

SELECT 
  ddo.id   AS id_order
  , ddt.id AS ts_id_order
  , ddd.id AS id_delivery
  , ddc.courier_id
  , ddc.courier_name
  , ddd.courier_rate
  , ddd.order_sum
  , ddd.order_tip_sum
FROM dds.dm_orders AS ddo
JOIN dds.dm_timestamps AS ddt ON ddt.id = ddo.timestamp_id
JOIN dds.dm_deliveries AS ddd ON ddd.order_id = ddo.order_key 
JOIN dds.dm_couriers   AS ddc ON ddc.courier_id = ddd.courier_id
;

orders_count — количество заказов за период (месяц).
orders_total_sum — общая стоимость заказов.
rate_avg — средний рейтинг курьера по оценкам пользователей.
order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. 

SELECT *  FROM stg.delivery_system_deliveries;
За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:
r < 4 — 5% от заказа, но не менее 100 р.;
4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
4.9 <= r — 10% от заказа, но не менее 200 р.

Данные о заказах уже есть в хранилище. Данные курьерской службы вам необходимо забрать из API курьерской службы, после чего совместить их с данными подсистемы заказов.
Отчёт собирается по дате заказа. Если заказ был сделан ночью и даты заказа и доставки не совпадают, 
в отчёте стоит ориентироваться на дату заказа, а не дату доставки. Иногда заказы, сделанные ночью до 23:59, доставляют на следующий день: дата заказа и доставки не совпадёт. 
Это важно, потому что такие случаи могут выпадать в том числе и на последний день месяца. Тогда начисление курьеру относите к дате заказа, а не доставки.
 */


SELECT COUNT(*) FROM stg.delivery_system_deliveries;
SELECT COUNT(*) FROM dds.dm_deliveries ;
SELECT COUNT(*) FROM dds.dm_orders do2 ;

SELECT * FROM stg.delivery_system_deliveries;
SELECT * FROM dds.dm_orders do2 ;
SELECT * FROM dds.dm_deliveries ;
SELECT * FROM dds.dm_timestamps dt ;
--SELECT * FROM dds.fct_product_sales ;


SELECT id, order_id, delivery_id, courier_id, courier_rate, order_sum, order_tip_sum
FROM dds.dm_deliveries;


SELECT stg.delivery_system_deliveries.delivery_id , COUNT(*) 
FROM stg.delivery_system_deliveries
GROUP BY delivery_id 
HAVING  COUNT(delivery_id) > 1 
;

SELECT stg.delivery_system_deliveries.order_id , COUNT(*) 
FROM stg.delivery_system_deliveries
GROUP BY order_id 
HAVING  COUNT(order_id) > 1 
;