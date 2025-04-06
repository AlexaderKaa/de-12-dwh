CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id serial NOT NULL PRIMARY KEY,
    courier_id varchar NOT NULL,
    courier_name varchar NOT NULL,
    --settlement_year date NOT NULL,
	settlement_year INT NOT NULL,
    --settlement_month date NOT NULL,
	settlement_month INT NOT NULL,
    orders_count integer NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    rate_avg numeric(14, 2) NOT NULL CHECK (rate_avg >= 0 AND rate_avg <= 5),
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    total_payment numeric(14, 2) NOT NULL DEFAULT 0 CHECK (total_payment >= 0),
    CONSTRAINT cdm_courier_ledger_courier_id UNIQUE (courier_id, settlement_year, settlement_month)
);

/*
               WITH orders_data AS (
                    SELECT
                      fod.id_order 
                      ,fod.courier_id
                      , fod.courier_name
                      , ts.year AS settlement_year
                      , ts.month AS settlement_month
                      , COUNT(fod.id) OVER (PARTITION BY fod.courier_id, ts.month  ORDER BY ts.month) AS orders_count
                      , SUM(fod.order_sum) AS orders_total_sum
                      , AVG(fod.courier_rate) AS rate_avg
                      , SUM(fod.order_tip_sum) AS courier_tips_sum
                    FROM
                        dds.fct_order_delivery fod
                    JOIN
                        dds.dm_timestamps ts ON fod.ts_id_order = ts.id
                    GROUP BY
                        fod.id,
                        ts.year, ts.month
                ),
                courier_payment_calcs AS (
                    SELECT
                     -- ROW_NUMBER() OVER () AS id,
                       od.courier_id
                      , od.courier_name
                      , od.settlement_year
                      , od.settlement_month
                      , od.orders_count
                      , od.orders_total_sum
                      , od.rate_avg
                      , (od.orders_total_sum * 0.25) AS order_processing_fee
                      -- Расчет суммы, которую необходимо перечислить курьеру за доставленные заказы
                      , CASE 
                          WHEN od.rate_avg < 4 THEN GREATEST(0.05 * od.orders_total_sum, 100)
                          WHEN od.rate_avg >= 4 AND od.rate_avg < 4.5 THEN GREATEST(0.07 * od.orders_total_sum, 150)
                          WHEN od.rate_avg >= 4.5 AND od.rate_avg < 4.9 THEN GREATEST(0.08 * od.orders_total_sum, 175)
                          ELSE GREATEST(0.10 * od.orders_total_sum, 200)
                        END AS courier_order_sum
                        -- Сумма чаевых с учетом комиссии
                       , (SUM(courier_tips_sum) * 0.95) AS courier_reward_sum
                    FROM orders_data od
                    GROUP BY  
                      od.courier_id, od.courier_name, od.settlement_year, od.settlement_month
                      ,od.orders_count, od.orders_total_sum, od.rate_avg
                )
                SELECT 
                  --id,
                  courier_id
                  , courier_name
                  , settlement_year
                  , settlement_month
                  , COUNT(orders_count) AS orders_count
                  , SUM(orders_total_sum) AS orders_total_sum
                  , AVG(rate_avg) AS rate_avg
                  , SUM(order_processing_fee) AS order_processing_fee
                  -- Общая сумма к выплате курьеру с учетом чаевых и комиссии
                  ,SUM( courier_order_sum + courier_reward_sum) AS total_payment 
                FROM 
                    courier_payment_calcs
                --WHERE courier_id > CAST(%(threshold)s AS text)
                GROUP BY  
                  courier_id, courier_name, settlement_year, settlement_month
                ORDER BY courier_id ASC
                --LIMIT %(limit)s  
;
*/