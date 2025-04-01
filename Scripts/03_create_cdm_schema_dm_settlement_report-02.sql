SELECT id, restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum
FROM cdm.dm_settlement_report;


-- DELETE FROM cdm.dm_settlement_report;
SELECT id, workflow_key, workflow_settings
FROM cdm.srv_wf_settings;

-- ПРОВЕРКИ
SELECT
  restaurant_name,
  settlement_date,
  orders_count,
  orders_total_sum,
  orders_bonus_payment_sum,
  orders_bonus_granted_sum,
  order_processing_fee,
  restaurant_reward_sum
FROM cdm.dm_settlement_report
where
  (settlement_date::date >= (now()at time zone 'utc')::date - 3
AND
settlement_date::date <= (now()at time zone 'utc')::date - 1)
ORDER BY settlement_date desc;
