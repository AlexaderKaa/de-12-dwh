CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id serial NOT NULL PRIMARY KEY,
    restaurant_id varchar NOT NULL,
    restaurant_name varchar NOT NULL,
    settlement_date date NOT NULL,
    orders_count integer NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    orders_bonus_payment_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_payment_sum >= 0),
    orders_bonus_granted_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_bonus_granted_sum >= 0),
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    restaurant_reward_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (restaurant_reward_sum >= 0),
    CONSTRAINT dm_settlement_report_settlement_date_check CHECK (EXTRACT(YEAR FROM settlement_date) >= 2022 AND EXTRACT(YEAR FROM settlement_date) < 2500),
    CONSTRAINT unique_restaurant_period UNIQUE (restaurant_id, settlement_date)
);