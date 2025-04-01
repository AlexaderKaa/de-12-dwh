SELECT id, ts, "year", "month", "day", "time", "date"
FROM dds.dm_timestamps;

                    SELECT DISTINCT
                      COALESCE(TO_CHAR(update_ts, 'YYYY-MM-DD HH24:MI:SS')::text, '') AS ts_ts,
                      EXTRACT(YEAR FROM update_ts)::int AS ts_year,
                      EXTRACT(MONTH FROM update_ts)::int AS ts_month,
                      EXTRACT(DAY FROM update_ts)::int AS ts_day,
                      COALESCE(TO_CHAR(update_ts, 'YYYY-MM-DD')::text, '') AS ts_date,
                      COALESCE(TO_CHAR(update_ts, 'HH24:MI:SS')::text, '') AS ts_time
                      FROM stg.ordersystem_orders
                      WHERE object_value::JSONB->> 'final_status' IN ('CANCELLED', 'CLOSED')
                      ;
                    
CREATE TABLE dds.dm_timestamps (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP NOT NULL,
    year SMALLINT CHECK (year >= 2022 AND year < 2500) NOT NULL,
    month SMALLINT CHECK (month >= 1 AND month <= 12) NOT NULL,
    day SMALLINT CHECK (day >= 1 AND day <= 31) NOT NULL,
    time TIME NOT NULL,
    date DATE NOT NULL,
    UNIQUE (ts, year, month, day)  -- Добавлено уникальное ограничение
);

ALTER TABLE dds.dm_timestamps
ADD CONSTRAINT unique_ts UNIQUE (ts);


                   SELECT
                      id                     
                      ,(SELECT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id') AS user_id
                      ,(SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id') AS restaurant_id
                      ,(SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp) AS timestamp_id
                      ,ou.object_id AS order_key
                      ,ou.object_value::JSONB->>'final_status' AS order_status
                    FROM 
                    stg.ordersystem_orders ou