from logging import Logger
from typing import List, Optional
from decimal import Decimal
from datetime import date, datetime

from examples.cdm import EtlSetting, CDMEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierLedgerObj(BaseModel):
   #id: int
   courier_id: str
   courier_name: str
   #settlement_year: date
   settlement_year: int
   #settlement_month: date
   settlement_month: int
   orders_count: int
   orders_total_sum: Decimal
   rate_avg: Decimal
   order_processing_fee: Decimal
   total_payment: Decimal


class CourierLedgerOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_courier_ledger(self, user_threshold: str, limit: int) -> List[CourierLedgerObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgerObj)) as cur:
            cur.execute(
                """
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
                WHERE courier_id > CAST(%(threshold)s AS text)
                GROUP BY  
                  courier_id, courier_name, settlement_year, settlement_month
                ORDER BY courier_id ASC
                LIMIT %(limit)s  
                ;
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class CourierLedgerDestRepository:
    def insert_courier_ledger(self, conn: Connection, CourierLedger: CourierLedgerObj) -> None:
        with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.dm_courier_ledger 
                      (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum
                      , rate_avg, order_processing_fee, total_payment)
                    VALUES 
                      (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s
                      , %(rate_avg)s, %(order_processing_fee)s, %(total_payment)s)
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE 
                    SET 
                      courier_name = EXCLUDED.courier_name,
                      --settlement_year = EXCLUDED.settlement_year,
                      --settlement_month = EXCLUDED.settlement_month,
                      orders_count = EXCLUDED.orders_count,
                      orders_total_sum = EXCLUDED.orders_total_sum,
                      rate_avg = EXCLUDED.rate_avg,
                      order_processing_fee = EXCLUDED.order_processing_fee,
                      total_payment = EXCLUDED.total_payment
                    ;
                    """,
                    {
                        #"id": CourierLedger.id,
                        "courier_id": CourierLedger.courier_id,
                        "courier_name": CourierLedger.courier_name,
                        "settlement_year": CourierLedger.settlement_year,
                        "settlement_month": CourierLedger.settlement_month,
                        "orders_count": CourierLedger.orders_count,
                        "orders_total_sum": CourierLedger.orders_total_sum,
                        "rate_avg": CourierLedger.rate_avg,
                        "order_processing_fee": CourierLedger.order_processing_fee,
                        "total_payment": CourierLedger.total_payment
                    }
                )


class CDMCourierLedgerLoader:
    WF_KEY = "cdm_dm_courier_ledger_dds_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierLedgerOriginRepository(pg_origin)
        self.cdm = CourierLedgerDestRepository()
        self.settings_repository = CDMEtlSettingsRepository()
        self.log = log

    def load_dm_courier_ledger(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_courier_ledger(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} data to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for courier_ledger in load_queue:
                self.cdm.insert_courier_ledger(conn, courier_ledger)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.courier_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")