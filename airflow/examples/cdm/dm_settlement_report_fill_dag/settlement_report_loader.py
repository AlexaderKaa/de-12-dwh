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


class SettlementReportObj(BaseModel):
    id: int
    restaurant_id: int
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: Decimal
    orders_bonus_payment_sum: Decimal
    orders_bonus_granted_sum: Decimal
    order_processing_fee: Decimal
    restaurant_reward_sum: Decimal


class SettlementReportOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_settlement_report(self, user_threshold: int, limit: int) -> List[SettlementReportObj]:
        with self._db.client().cursor(row_factory=class_row(SettlementReportObj)) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (dt.date, do2.restaurant_id, r.restaurant_name)
                    COALESCE((SELECT MAX(id) FROM cdm.dm_settlement_report), nextval('cdm.dm_settlement_report_id_seq')-1) AS id,
                    do2.restaurant_id,
                    r.restaurant_name,
                    dt.date AS settlement_date, 
                    COUNT(distinct do2.order_key) AS orders_count,
                    COALESCE(SUM(fps.total_sum), 0) AS orders_total_sum,
                    COALESCE(SUM(fps.bonus_payment), 0) AS orders_bonus_payment_sum,
                    COALESCE(SUM(fps.bonus_grant), 0) AS orders_bonus_granted_sum,
                    COALESCE(SUM(fps.total_sum) * 0.25, 0) AS order_processing_fee,
                    COALESCE(SUM(fps.total_sum) - (SUM(fps.total_sum) * 0.25) - SUM(fps.bonus_payment), 0) AS restaurant_reward_sum
                FROM dds.dm_orders do2
                left join dds.dm_timestamps dt on dt.id = do2.timestamp_id
                LEFT JOIN dds.fct_product_sales fps ON fps.order_id = do2.id
                LEFT JOIN dds.dm_restaurants r ON r.id = do2.restaurant_id
                WHERE do2.order_status = 'CLOSED'
                GROUP BY do2.restaurant_id, r.restaurant_name, dt.date
                --ORDER BY do2.restaurant_id, dt.date 
                ORDER BY dt.date DESC, do2.restaurant_id DESC, r.restaurant_name
                --WHERE id > %(threshold)s                 
                --LIMIT %(limit)s
                 ;
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SettlementReportDestRepository:
    def insert_dm_settlement_report(self, conn: Connection, SettlementReport: SettlementReportObj) -> None:
        with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO cdm.dm_settlement_report 
                      (id, restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum
                      , orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES 
                      (%(id)s, %(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, %(orders_total_sum)s
                      , %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s)
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE 
                    --ON CONFLICT (id) DO UPDATE
                    SET 
                    restaurant_id = EXCLUDED.restaurant_id,
                    settlement_date = EXCLUDED.settlement_date,
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
                    ;
                    """,
                    {
                        "id": SettlementReport.id,
                        "restaurant_id": SettlementReport.restaurant_id,
                        "restaurant_name": SettlementReport.restaurant_name,
                        "settlement_date": SettlementReport.settlement_date,
                        "orders_count": SettlementReport.orders_count,
                        "orders_total_sum": SettlementReport.orders_total_sum,
                        "orders_bonus_payment_sum": SettlementReport.orders_bonus_payment_sum,
                        "orders_bonus_granted_sum": SettlementReport.orders_bonus_granted_sum,
                        "order_processing_fee": SettlementReport.order_processing_fee,
                        "restaurant_reward_sum": SettlementReport.restaurant_reward_sum
                    }
                )


class CDMSettlementReportLoader:
    WF_KEY = "cdm_dm_settlement_report_dds_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SettlementReportOriginRepository(pg_origin)
        self.cdm = SettlementReportDestRepository()
        self.settings_repository = CDMEtlSettingsRepository()
        self.log = log

    def load_dm_settlement_report(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dm_settlement_report(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} data to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for user in load_queue:
                self.cdm.insert_dm_settlement_report(conn, user)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")