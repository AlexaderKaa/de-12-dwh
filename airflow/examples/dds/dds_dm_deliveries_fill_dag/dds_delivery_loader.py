from logging import Logger
from typing import List

from examples.dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel



class DeliveryObj(BaseModel):
   # id: int
    order_id: str
    delivery_id: str
    courier_id: str
    courier_rate: int
    order_sum: int
    order_tip_sum: int


class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, user_threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT 
                      order_id::text AS order_id
                      , delivery_id::text AS delivery_id
                      , courier_id::text AS courier_id
                      , rate::numeric AS courier_rate
                      , sum::numeric AS order_sum
                      , tip_sum::numeric AS order_tip_sum
                    FROM stg.delivery_system_deliveries
                    WHERE delivery_id > %(threshold)s
                    ORDER BY delivery_id ASC
                    LIMIT %(limit)s;  
                """, {
                    "threshold": str(user_threshold),  # Преобразуем в строку
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveriesDestRepository:
    def insert_deliveries(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(order_id, delivery_id, courier_id, courier_rate, order_sum, order_tip_sum)
                    VALUES (%(order_id)s, %(delivery_id)s, %(courier_id)s, %(courier_rate)s, %(order_sum)s, %(order_tip_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id,
                        courier_rate = EXCLUDED.courier_rate,
                        order_sum = EXCLUDED.order_sum,
                        order_tip_sum = EXCLUDED.order_tip_sum
                    ;
                """,
                {
                    #"id": delivery.id,
                    "order_id": delivery.order_id,
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "courier_rate": delivery.courier_rate,
                    "order_sum": delivery.order_sum,
                    "order_tip_sum": delivery.order_tip_sum
                },
            )

class DDSDeliveryLoader:
    WF_KEY = "dm_deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository(pg_origin)
        self.dds = DeliveriesDestRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery in load_queue:
                self.dds.insert_deliveries(conn, delivery)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.delivery_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")