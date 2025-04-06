from logging import Logger
from typing import List

from examples.dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel



class OrderDeliveryObj(BaseModel):
   # id: int
    id_order: int
    ts_id_order: int
    id_delivery: int
    courier_id: str
    courier_name: str
    courier_rate: int
    order_sum: int
    order_tip_sum: int


class OrderDeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_order_deliveries(self, user_threshold: int, limit: int) -> List[OrderDeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(OrderDeliveryObj)) as cur:
            cur.execute(
                """
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
                    WHERE ddo.id > %(threshold)s
                    ORDER BY ddo.id ASC
                    LIMIT %(limit)s;  
                """, {
                    "threshold": str(user_threshold),  # Преобразуем в строку
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrderDeliveriesDestRepository:
    def insert_order_deliveries(self, conn: Connection, order_delivery: OrderDeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_order_delivery (id_order, ts_id_order, id_delivery, courier_id, courier_name, courier_rate, order_sum, order_tip_sum)
                    VALUES (%(id_order)s, %(ts_id_order)s, %(id_delivery)s, %(courier_id)s, %(courier_name)s, %(courier_rate)s, %(order_sum)s, %(order_tip_sum)s)
                    ON CONFLICT (id_order) DO UPDATE
                    SET
                        ts_id_order = EXCLUDED.ts_id_order,
                        id_delivery = EXCLUDED.id_delivery,
                        courier_id = EXCLUDED.courier_id,
                        courier_name = EXCLUDED.courier_name,
                        courier_rate = EXCLUDED.courier_rate,
                        order_sum = EXCLUDED.order_sum,
                        order_tip_sum = EXCLUDED.order_tip_sum
                    ;
                """,
                {
                    #"id": order_delivery.id,
                    "id_order": order_delivery.id_order,
                    "ts_id_order": order_delivery.ts_id_order,
                    "id_delivery": order_delivery.id_delivery,
                    "courier_id": order_delivery.courier_id,
                    "courier_name": order_delivery.courier_name,
                    "courier_rate": order_delivery.courier_rate,
                    "order_sum": order_delivery.order_sum,
                    "order_tip_sum": order_delivery.order_tip_sum
                },
            )

class DDSOrderDeliveryLoader:
    WF_KEY = "fct_order_deliveries_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrderDeliveriesOriginRepository(pg_origin)
        self.dds = OrderDeliveriesDestRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_order_deliveries(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_order_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} order deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for order_delivery in load_queue:
                self.dds.insert_order_deliveries(conn, order_delivery)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id_order for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")