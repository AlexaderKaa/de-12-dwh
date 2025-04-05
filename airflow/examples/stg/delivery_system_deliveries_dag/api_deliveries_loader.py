from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib import api_connect
from api_connect import FetchDeliveryData
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveryObj(BaseModel):
    d_order_id: str
    d_order_ts: str
    d_delivery_id: str
    d_order_ts: str
    d_courier_id: str
    d_address: str
    d_delivery_ts: str
    d_rate: str
    d_sum: str
    d_tip_sum: str

class DeliveryOriginRepository:
    def list_deliveries(self) -> List[DeliveryObj]:
        deliveries_data = FetchDeliveryData('http_conn_id','nikname', 'cohort', endpoint='deliveries')
        data = deliveries_data.fetch_data()  # Предполагается, что это возвращает список словарей
        # Преобразуем каждый словарь в объект DeliveryRestaurantObj
        objs = [DeliveryObj(
                            d_order_id=d['order_id'],
                            d_order_ts=d['order_ts'],
                            d_delivery_id=d['delivery_id'],
                            d_courier_id=d['courier_id'],
                            d_address=d['address'],
                            d_delivery_ts=d['delivery_ts'],
                            d_rate=d['rate'],
                            d_sum=d['sum'],
                            d_tip_sum=d['tip_sum']
                            ) for d in data]
        print(objs)
        return objs


class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, delivery_order: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.delivery_system_deliveries(order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
                    VALUES (%(order_id)s, %(order_ts)s, %(delivery_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(sum)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum
                        ;
                """,
                {
                    "order_id": delivery_order.d_order_id,
                    "order_ts": delivery_order.d_order_ts,
                    "delivery_id": delivery_order.d_delivery_id,
                    "courier_id": delivery_order.d_courier_id,
                    "address": delivery_order.d_address,
                    "delivery_ts": delivery_order.d_delivery_ts,
                    "rate": delivery_order.d_rate,
                    "sum": delivery_order.d_sum,
                    "tip_sum": delivery_order.d_tip_sum,
                },
            )


class DeliveryLoader:
    WF_KEY = "delivery_deliveries_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    #BATCH_LIMIT = 1000

    #def __init__(self, pg_dest: PgConnect, log: Logger, base_url: str, api_key: str) -> None:
    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveryOriginRepository()
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries()
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.d_delivery_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")