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

class DeliveryCourierObj(BaseModel):
    c_id: str
    c_name: str


class DeliveryCourierOriginRepository:
    def list_delivery_couriers(self, offset: int) -> List[DeliveryCourierObj]:
        couriers_data = FetchDeliveryData('http_conn_id', 'nikname', 'cohort', endpoint='couriers', offset=offset)
        data = couriers_data.fetch_data()  # Предполагается, что это возвращает список словарей
        # Преобразуем каждый словарь в объект DeliveryCourierObj
        objs = [DeliveryCourierObj(c_id=courier['_id'], c_name=courier['name']) for courier in data]
        print(objs)
        return objs


class DeliveryCourierDestRepository:
    def insert_delivery_courier(self, conn: Connection, delivery_courier: DeliveryCourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.delivery_system_couriers(object_id, object_name)
                    VALUES (%(object_id)s, %(object_name)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_name = EXCLUDED.object_name;
                """,
                {
                    "object_id": delivery_courier.c_id,
                    "object_name": delivery_courier.c_name
                },
            )


class DeliveryCourierLoader:
    WF_KEY = "delivery_couriers_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # Установим лимит на 50 курьеров за раз

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveryCourierOriginRepository()
        self.stg = DeliveryCourierDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_delivery_couriers(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = int(wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])  # Преобразуем в int
            offset = last_loaded + 1  # Начинаем с последнего загруженного ID + 1
            
            while True:
                load_queue = self.origin.list_delivery_couriers(offset)
                self.log.info(f"Found {len(load_queue)} couriers to load.")
                
                if not load_queue:
                    self.log.info("No more couriers to load. Quitting.")
                    break

                for delivery_courier in load_queue:
                    self.stg.insert_delivery_courier(conn, delivery_courier)

                # Обновляем offset для следующей итерации
                offset += len(load_queue)

            if load_queue:
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([int(t.c_id) for t in load_queue])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")