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

"""
class DeliveryRestaurantObj:
    def __init__(self, r_id: str, r_name: str):
        self.r_id = r_id
        self.r_name = r_name

    def __repr__(self):
        return f"DeliveryRestaurantObj(id={self.r_id}, name={self.r_name})"
"""
class DeliveryRestaurantObj(BaseModel):
    r_id: str
    r_name: str

class DeliveryRestaurantOriginRepository:
    #def __init__(self, base_url: str, api_key: str):
    #def __init__(self):
        #self.base_url = base_url
        #self.api_key = api_key
    #http_conn_id = HttpHook.get_connection('http_conn_id')
    
    def list_delivery_restaurants(self) -> List[DeliveryRestaurantObj]:
        restaurants_data = FetchDeliveryData('http_conn_id','nikname', 'cohort', endpoint='restaurants')
        data = restaurants_data.fetch_data()  # Предполагается, что это возвращает список словарей
        # Преобразуем каждый словарь в объект DeliveryRestaurantObj
        objs = [DeliveryRestaurantObj(r_id=restaurant['_id'], r_name=restaurant['name']) for restaurant in data]
        print(objs)
        return objs


class DeliveryRestaurantDestRepository:
    def insert_delivery_restaurant(self, conn: Connection, delivery_restaurant: DeliveryRestaurantObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.delivery_system_restaurants(object_id, object_name)
                    VALUES (%(object_id)s, %(object_name)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_name = EXCLUDED.object_name;
                """,
                {
                    "object_id": delivery_restaurant.r_id,
                    "object_name": delivery_restaurant.r_name
                },
            )


class DeliveryRestaurantLoader:
    WF_KEY = "delivery_restaurants_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1

    #def __init__(self, pg_dest: PgConnect, log: Logger, base_url: str, api_key: str) -> None:
    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        #self.origin = DeliveryRestaurantOriginRepository(base_url, api_key)
        self.origin = DeliveryRestaurantOriginRepository()
        self.stg = DeliveryRestaurantDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_delivery_restaurants(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_delivery_restaurants()
            self.log.info(f"Found {len(load_queue)} delivery restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery_restaurant in load_queue:
                self.stg.insert_delivery_restaurant(conn, delivery_restaurant)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.r_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")