from logging import Logger
from typing import List
from datetime import datetime

from examples.dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel



class ProductObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: str
    restaurant_id: str
    active_from: datetime
    active_to: datetime


class ProductOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, user_threshold: int, limit: int) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """
                        SELECT 
                           --ROW_NUMBER() OVER (ORDER BY menu_item->>'_id') AS id
                           COALESCE(nextval('dds.dm_products_id_seq')-1, (SELECT MAX(id) FROM dds.dm_products)) AS id
                           --nextval('dds.dm_products_id_seq')-1 AS id
                          ,menu_item->>'_id' AS product_id
                          ,menu_item->>'name' AS product_name
                          ,(menu_item->>'price')::numeric AS product_price
                          ,r.id AS restaurant_id
                          --,TO_CHAR(r.update_ts, 'YYYY-MM-DD HH24:MI:SS') AS active_from
                          ,r.update_ts::timestamp  AS active_from
                          ,'2099-12-31 00:00:00.000'::timestamp AS active_to
                        FROM 
                        stg.ordersystem_restaurants r,
                        jsonb_array_elements(r.object_value::jsonb->'menu') AS menu_item
                        WHERE id > %(threshold)s
                        --ORDER BY id ASC
                        ORDER BY menu_item->>'_id' ASC
;
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class ProductDestRepository:
    def insert_products(self, conn: Connection, Product: ProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(id, product_id, product_name, product_price, restaurant_id, active_from, active_to)
                    VALUES (%(id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(restaurant_id)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    --ON CONFLICT (product_id) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id;
                """,
                {
                    "id": Product.id,
                    "product_id": Product.product_id,
                    "product_name": Product.product_name,
                    "product_price": Product.product_price,
                    "restaurant_id": Product.restaurant_id,
                    "active_from": Product.active_from,
                    "active_to": Product.active_to
                },
            )

class DDSProductLoader:
    WF_KEY = "dm_products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductOriginRepository(pg_origin)
        self.dds = ProductDestRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_products(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for user in load_queue:
                self.dds.insert_products(conn, user)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            #wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.product_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")