from logging import Logger
from typing import List
from datetime import datetime

from examples.dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderObj(BaseModel):
    id: int
    u_id: int
    r_id: int
    ts_id: datetime
    order_key: str
    order_status: str


class OrderOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, user_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    SELECT
                      id,
                      (SELECT id FROM dds.dm_users WHERE user_id = ou.object_value::JSONB->'user'->>'id') AS user_id,
                      (SELECT id FROM dds.dm_restaurants WHERE restaurant_id = ou.object_value::JSONB->'restaurant'->>'id') AS restaurant_id,
                      (SELECT id FROM dds.dm_timestamps WHERE ts = TO_CHAR(ou.update_ts, 'YYYY-MM-DD HH24:MI:SS')::timestamp) AS timestamp_id,
                      ou.object_id AS order_key,
                      ou.object_value::JSONB->>'final_status' AS order_status
                    FROM 
                    stg.ordersystem_orders ou
                    WHERE id > %(threshold)s
                    ORDER BY id DESC
                    LIMIT %(limit)s;  -- Добавлено ограничение
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrderDestRepository:
    def insert_orders(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s)
                    ON CONFLICT (order_key) DO UPDATE
                    SET 
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        order_status = EXCLUDED.order_status;
                """,
                {
                    "user_id": order.u_id,
                    "restaurant_id": order.r_id,
                    "timestamp_id": order.ts_id,
                    "order_key": order.order_key,
                    "order_status": order.order_status
                },
            )


class DDSOrderLoader:
    WF_KEY = "dm_orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrderOriginRepository(pg_origin)
        self.dds = OrderDestRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for order in load_queue:
                self.dds.insert_orders(conn, order)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(order.id for order in load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")