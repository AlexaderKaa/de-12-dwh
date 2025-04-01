from logging import Logger
from typing import List, Optional
from datetime import date, datetime, time
from examples.dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime



class TimestampObj(BaseModel):
    id: Optional[int]
    ts_ts: Optional[str]
    ts_year: Optional[str]
    ts_month: Optional[str]
    ts_day: Optional[str]
    ts_date: Optional[date]    
    ts_time: Optional[str]   


class TimestampOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, user_threshold: int, limit: int) -> List[TimestampObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                    SELECT DISTINCT
                      id,
                      COALESCE(TO_CHAR(update_ts, 'YYYY-MM-DD HH24:MI:SS')::text, '') AS ts_ts,
                      EXTRACT(YEAR FROM update_ts)::int AS ts_year,
                      EXTRACT(MONTH FROM update_ts)::int AS ts_month,
                      EXTRACT(DAY FROM update_ts)::int AS ts_day,
                      COALESCE(TO_CHAR(update_ts, 'YYYY-MM-DD')::text, '') AS ts_date,
                      COALESCE(TO_CHAR(update_ts, 'HH24:MI:SS')::text, '') AS ts_time
                      FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s
                    AND object_value::JSONB->> 'final_status' IN ('CANCELLED', 'CLOSED')
                    --ORDER BY id ASC
                    ORDER BY id DESC
                    LIMIT %(limit)s;  
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TimestampDestRepository:
    def insert_timestamps(self, conn: Connection, ts_obj: TimestampObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(id, ts, year, month, day, date, time)
                    VALUES (%(id)s, %(ts_ts)s, %(ts_year)s, %(ts_month)s, %(ts_day)s, %(ts_date)s, %(ts_time)s)
                    --ON CONFLICT (ts) DO NOTHING
                    ON CONFLICT (ts) DO UPDATE
                    --ON CONFLICT (id) DO UPDATE
                    SET 
                        ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time
                    ;
                """,
                {
                    "id": ts_obj.id,
                    "ts_ts": ts_obj.ts_ts, 
                    "ts_year": ts_obj.ts_year, 
                    "ts_month": ts_obj.ts_month, 
                    "ts_day": ts_obj.ts_day, 
                    "ts_date": ts_obj.ts_date, 
                    "ts_time": ts_obj.ts_time                   
                },
            )

class DDSTimestampLoader:
    WF_KEY = "dm_timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimestampOriginRepository(pg_origin)
        self.dds = TimestampDestRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for user in load_queue:
                self.dds.insert_timestamps(conn, user)

            #wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.ts_ts for t in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")