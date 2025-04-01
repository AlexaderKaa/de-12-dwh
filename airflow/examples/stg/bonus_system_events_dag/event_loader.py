from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
import psycopg
import json
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class OutboxRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, last_loaded_id: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                SELECT id, event_ts, event_type, event_value
                FROM outbox
                WHERE id > %(last_loaded_id)s
                ORDER BY id ASC;
                """,
                {"last_loaded_id": last_loaded_id},
            )
            objs = cur.fetchall()
        return objs


class EventDestRepository:
    def insert_event(self, conn: Connection, event: EventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (id) DO NOTHING;  -- если уже есть, не вставляем дубликат
                """,
                (event.id, event.event_ts, event.event_type, event.event_value)
            )


class EventLoader:
    WF_KEY = "events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OutboxRepository(pg_origin)
        self.stg = EventDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log


    def load_events(self):
        with self.pg_dest.connection() as conn:
            # Получаем настройки ETL
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f"Last loaded ID: {last_loaded}")

            # Загружаем все события
            load_queue = self.origin.list_events(last_loaded)
            self.log.info(f"Found {len(load_queue)} events to load.")

            if not load_queue:
                self.log.info("No more events to load.")
                return

            # Логируем первые 5 событий для отладки
            for event in load_queue[:5]:
                self.log.info(f"Processing event: id={event.id}, event_ts={event.event_ts}, event_type={event.event_type}, event_value={event.event_value}")

            # Вставляем все события в целевую таблицу
            for event in load_queue:
                self.stg.insert_event(conn, event)

            # Обновляем last_loaded_id
            last_loaded = max([t.id for t in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Loaded up to id {last_loaded}")
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")