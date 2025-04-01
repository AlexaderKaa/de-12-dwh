import logging

import pendulum
from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from lib import ConnectionBuilder
from examples.stg.bonus_system_events_dag.event_loader import EventLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Запуск каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала
    catchup=False,  # Не выполнять пропущенные запуски
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги
    is_paused_upon_creation=True  # DAG сразу запущен
)
def stg_bonus_system_events_dag():

    @task(task_id="events_load")
    def events_load():
        # Подключение к базе DWH
        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

        # Подключение к базе подсистемы бонусов
        origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
        log = LoggingMixin().log

        loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        loader.load_events()

    events_load_task = events_load()

    # Добавьте другие задачи в DAG, если необходимо
    # users_load_task = ...

    # Определите порядок выполнения задач
    events_load_task  # >> users_load_task


stg_bonus_system_events_dag = stg_bonus_system_events_dag()
