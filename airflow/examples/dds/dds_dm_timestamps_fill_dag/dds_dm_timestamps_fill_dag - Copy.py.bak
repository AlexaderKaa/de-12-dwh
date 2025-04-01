import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.dds.dds_dm_timestamps_fill_dag.dds_timestamps_loader import DDSTimestampLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Запуск каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала
    catchup=False,  # Не выполнять пропущенные запуски
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги
    is_paused_upon_creation=True  # DAG сразу запущен
)
def dds_dm_timestamps_fill_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Таск для загрузки данных
    @task(task_id="dds_load_timestamps")
    def dds_load_timestamps():
        # Создаем экземпляр UserLoader
        dds_timestamp_loader = DDSTimestampLoader(origin_pg_connect, dwh_pg_connect, log)
        # Загружаем данные
        dds_timestamp_loader.load_timestamps()

    # Инициализируем таск
    users_dict = dds_load_timestamps()
    users_dict  # type: ignore


# Инициализируем DAG
dds_dm_timestamps_fill_dag = dds_dm_timestamps_fill_dag()