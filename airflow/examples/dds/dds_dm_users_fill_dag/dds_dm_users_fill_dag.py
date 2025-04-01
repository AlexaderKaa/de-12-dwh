import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.dds.dds_dm_users_fill_dag.dds_user_loader import DDSUserLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Запуск каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала
    catchup=False,  # Не выполнять пропущенные запуски
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги
    is_paused_upon_creation=True  # DAG сразу запущен
)
def dds_dm_users_fill_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Таск для загрузки данных
    @task(task_id="dds_load_users")
    def dds_load_users():
        # Создаем экземпляр UserLoader
        dds_user_loader = DDSUserLoader(origin_pg_connect, dwh_pg_connect, log)
        # Загружаем данные
        dds_user_loader.load_users()

    # Инициализируем таск
    users_dict = dds_load_users()
    users_dict  # type: ignore


# Инициализируем DAG
dds_dm_users_fill_dag = dds_dm_users_fill_dag()