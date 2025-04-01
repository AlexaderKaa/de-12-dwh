import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.dds.dds_dm_orders_fill_dag.dds_orders_loader import DDSOrderLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Запуск каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала
    catchup=False,  # Не выполнять пропущенные запуски
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги
    is_paused_upon_creation=True  # DAG сразу запущен
)
def dds_dm_orders_fill_dag_new():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Таск для загрузки данных
    @task(task_id="dds_load_orders")
    def dds_load_orders():
        # Создаем экземпляр UserLoader
        dds_order_loader = DDSOrderLoader(origin_pg_connect, dwh_pg_connect, log)
        #dds_order_loader = DDSOrderLoader(dwh_pg_connect, log)
        # Загружаем данные
        dds_order_loader.load_orders()

    # Инициализируем таск
    orders_dict = dds_load_orders()
    orders_dict  # type: ignore


# Инициализируем DAG
dds_dm_orders_fill_dag_new = dds_dm_orders_fill_dag_new()