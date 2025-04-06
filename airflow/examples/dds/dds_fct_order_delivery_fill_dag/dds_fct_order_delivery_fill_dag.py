import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.dds.dds_fct_order_delivery_fill_dag.dds_order_delivery_loader import DDSOrderDeliveryLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/30 * * * *',  # Запуск каждые 30 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала
    catchup=False,  # Не выполнять пропущенные запуски
    tags=['sprint5', 'dds', 'origin', 'example', 'project', 'fct'],  # Теги
    is_paused_upon_creation=True  # DAG сразу запущен
)
def dds_fct_order_delivery_fill_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Таск для загрузки данных
    @task(task_id="dds_load_order_deliveries")
    def dds_load_fct_order_delivery():
        # Создаем экземпляр DeliveryLoader
        dds_order_delivery_loader = DDSOrderDeliveryLoader(origin_pg_connect, dwh_pg_connect, log)
        # Загружаем данные
        dds_order_delivery_loader.load_order_deliveries()

    # Инициализируем таск
    fct_order_delivery_dict = dds_load_fct_order_delivery()
    fct_order_delivery_dict  # type: ignore


# Инициализируем DAG
dds_fct_order_delivery_fill_dag = dds_fct_order_delivery_fill_dag()