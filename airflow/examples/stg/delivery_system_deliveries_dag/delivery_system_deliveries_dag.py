import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.stg.delivery_system_deliveries_dag.api_deliveries_loader import DeliveryLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Запуск каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала
    catchup=False,  # Не выполнять пропущенные запуски
    tags=['sprint5', 'stg', 'origin', 'project'],  # Теги
    is_paused_upon_creation=True  # DAG сразу запущен
)
def stg_delivery_system_deliveries_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    #origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Таск для загрузки данных
    @task(task_id="delivery_deliveries_load")
    def load_deliveries():
        # Создаем экземпляр DeliveryLoader
        delivery_loader = DeliveryLoader(dwh_pg_connect, log)
        # Загружаем данные
        delivery_loader.load_deliveries()

    # Инициализируем таск
    deliveries_dict = load_deliveries()
    deliveries_dict  # type: ignore


# Инициализируем DAG
stg_delivery_system_deliveries_dag = stg_delivery_system_deliveries_dag()