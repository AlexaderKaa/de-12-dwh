import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.cdm.dm_courier_ledger_fill_dag.courier_ledger_loader import CDMCourierLedgerLoader


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/30 * * * *',  # Запуск каждые 20 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала
    catchup=False,  # Не выполнять пропущенные запуски
    tags=['sprint5', 'cdm', 'origin', 'example', 'project'],  # Теги
    is_paused_upon_creation=True  # DAG сразу запущен
)
def cdm_dm_courier_ledger_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Таск для загрузки данных
    @task(task_id="cdm_load_dm_courier_ledger")
    def cdm_load_dm_courier_ledger():
        # Создаем экземпляр 
        cdm_dm_courier_ledger_loader = CDMCourierLedgerLoader(origin_pg_connect, dwh_pg_connect, log)
        # Загружаем данные
        cdm_dm_courier_ledger_loader.load_dm_courier_ledger()

    # Инициализируем таск
    dm_courier_ledger_dict = cdm_load_dm_courier_ledger()
    dm_courier_ledger_dict  # type: ignore


# Инициализируем DAG
cdm_dm_courier_ledger_dag = cdm_dm_courier_ledger_dag()