import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.cdm.dm_settlement_report_fill_dag.settlement_report_loader import CDMSettlementReportLoader


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/20 * * * *',  # Запуск каждые 20 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала
    catchup=False,  # Не выполнять пропущенные запуски
    tags=['sprint5', 'cdm', 'origin', 'example'],  # Теги
    is_paused_upon_creation=True  # DAG сразу запущен
)
def cdm_dm_settlement_report_fill_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Таск для загрузки данных
    @task(task_id="cdm_load_dm_settlement_report")
    def cdm_load_dm_settlement_report():
        # Создаем экземпляр 
        cdm_dm_settlement_report_loader = CDMSettlementReportLoader(origin_pg_connect, dwh_pg_connect, log)
        # Загружаем данные
        cdm_dm_settlement_report_loader.load_dm_settlement_report()

    # Инициализируем таск
    dm_settlement_report_dict = cdm_load_dm_settlement_report()
    dm_settlement_report_dict  # type: ignore


# Инициализируем DAG
cdm_load_dm_settlement_report = cdm_dm_settlement_report_fill_dag()