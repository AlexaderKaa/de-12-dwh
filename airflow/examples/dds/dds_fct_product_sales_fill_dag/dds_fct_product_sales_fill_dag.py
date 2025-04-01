import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.dds.dds_fct_product_sales_fill_dag.product_sales_loader import DDSProductSaleLoader


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/20 * * * *',  # Запуск каждые 20 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала
    catchup=False,  # Не выполнять пропущенные запуски
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги
    is_paused_upon_creation=True  # DAG сразу запущен
)
def dds_fct_product_sales_fill_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Таск для загрузки данных
    @task(task_id="dds_load_product_sales")
    def dds_load_product_sales():
        # Создаем экземпляр UserLoader
        dds_product_sale_loader = DDSProductSaleLoader(origin_pg_connect, dwh_pg_connect, log)
        #dds_Product_sale_loader = DDSProduct_saleLoader(dwh_pg_connect, log)
        # Загружаем данные
        dds_product_sale_loader.load_product_sales()

    # Инициализируем таск
    Product_sales_dict = dds_load_product_sales()
    Product_sales_dict  # type: ignore


# Инициализируем DAG
dds_fct_product_sales_fill_dag = dds_fct_product_sales_fill_dag()