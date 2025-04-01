import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.stg.bonus_system_users_dag.users_loader import UserLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Запуск каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала
    catchup=False,  # Не выполнять пропущенные запуски
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги
    is_paused_upon_creation=True  # DAG сразу запущен
)
def stg_bonus_system_users_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Таск для загрузки данных
    @task(task_id="users_load")
    def load_users():
        # Создаем экземпляр UserLoader
        user_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        # Загружаем данные
        user_loader.load_users()

    # Инициализируем таск
    users_dict = load_users()
    users_dict  # type: ignore


# Инициализируем DAG
stg_bonus_system_users_dag = stg_bonus_system_users_dag()