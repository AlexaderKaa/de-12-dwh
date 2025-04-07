import requests
import logging
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

class FetchDeliveryData:
    def __init__(self, http_conn_id, nickname, cohort, endpoint, sort_field='id', sort_direction='desc', limit=50):
        self.http_conn_id = http_conn_id
        self.nickname = self.get_api_nickname()
        self.cohort = self.get_api_cohort()
        self.endpoint = endpoint
        self.sort_field = sort_field
        self.sort_direction = sort_direction
        self.limit = limit
        self.api_key = self.get_api_key()
        self.base_url = self.get_base_url()

    def get_api_key(self):
        connection = BaseHook.get_connection(self.http_conn_id)
        api_key = connection.extra_dejson.get('api_key')
        if not api_key:
            logging.error("API key not found in connection extra.")
            raise ValueError("API key is required.")
        print(api_key)
        return api_key

    def get_base_url(self):
        connection = BaseHook.get_connection(self.http_conn_id)
        return connection.host

    def get_api_nickname(self):
        connection = BaseHook.get_connection(self.http_conn_id)
        return connection.extra_dejson.get('nickname')

    def get_api_cohort(self):
        connection = BaseHook.get_connection(self.http_conn_id)
        return connection.extra_dejson.get('cohort')

    def fetch_data(self):
        all_data = []
        offset = 0
        
        # Устанавливаем дату начала и конца для выборки данных за последние 7 дней
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        while True:
            try:
                # Выполняем запрос с параметрами
                params = {
                    "sort_field": self.sort_field,
                    "sort_direction": self.sort_direction,
                    "limit": self.limit,
                    "offset": offset,
                    "start_date": start_date.isoformat(),  # Добавляем параметры для фильтрации по дате
                    "#end_date": end_date.isoformat()
                }
                headers = {
                    "X-API-KEY": self.api_key,
                    "X-Nickname": self.nickname,
                    "X-Cohort": self.cohort
                }

                response = requests.get(f"{self.base_url}/{self.endpoint}", headers=headers, params=params)

                # Проверяем, успешен ли запрос
                response.raise_for_status()  # Это вызовет исключение для ошибок HTTP

                # Преобразуем ответ в JSON
                data = response.json()

                # Проверяем, что data не None и является списком
                if data is None:
                    logging.warning("Получен None от API.")
                    break

                if not isinstance(data, list):
                    logging.warning(f"Ожидался список, но получен объект типа {type(data)}: {data}")
                    break

                logging.info(f"Получено {len(data)} записей на странице с offset {offset}")

                if len(data) == 0:  # Если данных больше нет, выходим из цикла
                    break

                all_data.extend(data)  # Добавляем полученные данные в общий список
                
                offset += self.limit  # Увеличиваем offset на размер лимита для следующего запроса

            except requests.exceptions.RequestException as e:
                logging.error(f"Произошла ошибка при выполнении запроса: {e}")
                break

        return all_data  # Возвращаем все собранные данные

# Пример использования класса:
# fetcher = FetchDeliveryData(http_conn_id='your_http_conn_id', endpoint='your_endpoint')
# all_records = fetcher.fetch_data()