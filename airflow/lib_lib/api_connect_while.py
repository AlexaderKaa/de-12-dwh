import requests
from airflow.hooks.base import BaseHook

class FetchDeliveryData:
    def __init__(self, http_conn_id, nickname, cohort, endpoint, sort_field='id', sort_direction='desc', limit=50):
        self.http_conn_id = http_conn_id
        self.nickname = nickname
        self.cohort = cohort
        self.endpoint = endpoint
        self.sort_field = sort_field
        self.sort_direction = sort_direction
        self.limit = limit
        self.offset = 0
        self.api_key = self.get_api_key()
        self.nickname = self.get_api_nickname()
        self.cohort = self.get_api_cohort()
        self.base_url = self.get_base_url()

    def get_api_key(self):
        connection = BaseHook.get_connection(self.http_conn_id)
        return connection.extra_dejson.get('api_key')

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
        try:
            while True:
                # Выполняем запрос
                response = requests.get(
                    f"{self.base_url}/{self.endpoint}?sort_field={self.sort_field}&sort_direction={self.sort_direction}&limit={self.limit}&offset={self.offset}",
                    headers={
                        "X-API-KEY": self.api_key,
                        "X-Nickname": self.nickname,
                        "X-Cohort": self.cohort
                    }
                )

                # Проверяем, успешен ли запрос
                response.raise_for_status()  # Это вызовет исключение для ошибок HTTP

                # Преобразуем ответ в JSON
                data = response.json()
                print(f"Получено {len(data)} записей на странице с offset {self.offset}")

                # Проверяем, что data не None и является списком
                if data is None:
                   print("Получен None от API, выходим из цикла.")
                   break

                if not isinstance(data, list):
                   print(f"Ожидался список, но получен объект типа {type(data)}: {data}")
                   break

                   print(f"Получено {len(data)} записей на странице с offset {self.offset}")

                # Если данных нет, выходим из цикла
                if not data:
                    break

                # Обрабатываем полученные данные
                #print(data)

                # Увеличиваем offset для следующей порции данных
                self.offset += self.limit

        except requests.exceptions.RequestException as e:
            print(f"Произошла ошибка при выполнении запроса: {e}")


"""
# Пример использования
if __name__ == "__main__":
    fetcher = FetchDeliveryData(http_conn_id='http_conn_id', nickname='nik', cohort='1', endpoint='restaurants')
    fetcher.fetch_data()"
"""