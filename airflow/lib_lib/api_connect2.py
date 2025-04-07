import requests

class FetchDeliveryData:
    def __init__(self, base_url, api_key, nickname, cohort, endpoint, sort_field='id', sort_direction='desc', limit=50):
        self.base_url = base_url
        self.api_key = api_key
        self.nickname = nickname
        self.cohort = cohort
        self.endpoint = endpoint
        self.sort_field = sort_field
        self.sort_direction = sort_direction
        self.limit = limit
        self.offset = 0

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

                # Если данных нет, выходим из цикла
                if not data:
                    break

                # Обрабатываем полученные данные
                print(data)

                # Увеличиваем offset для следующей порции данных
                self.offset += self.limit

        except requests.exceptions.RequestException as e:
            print(f"Произошла ошибка при выполнении запроса: {e}")

# Пример использования
if __name__ == "__main__":
    # Задайте значения для base_url и api_key
    base_url = 'https://example.com/api'  # Замените на ваш базовый URL
    api_key = 'your_api_key'  # Замените на ваш API ключ
    fetcher = FetchDeliveryData(base_url=base_url, api_key=api_key, nickname='nik', cohort='1', endpoint='restaurants')
    fetcher.fetch_data()