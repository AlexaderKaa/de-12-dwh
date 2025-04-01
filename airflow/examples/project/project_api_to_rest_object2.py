import requests
from typing import List

class RestaurantObj:
    def __init__(self, r_id: str, r_name: str):
        self.r_id = r_id
        self.r_name = r_name

    def __repr__(self):
        return f"RestaurantObj(id={self.r_id}, order_user_id={self.r_name})"

def get_report_response(nickname, cohort, sort_field='id', sort_direction='desc', limit=50, offset=0):
    try:
        response = requests.get(
            f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}",
            headers={
                "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
                "X-Nickname": nickname,
                "X-Cohort": cohort
            }
        )

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Произошла ошибка при выполнении запроса: {e}")
        return None

class RestAPIOriginRepository:
    def __init__(self, nickname: str, cohort: str) -> None:
        self.nickname = nickname
        self.cohort = cohort

    def list_restaurants(self, limit: int, offset: int) -> List[RestaurantObj]:
        data = get_report_response(self.nickname, self.cohort, limit=limit, offset=offset)

        if data is None:
            return []

        restaurants = [RestaurantObj(r_id=r['_id'], r_name=r['name']) for r in data]
        return restaurants

# Пример использования
nickname = 'nik'
cohort = '1'
limit = 50
offset = 0

repository = RestAPIOriginRepository(nickname, cohort)

while True:
    restaurants = repository.list_restaurants(limit, offset)

    print(f"Получено {len(restaurants)} записей на странице с offset {offset}")

    if not restaurants:
        break

    offset += limit
    
    print(restaurants)

