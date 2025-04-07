#from examples.stg import EtlSetting, StgEtlSettingsRepository
from api_connect2 import FetchDeliveryData
from typing import List


class DeliveryRestaurantObj:
    def __init__(self, r_id: str, r_name: str):
        self.r_id = r_id
        self.r_name = r_name

    def __repr__(self):
        return f"DeliveryRestaurantObj(id={self.r_id}, order_user_id={self.r_name})"
    

# Пример использования
if __name__ == "__main__":
    # Задайте значения для base_url и api_key
    base_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'  # Замените на ваш базовый URL
    api_key = '25c27781-8fde-4b30-a22e-524044a7580f'
    #restaurants_data = FetchDeliveryData(base_url=base_url, api_key=api_key, nickname='aleksandr-k-vt', cohort='1', endpoint='restaurants')
    #restaurants_data.fetch_data()

    couriers_data = FetchDeliveryData(base_url=base_url, api_key=api_key, nickname='aleksandr-k-vt', cohort='1', endpoint='couriers')
    couriers_data.fetch_data()

    #deliveries_data = FetchDeliveryData(base_url=base_url, api_key=api_key, nickname='aleksandr-k-vt', cohort='1', endpoint='deliveries')
    #deliveries_data.fetch_data()


    def list_delivery_restaurants() -> List[DeliveryRestaurantObj]:
        restaurants_data = FetchDeliveryData(base_url=base_url, api_key=api_key, nickname='aleksandr-k-vt', cohort='1', endpoint='restaurants')
        objs = restaurants_data.fetch_data()
        return objs


    print(list_delivery_restaurants())
