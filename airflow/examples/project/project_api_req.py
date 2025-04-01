import requests

nickname = 'aleksandr-k-vt'
cohort = '1'
sort_field = 'id'  # Замените на нужное поле для сортировки
sort_direction = 'desc'  # Замените на 'asc' для сортировки по возрастанию
limit = 50
offset = 0

try:

    while True:
        # Выполняем запрос
        get_report_response = requests.get(
            f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}",
            headers={
                "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
                "X-Nickname": nickname,
                "X-Cohort": cohort
            }
        )

        # Проверяем, успешен ли запрос
        get_report_response.raise_for_status()  # Это вызовет исключение для ошибок HTTP

        # Преобразуем ответ в JSON
        data = get_report_response.json()
        print(f"Получено {len(data)} записей на странице с offset {offset}")

        # Преобразуем ответ в JSON
        data = get_report_response.json()
        print(data)

        # Если данных нет, выходим из цикла
        if not data:
            break
        # Увеличиваем offset для следующей порции данных
        offset += limit


except requests.exceptions.RequestException as e:
    print(f"Произошла ошибка при выполнении запроса: {e}")