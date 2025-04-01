import requests
import psycopg2
import json
from psycopg2 import sql

# Настройки подключения к базе данных
db_config = {
    'dbname': 'your_database_name',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',  # или IP-адрес вашего сервера
    'port': '5432'  # стандартный порт PostgreSQL
}

nickname = 'nik'
cohort = '1'
sort_field = 'id'  # Замените на нужное поле для сортировки
sort_direction = 'desc'  # Замените на 'asc' для сортировки по возрастанию
limit = 50
offset = 0

# Подключаемся к базе данных
conn = None
cursor = None

try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    while True:
        # Выполняем запрос
        response = requests.get(
            f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}",
            headers={
                "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
                "X-Nickname": nickname,
                "X-Cohort": cohort
            }
        )

        # Проверяем, успешен ли запрос
        response.raise_for_status()  # Это вызовет исключение для ошибок HTTP

        # Преобразуем ответ в JSON
        data = response.json()
        print(f"Получено {len(data)} записей на странице с offset {offset}")

        # Преобразуем ответ в JSON
        data = get_report_response.json()
        print(data)

        # Если данных нет, выходим из цикла
        if not data:
            break

        # Сериализуем JSON в строку
        json_data = json.dumps(data)

        # Вставляем данные в таблицу
        insert_query = sql.SQL("INSERT INTO restaurants (data) VALUES (%s)")
        cursor.execute(insert_query, (json_data,))

        # Сохраняем изменения
        conn.commit()

        # Увеличиваем offset для следующей порции данных
        offset += limit

except requests.exceptions.RequestException as e:
    print(f"Произошла ошибка при выполнении запроса: {e}")
except psycopg2.Error as e:
    print(f"Произошла ошибка при работе с базой данных: {e}")
finally:
    # Закрываем соединение с базой данных
    if cursor:
        cursor.close()
    if conn:
        conn.close()