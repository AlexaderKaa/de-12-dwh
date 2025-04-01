from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_object(self, conn: Connection, id: str, update_ts: datetime, val: Any):
        # Преобразуем значение в строку
        str_val = json2str(val)
        
        # Извлекаем необходимые данные из val
        name = val.get('name')
        #print(name)
        login = val.get('login')
        #print(name)
        
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.ordersystem_users(object_id, user_name, user_login, update_ts)
                    VALUES (%(id)s, %(name)s, %(login)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login,
                        update_ts = EXCLUDED.update_ts
                    ;
                """,
                {
                    "id": id,
                    "name": name,
                    "login": login,
                    "update_ts": update_ts
                }
            )
