SELECT COUNT(*)
FROM dds.dm_users;

SELECT DISTINCT u.user_id FROM dds.dm_users u;

SELECT id, object_id as user_id, user_name, user_login
                    FROM stg.ordersystem_users;

SELECT 
    id, 
    MIN(object_id) AS user_id, 
    MIN(user_name) AS user_name, 
    MIN(user_login) AS user_login
FROM stg.ordersystem_users
GROUP BY id;

SELECT DISTINCT ON(object_id) id, object_id AS user_id, user_name, user_login
FROM stg.ordersystem_users;
                    
SELECT * FROM dds.dm_users u;

--DELETE FROM dds.dm_users u;

SELECT COUNT(*) FROM dds.dm_timestamps ;


/*
В STG данные о заказе лежат в формате JSON. Поэтому логика шага может выглядеть таким образом:
Прочитать данные из таблицы в STG.
Преобразовать JSON к объекту.
Найти версию ресторана по id и update_ts.
Найти версию пользователя по id и update_ts.
Найти временную метку по date в заказе.
*/
SELECT DISTINCT id, object_id as user_id, user_name, user_login
FROM stg.ordersystem_users