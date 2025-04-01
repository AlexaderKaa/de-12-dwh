-- В первую очередь необходимо узнать, какие таблицы есть в схеме public источника.

SELECT *
FROM pg_catalog.pg_tables
WHERE schemaname = 'public'; 


SELECT *
FROM information_schema.tables
WHERE table_schema = 'public'; 

-- Выберите запрос, который отобразит строки с информацией только об ограничениях внешних ключей в схеме public.

SELECT *
FROM information_schema.domain_constraints; 

--- верный
SELECT CONSTRAINT_SCHEMA,
       CONSTRAINT_NAME,
       update_rule,
       delete_rule
FROM information_schema.referential_constraints
WHERE CONSTRAINT_SCHEMA = 'public'; 


SELECT *
FROM information_schema.constraint_table_usage
WHERE table_schema = 'public'; 