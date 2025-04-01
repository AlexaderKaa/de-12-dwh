--DELETE FROM stg.ordersystem_users;

SELECT id, workflow_key, workflow_settings
FROM stg.srv_wf_settings;

--UPDATE stg.srv_wf_settings SET workflow_settings= '{"last_loaded_id": 0}' WHERE id = 4;

SELECT id, object_id, user_name, user_login, update_ts
FROM stg.ordersystem_users_bak;

SELECT id, object_id, user_name, user_login, update_ts
FROM stg.ordersystem_users;



--UPDATE stg.srv_wf_settings SET workflow_settings= '{"last_loaded_id": 0}' WHERE id = 4;