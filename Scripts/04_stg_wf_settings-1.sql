SELECT id, workflow_key, workflow_settings
FROM stg.srv_wf_settings;


-- delivery_deliveries_origin_to_stg_workflow
-- UPDATE stg.srv_wf_settings SET workflow_settings ='{"last_loaded_id": 0}' WHERE id = 4012;

SELECT id, courier_id, courier_name
FROM dds.dm_couriers;

--TRUNCATE TABLE dds.dm_couriers;

SELECT id, workflow_key, workflow_settings
FROM dds.srv_wf_settings;


-- delivery_deliveries_origin_to_stg_workflow
--UPDATE dds.srv_wf_settings SET workflow_settings ='{"last_loaded_id": 0}' WHERE id = 2480;
--UPDATE dds.srv_wf_settings SET workflow_settings ='{"last_loaded_id": 0}' WHERE id = 2558;

--DELETE FROM dds.srv_wf_settings  WHERE id = 2480;

SELECT id, workflow_key, workflow_settings
FROM cdm.srv_wf_settings;

-- UPDATE cdm.srv_wf_settings SET workflow_settings ='{"last_loaded_id": 0}' WHERE id = 123;
	