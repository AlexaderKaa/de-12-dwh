SELECT id, workflow_key, workflow_settings
FROM stg.srv_wf_settings;


-- delivery_deliveries_origin_to_stg_workflow
UPDATE stg.srv_wf_settings SET workflow_settings ='{"last_loaded_id": 0}' WHERE id = 4012;
	