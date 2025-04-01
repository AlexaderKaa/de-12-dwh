CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
	id int4 GENERATED ALWAYS AS IDENTITY NOT NULL,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);
