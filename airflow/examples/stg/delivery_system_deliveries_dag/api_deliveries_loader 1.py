class DeliveryLoader:
    WF_KEY = "delivery_deliveries_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveryOriginRepository()
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            while True:
                load_queue = self.origin.list_deliveries()  # Получаем данные о доставках
                self.log.info(f"Found {len(load_queue)} deliveries to load.")
                
                if not load_queue:
                    self.log.info("No more deliveries to load. Quitting.")
                    break  # Выходим из цикла, если нет данных для загрузки

                for delivery in load_queue:
                    self.stg.insert_delivery(conn, delivery)  # Загружаем каждую доставку

                # Обновляем последний загруженный ID
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.d_delivery_id for t in load_queue])

                # Сохраняем обновленные настройки
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")