"""Locust tasks module for Superset ETL flow testing"""

import logging
import random
import time
import urllib3

from locust import task, between

from common.auth import establish_session
from common.api import Api
from common.csv_utils import count_chunks, count_csv_lines
from common.managers import UserPool
from config import CONFIG

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ProcessMetricsCalculator(Api):
    """Создание датасета и дашборда 'Расчет метрик Process Mining'"""

    wait_time = between(min_wait=1, max_wait=5)

    def __init__(self, parent):
        super().__init__(parent)
        self.user_id = f"user_{random.randint(10000, 99999)}"
        self.session_id = f"{random.randint(1000, 9999)}"
        self.logged_in = False
        self.session_valid = False
        self.total_chunks = count_chunks(CONFIG["csv_file_path"], CONFIG["chunk_size"])
        self.total_lines = count_csv_lines(CONFIG["csv_file_path"])
        self.worker_id = 0
        self.username = None
        self.password = None
        self.flow_id = None

    def establish_session(self):
        """Establish user session with authentication"""
        success = establish_session(
            client=self.client,
            username=self.username,
            password=self.password,
            session_id=self.session_id,
            log_function=self.log
        )

        if success:
            self.logged_in = True
            self.session_valid = True
            self.log(f"Authentication successful for {self.username}")
        else:
            self.log("Authentication failed", logging.ERROR)
            self.interrupt()

    def on_start(self):
        """Initialize user session and credentials"""
        runner = getattr(self, "environment", None)
        if runner:
            runner = getattr(runner, "runner", None)
            self.worker_id = getattr(runner, "worker_id", 0) if runner else 0

        creds = UserPool.get_credentials()
        self.username = creds["username"]
        self.password = creds["password"]
        self.client.verify = False
        self.establish_session()

        self.log("User started for dashboard metrics process mining")

    def on_stop(self):
        """Clean up when user stops"""
        self.log("User stopping")

    @task
    def create_and_upload_pm(self):
        """Основная задача: создание flow с загрузкой файла и отдельного PM flow"""

        if not self.logged_in:
            self.establish_session()
            if not self.logged_in:
                self.log("Failed to establish session", logging.ERROR)
                return

        self.log("Starting flow creation and upload process")

        try:
            # 1. Создание flow для загрузки файла
            flow_name, flow_id = self._create_flow(worker_id=self.worker_id)
            self.flow_id = flow_id

            if not flow_id:
                self.log("Failed to create flow", logging.ERROR)
                return

            self.log(f"File flow created: {flow_name} (ID: {flow_id})")

            # 2. Получение параметров DAG
            target_connection, target_schema = self._get_dag_import_params(flow_id)
            if not target_connection or not target_schema:
                self.log("Missing DAG parameters", logging.ERROR)
                return

            # 3. Обновление flow перед загрузкой
            update_resp = self._update_flow(
                flow_id,
                flow_name,
                target_connection,
                target_schema,
                file_uploaded=False,
                count_chunks_val=self.total_chunks,
            )
            if not update_resp or not update_resp.ok:
                self.log("Failed to update flow before upload", logging.ERROR)
                return

            # 4. Получение ID базы данных пользователя
            db_id = self._get_user_database_id()
            if not db_id:
                self.log("User database not found", logging.ERROR)
                return

            if self.total_chunks == 0:
                self.log("No chunks to upload", logging.WARNING)
                return

            timeout = (
                CONFIG["upload_control"]["timeout_large"]
                if self.total_chunks > CONFIG["upload_control"]["chunk_threshold"]
                else CONFIG["upload_control"]["timeout_small"]
            )

            # 5. Начало загрузки
            if not self._start_file_upload(flow_id, db_id, target_schema, self.total_chunks, timeout):
                return

            # 6. Загрузка чанков
            uploaded_chunks = self._upload_chunks(flow_id, db_id, target_schema, self.total_chunks)
            self.log(f"Chunk upload completed: {uploaded_chunks}/{self.total_chunks} chunks")

            # 7. Финализация загрузки
            if not self._finalize_file_upload(flow_id, uploaded_chunks, timeout):
                return

            # 8. Начало обработки
            file_run_id = self._start_file_processing(flow_id, target_connection, target_schema,
                                                 self.total_chunks, timeout)
            if not file_run_id:
                return

            # 9. Мониторинг статуса обработки файла
            file_processing_start = time.time()
            success = self._monitor_processing_status(
                file_run_id, timeout, flow_id, db_id, target_schema,
                self.total_lines, file_processing_start, is_pm_flow=False
            )

            if not success:
                self.log("File processing failed", logging.ERROR)
                return

            self.log(f"File processing completed successfully for flow {flow_id}")

            # 10. Получаем параметры для PM блока
            source_connection, source_schema, storage_connection, compute_connection = self._get_dag_pm_params(flow_id)
            if not all([source_connection, source_schema, storage_connection, compute_connection]):
                self.log("Missing PM DAG parameters", logging.ERROR)
                return

            # 11. Создаем отдельный flow только с Process Mining блоком
            self.log("Creating separate Process Mining flow...")
            table_name = f"Tube_{flow_id}"

            pm_flow_name, pm_flow_id = self._create_pm_flow(
                worker_id=self.worker_id,
                source_connection=source_connection,
                source_schema=source_schema,
                storage_connection=storage_connection,
                compute_connection=compute_connection,
                table_name=table_name,
                base_flow_name=flow_name
            )

            if not pm_flow_id:
                self.log("Failed to create Process Mining flow", logging.ERROR)
                return

            self.log(f"Successfully created PM flow: {pm_flow_name} (ID: {pm_flow_id})")
            self.log(f"Process Mining will use existing table: {table_name}")

            # 12. Запускаем Process Mining flow
            self.log(f"Starting Process Mining flow {pm_flow_id}...")
            pm_run_id = self._start_pm_flow(pm_flow_id, source_connection, source_schema, storage_connection, compute_connection, table_name)

            if not pm_run_id:
                self.log("Failed to start Process Mining flow", logging.ERROR)
                return

            # 13. Мониторинг статуса Process Mining
            pm_timeout = CONFIG["upload_control"]["pm_timeout"]
            pm_result = self._monitor_processing_status(
                pm_run_id, pm_timeout, pm_flow_id, is_pm_flow=True
            )

            # 14. Обработка результата PM flow и открытие дашборда
            if isinstance(pm_result, dict) and pm_result.get("success"):
                self.log(f"Process Mining completed successfully for flow {pm_flow_id}!")

                # Получаем block_run_ids из результата мониторинга
                block_run_ids = pm_result.get("block_run_ids", {})

                # Определяем целевой блок
                target_block_id = "spm_dashboard_creation_v_0_2[0]"
                block_run_id = block_run_ids.get(target_block_id)

                if block_run_id:
                    # Получаем URL дашборда из артефактов
                    self.log(f"Fetching dashboard URL for block {target_block_id}...")
                    dashboard_url = self._get_dashboard_url_from_artefacts(
                        pm_flow_id=pm_flow_id,
                        block_id=target_block_id,
                        block_run_id=block_run_id,
                        run_id=pm_run_id
                    )

                    if dashboard_url:
                        # Открываем дашборд
                        self.log(f"Opening dashboard: {dashboard_url}")
                        dashboard_loaded = self._open_dashboard(dashboard_url)

                        if dashboard_loaded:
                            self.log(f"Dashboard successfully loaded: {dashboard_url}")
                        else:
                            self.log(f"Failed to load dashboard: {dashboard_url}", logging.WARNING)
                    else:
                        self.log("Could not retrieve dashboard URL from artefacts", logging.WARNING)
                else:
                    self.log(f"block_run_id not found for block {target_block_id}", logging.WARNING)

            elif pm_result is True:
                # Старый формат ответа (без block_run_ids)
                self.log(f"Process Mining completed but no block_run_ids available", logging.WARNING)
            else:
                self.log(f"Process Mining failed for flow {pm_flow_id}", logging.ERROR)

            self.log(f"Complete process finished. File flow: {flow_name} (ID: {flow_id}), PM flow: {pm_flow_name} (ID: {pm_flow_id})")

        except Exception as e:
            self.log(f"Unexpected error in flow processing: {str(e)}", logging.ERROR)