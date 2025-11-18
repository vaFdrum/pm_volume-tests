"""Base api classes with reusable methods"""

import copy
import logging
import time
from datetime import datetime
from urllib.parse import quote

from locust import SequentialTaskSet

from common.csv_utils import split_csv_generator
from common.managers import FlowManager, stop_manager
from common.metrics import (
    REQUEST_COUNT,
    REQUEST_DURATION,
    FLOW_CREATIONS,
    CHUNK_UPLOADS,
    CHUNKS_IN_PROGRESS,
    UPLOAD_PROGRESS,
    CHUNK_UPLOAD_DURATION,
    DB_ROW_COUNT,
    COUNT_VALIDATION_RESULT,
    FLOW_PROCESSING_DURATION,
)
from config import CONFIG


class Api(SequentialTaskSet):
    def __init__(self, parent):
        super().__init__(parent)
        self.username = None
        self.password = None
        self.session_id = None
        self.logged_in = False
        self.session_valid = False

    def log(self, message, level=logging.INFO):
        """Logging with session context"""
        if not CONFIG.get("log_verbose") and level not in [
            logging.ERROR,
            logging.CRITICAL,
        ]:
            return

        level_name = logging.getLevelName(level)
        timestamp = datetime.now().strftime("%Y-%m-%d")

        # Формируем контекст для лога
        iteration_info = ""
        if hasattr(self, 'user_iteration_count') and hasattr(self, 'max_user_iterations'):
            iteration_info = f"[Iter {self.user_iteration_count}/{self.max_user_iterations}]"

        log_message = (
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - "
            f"SupersetLoadTest - {level_name} - "
            f"[User {self.username or 'N/A'}][Session {self.session_id}]{iteration_info} {message}\n"
        )

        if level >= logging.WARNING or CONFIG.get("log_verbose"):
            print(log_message, end="")

        try:
            log_filename = f"./logs/locust_test_{timestamp}.log"
            with open(log_filename, "a", encoding="utf-8") as file:
                file.write(log_message)
                file.flush()
        except Exception as error:
            print(f"Log file error: {error}")

    def _retry_request(self, method, url, name, **kwargs):
        """Retry mechanism with timeouts and metrics"""
        timeout = kwargs.pop("timeout", CONFIG["request_timeout"])
        start_time = time.time()

        for attempt in range(CONFIG["max_retries"]):
            try:
                kwargs["timeout"] = timeout
                with method(url, name=name, catch_response=True, **kwargs) as response:
                    if response.status_code < 400:
                        # Записываем метрики успешного запроса
                        duration = time.time() - start_time
                        REQUEST_DURATION.labels(
                            method=method.__name__.upper(), endpoint=name
                        ).observe(duration)
                        REQUEST_COUNT.labels(
                            method=method.__name__.upper(),
                            endpoint=name,
                            status=response.status_code,
                        ).inc()
                        return response

                    elif 400 <= response.status_code < 500:
                        self.log(
                            f"Client error {response.status_code} from {name}",
                            logging.WARNING,
                        )
                        # Записываем метрики ошибки клиента
                        REQUEST_COUNT.labels(
                            method=method.__name__.upper(),
                            endpoint=name,
                            status=response.status_code,
                        ).inc()
                        response.failure(f"Client error: {response.status_code}")
                        return response

                    else:
                        self.log(
                            f"Server error {response.status_code} from {name}, attempt {attempt + 1}",
                            logging.WARNING,
                        )
                        # Записываем метрики ошибки сервера
                        REQUEST_COUNT.labels(
                            method=method.__name__.upper(),
                            endpoint=name,
                            status=response.status_code,
                        ).inc()

            except Exception as e:
                self.log(
                    f"Request {name} attempt {attempt + 1} failed: {str(e)}",
                    logging.WARNING,
                )
                # Записываем метрики ошибки запроса
                REQUEST_COUNT.labels(
                    method=method.__name__.upper(), endpoint=name, status="error"
                ).inc()

            if attempt < CONFIG["max_retries"] - 1:
                delay = CONFIG["retry_delay"] * (2 ** attempt)
                time.sleep(min(delay, 10))

        self.log(f"All attempts for {name} failed", logging.ERROR)
        return None

    def _get_user_database_id(self):
        """Get user's database ID by username pattern"""
        resp = self._retry_request(
            self.client.get,
            url="/api/v1/database/",
            name="Get databases list",
            timeout=15,
        )
        if not resp or not resp.ok:
            return None

        normalized_username = str(self.username).replace("_", "")
        expected_pattern = f"SberProcessMiningDB_{normalized_username}"

        # Сначала ищем точное совпадение
        for db in resp.json().get("result", []):
            db_name = db.get("database_name", "")
            if db_name.startswith(expected_pattern):
                return db.get("id")

        # Если не нашли, ищем частичное совпадение
        for db in resp.json().get("result", []):
            db_name = db.get("database_name", "")
            if ("SberProcessMiningDB" in db_name and
                    normalized_username in db_name):
                return db.get("id")

        return None

    def _create_flow(self, worker_id=0):
        """Create a new flow"""
        flow_id = FlowManager.get_next_id(worker_id=worker_id)
        flow_name = f"Tube_{flow_id}"
        flow_data = copy.deepcopy(CONFIG["flow_template"])
        flow_data["label"] = flow_name

        resp = self._retry_request(
            self.client.post,
            CONFIG["api"]["flow_endpoint"],
            name="Create flow",
            json=flow_data,
            timeout=20,
        )

        if not resp or not resp.ok:
            FLOW_CREATIONS.labels(status="failed").inc()
            return None, None

        new_flow_id = resp.json().get("id")
        FLOW_CREATIONS.labels(status="success").inc()
        return flow_name, new_flow_id

    def _get_dag_params(self, flow_id):
        """Get DAG parameters for flow"""
        url = (
            f"/etl/api/v1/flow/dag_params/v2/spm_file_loader_v2?"
            f"q=(active:!f,block_id:0,enum_limit:20,flow_id:{flow_id})"
        )
        resp = self._retry_request(
            self.client.get, url, name="Get DAG parameters", timeout=15
        )
        if not resp or not resp.ok:
            return None, None

        target_connection = target_schema = None
        for item in resp.json().get("result", []):
            if item[0] == "target_connection":
                target_connection = item[1]["value"]
            elif item[0] == "target_schema":
                target_schema = item[1]["value"]

        return target_connection, target_schema

    def _update_flow(
            self,
            flow_id,
            flow_name,
            target_connection,
            target_schema,
            file_uploaded=False,
            count_chunks_val=0
    ):
        """Update flow configuration"""
        update_data = copy.deepcopy(CONFIG["flow_template"])
        update_data["label"] = flow_name
        update_data["config_inactive"]["blocks"] = [
            {
                "block_id": CONFIG["block"]["block_id"],
                "config": {
                    "date_convert": True,
                    "default_timezone": "Europe/Moscow",
                    "delimiter": ",",
                    "encoding": "UTF-8",
                    "file_type": "CSV",
                    "if_exists": "replace",
                    "is_config_valid": True,
                    "skip_rows": 0,
                    "target_connection": target_connection,
                    "target_schema": target_schema,
                    "target_table": f"Tube_{flow_id}",
                    "fileUploaded": file_uploaded,
                    "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
                    "count_chunks": str(count_chunks_val),
                    "preview": {},
                    "columns": CONFIG["update_columns"],
                },
                "dag_id": CONFIG["block"]["dag_id"],
                "id": CONFIG["block"]["block_id"],
                "is_deprecated": False,
                "label": "Импорт данных из файла",
                "number": 1,
                "parent_ids": [],
                "status": "deferred",
                "type": CONFIG["block"]["dag_id"],
                "x": 152,
                "y": 0,
            }
        ]

        return self._retry_request(
            self.client.put,
            url=f"{CONFIG['api']['flow_endpoint']}{flow_id}",
            name="Update flow config",
            json=update_data,
            timeout=20,
        )

    def _upload_chunks(self, flow_id, db_id, target_schema, total_chunks):
        """Upload CSV chunks to server with progress tracking"""
        uploaded_chunks = 0
        chunk_timeout = 30

        # Увеличиваем счетчик активных загрузок
        CHUNKS_IN_PROGRESS.inc()

        try:
            for chunk in split_csv_generator(CONFIG["csv_file_path"], CONFIG["chunk_size"]):
                if not chunk or not chunk["chunk_text"]:
                    continue

                chunk_start_time = time.time()
                success = False

                for attempt in range(CONFIG["max_retries"]):
                    try:
                        data_payload = {
                            "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
                            "database_id": str(db_id),
                            "schema": target_schema,
                            "table_name": f"Tube_{flow_id}",
                            "part_num": str(chunk["chunk_number"]),
                            "total_chunks": str(total_chunks),
                            "block_id": CONFIG["block"]["block_id"],
                            "flow_id": str(flow_id),
                        }
                        files_payload = {
                            "file": (
                                f"chunk_{chunk['chunk_number']}.csv",
                                chunk["chunk_text"],
                                "text/csv",
                            )
                        }

                        resp = self._retry_request(
                            self.client.post,
                            url="/etl/api/v1/file/upload",
                            name=f"Upload chunk {chunk['chunk_number']}",
                            data=data_payload,
                            files=files_payload,
                            timeout=chunk_timeout,
                        )

                        if resp and resp.ok:
                            uploaded_chunks += 1
                            success = True

                            # Записываем метрики успешной загрузки
                            chunk_duration = time.time() - chunk_start_time
                            CHUNK_UPLOAD_DURATION.observe(chunk_duration)
                            CHUNK_UPLOADS.labels(
                                flow_id=str(flow_id), status="success"
                            ).inc()

                            # Обновляем прогресс
                            progress = (uploaded_chunks / total_chunks) * 100
                            UPLOAD_PROGRESS.labels(flow_id=str(flow_id)).set(progress)

                            self.log(
                                f"Chunk {chunk['chunk_number']}/{total_chunks} uploaded"
                            )
                            break

                    except Exception as e:
                        self.log(
                            f"Chunk {chunk['chunk_number']} upload failed: {str(e)}",
                            logging.WARNING,
                        )
                        CHUNK_UPLOADS.labels(
                            flow_id=str(flow_id), status="failed"
                        ).inc()

                    if not success and attempt < CONFIG["max_retries"] - 1:
                        time.sleep(CONFIG["retry_delay"] * (attempt + 1))

                if not success:
                    self.log(
                        f"Failed to upload chunk {chunk['chunk_number']} "
                        f"after {CONFIG['max_retries']} attempts",
                        logging.ERROR,
                    )

        finally:
            # Уменьшаем счетчик активных загрузок
            CHUNKS_IN_PROGRESS.dec()

        return uploaded_chunks

    def _start_file_upload(self, flow_id, db_id, target_schema, total_chunks, timeout):
        """Start file upload process"""
        start_data = {
            "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
            "database_id": str(db_id),
            "table_name": f"Tube_{flow_id}",
            "schema": target_schema,
            "flow_id": str(flow_id),
            "block_id": CONFIG["block"]["block_id"],
            "total_chunks": str(total_chunks),
        }

        start_resp = self._retry_request(
            self.client.post,
            url="/etl/api/v1/file/start_upload",
            name="Start file upload",
            json=start_data,
            timeout=timeout,
        )

        if not start_resp or not start_resp.ok:
            self.log("Failed to start file upload", logging.ERROR)
            return False

        self.log("File upload started successfully", logging.INFO)
        return True

    def _finalize_file_upload(self, flow_id, uploaded_chunks, timeout):
        """Finalize file upload"""
        finalize_data = {
            "count_chunks": uploaded_chunks,
            "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
        }

        finalize_resp = self._retry_request(
            self.client.post,
            url="/etl/api/v1/file/finalize",
            name="Finalize file upload",
            json=finalize_data,
            timeout=timeout,
        )

        if not finalize_resp or not finalize_resp.ok:
            self.log("Failed to finalize file upload", logging.ERROR)
            return False

        self.log("File upload finalized successfully")
        return True

    def _start_file_processing(self, flow_id, target_connection, target_schema, total_chunks, timeout):
        """Start file processing"""
        final_data = {
            "flow_id": int(flow_id),
            "block_id": CONFIG["block"]["block_id"],
            "config": {
                **CONFIG["upload_settings"],
                "count_chunks": str(total_chunks),
                "fileUploaded": False,
                "target_connection": target_connection,
                "target_schema": target_schema,
                "target_table": f"Tube_{flow_id}",
                "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
                "preview": {},
                "columns": CONFIG["upload_columns"],
            },
        }

        final_resp = self._retry_request(
            self.client.post,
            url="/etl/api/v1/file/start",
            name="Final file",
            json=final_data,
            timeout=timeout,
        )

        if not final_resp or not final_resp.ok:
            self.log("Failed to start file processing", logging.ERROR)
            return None

        run_id = final_resp.json().get("run_id")
        if not run_id:
            self.log("No run_id in response", logging.ERROR)
            return None

        return run_id

    def _monitor_processing_status(self, run_id, timeout, flow_id, db_id, target_schema,
                                   total_lines, flow_processing_start):
        """Monitor file processing status"""
        encoded_string = quote(str(run_id))
        status_url = f"/etl/api/v1/file/status/{encoded_string}"
        max_wait_time = timeout
        start_time = time.time()
        poll_count = 0

        while time.time() - start_time < max_wait_time:
            if stop_manager.is_stop_called():
                self.log("Stop called during status monitoring", logging.ERROR)
                return False

            poll_count += 1
            status_response = self._retry_request(
                self.client.get, url=status_url, name="Status", timeout=15
            )

            if status_response and status_response.ok:
                status_data = status_response.json()
                current_status = status_data.get("status")
                error_message = status_data.get("error", "No error details")

                if current_status == "success":
                    self.log("Status 'success' received! Task completed.")

                    validation_result = self._validate_row_count(
                        db_id, target_schema, flow_id, total_lines
                    )

                    flow_processing_time = time.time() - flow_processing_start
                    minutes = int(flow_processing_time // 60)
                    seconds = flow_processing_time % 60

                    FLOW_PROCESSING_DURATION.labels(flow_id=str(flow_id)).observe(flow_processing_time)

                    self.log(f"Flow {flow_id} completed successfully!")
                    self.log(f"Total processing time: {minutes}m {seconds:.1f}s ({flow_processing_time:.2f} seconds)")
                    self.log(f"Validation: {'PASS' if validation_result else 'FAIL'}")

                    return True

                elif current_status == "failed":
                    self.log(f"The task ended with an error: {error_message}", logging.ERROR)
                    return False

                else:
                    if poll_count % 5 == 0:
                        elapsed = int(time.time() - start_time)
                        self.log(f"Current status: {current_status}. Elapsed: {elapsed}s, Poll: {poll_count}")

            else:
                self.log(f"Status check failed (attempt {poll_count})", logging.WARNING)

            time.sleep(CONFIG["upload_control"]["pool_interval"])

        self.log(f"Status wait timeout ({max_wait_time}s) expired", logging.ERROR)
        return False

    def _validate_row_count(self, db_id, target_schema, flow_id, expected_rows):
        """Validate row count in database table"""
        try:
            self.log(f"Start validating data for the table Tube_{flow_id}")

            payload = {
                "client_id": "",
                "database_id": str(db_id),
                "json": True,
                "runAsync": False,
                "schema": target_schema,
                "sql": f'SELECT COUNT(*) FROM "{target_schema}"."Tube_{flow_id}"',
                "sql_editor_id": "4",
                "tab": "Locust Validation",
                "tmp_table_name": "",
                "select_as_cta": False,
                "ctas_method": "TABLE",
                "queryLimit": 1000,
                "expand_data": True,
            }

            self.log(f"Sending a validation request for the table Tube_{flow_id}")

            resp = self._retry_request(
                self.client.post,
                url="/api/v1/sqllab/execute/",
                name="Validate row count",
                json=payload,
                headers={"Content-Type": "application/json"},
            )

            if resp and resp.status_code == 200:
                data = resp.json()
                if data.get("data") and data["data"]:
                    db_count = data["data"][0].get("count()", 0)

                    # Записываем метрики валидации
                    DB_ROW_COUNT.labels(flow_id=str(flow_id)).set(db_count)

                    validation_success = db_count == expected_rows
                    COUNT_VALIDATION_RESULT.labels(flow_id=str(flow_id)).set(
                        1 if validation_success else 0
                    )

                    self.log(
                        f"Rows in DB: {db_count}, expected: {expected_rows}"
                    )
                    return validation_success

            return False

        except Exception as e:
            self.log(f"Validation error: {str(e)}", logging.ERROR)
            # Записываем метрику неудачной валидации
            COUNT_VALIDATION_RESULT.labels(flow_id=str(flow_id)).set(0)
            return False

    def _save_process_mining_metrics(
            self,
            flow_id,
            flow_name,
            target_connection,
            target_schema,
            file_uploaded=False,
            count_chunks_val=None,
            run_id=""
    ):
        """Save Process Mining metrics configuration"""
        try:
            # 1. Сначала получим актуальную версию flow
            get_resp = self._retry_request(
                self.client.get,
                url=f"/etl/api/v1/flow/{flow_id}",
                name="Get flow before PM update"
            )

            if not get_resp or not get_resp.ok:
                self.log("Failed to get current flow configuration", logging.ERROR)
                return None

            current_flow = get_resp.json().get("result", {})
            current_version = current_flow.get("version", 1)
            current_version_inactive = current_flow.get("version_inactive", 1)
            
            self.log(
                f"Current flow version: {current_version}, "
                f"inactive: {current_version_inactive}"
            )

            # 2. Создаем обновленную конфигурацию на основе текущей
            update_pm = copy.deepcopy(current_flow)

            # 3. Обновляем основные поля
            update_pm["label"] = flow_name

            # 4. Проверяем и инициализируем config_inactive если его нет
            if "config_inactive" not in update_pm:
                update_pm["config_inactive"] = {"blocks": [], "chart_height": 288}

            if "blocks" not in update_pm["config_inactive"]:
                update_pm["config_inactive"]["blocks"] = []

            # 5. Обновляем блоки в config_inactive
            update_pm["config_inactive"]["blocks"] = [
                {
                    "block_id": CONFIG["block"]["block_id"],
                    "config": {
                        "date_convert": True,
                        "default_timezone": "Europe/Moscow",
                        "delimiter": ",",
                        "encoding": "UTF-8",
                        "file_type": "CSV",
                        "if_exists": "replace",
                        "is_config_valid": True,
                        "skip_rows": 0,
                        "target_connection": target_connection,
                        "target_schema": target_schema,
                        "target_table": f"Tube_{flow_id}",
                        "fileUploaded": file_uploaded,
                        "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
                        "count_chunks": str(count_chunks_val),
                        "preview": {},
                        "columns": CONFIG["update_columns"],
                    },
                    "dag_id": CONFIG["block"]["dag_id"],
                    "id": CONFIG["block"]["block_id"],
                    "is_deprecated": False,
                    "label": "Импорт данных из файла",
                    "number": 1,
                    "parent_ids": [],
                    "run_id": run_id,
                    "status": "success",
                    "type": CONFIG["block"]["dag_id"],
                    "x": 152,
                    "y": 0,
                },
                {
                    "id": "spm_dashboard_creation_v_0_2[1]",
                    "parent_ids": ["spm_file_loader_v2[0]"],
                    "label": "Расчет метрик Process Mining",
                    "status": "deferred",
                    "type": "spm_dashboard_creation_v_0_2",
                    "config": {
                        "source_connection": target_connection,
                        "source_schema": target_schema,
                        "source_table": f"Tube_{flow_id}",
                        "dashboard_title": f"Tube_{flow_id}_DASHBOARD", 
                        "threshold": 30,
                        "validation_types": [
                            "DUPLICATES",
                            "START_END_DATE",
                            "UNRECOGNIZED_VALUES_IN_KEY_FIELDS"
                        ],
                        "duplicate_reaction": "DROP_BY_KEY",
                        "marking": CONFIG["marking_config"],
                        "packet_size": 0,
                        "run_auto_insights": False,
                        "autoinsights_timeout_sec": 36000,
                        "compute_connection": "_internal_ch_admin",
                        "storage_connection": "SberProcessMiningDB_spm45",
                        "is_config_valid": True,
"activity_name_col": "activity",
                "activity_start_col": "timestamp_start", 
                "activity_end_col": "timestamp_end",
                "case_col_name": "case_id"
                    },
                    "number": 2,
                    "x": 304,
                    "y": 0,
                    "block_id": "spm_dashboard_creation_v_0_2[1]",
                    "dag_id": "spm_dashboard_creation_v_0_2"
                }
            ]

            # 6. Отправляем PUT запрос с обновленной конфигурацией
            resp_pm = self._retry_request(
                self.client.put,
                url=f"/etl/api/v1/flow/{flow_id}",
                json=update_pm,
                name="Save DAG Process Mining Metrics"
            )
            
            if not resp_pm or not resp_pm.ok:
                self.log("Failed to save process mining metrics", logging.ERROR)
                return None

            self.log("Process Mining metrics saved successfully")
            return resp_pm

        except Exception as e:
            self.log(f"Error in _save_process_mining_metrics: {str(e)}", logging.ERROR)
            return None