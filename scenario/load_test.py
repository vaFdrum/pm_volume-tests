"""Locust tasks module for Superset ETL flow testing"""

import copy
import logging
import random
import time
from datetime import datetime
from urllib.parse import urljoin, quote

import urllib3
from locust import SequentialTaskSet, task, between

from common.auth import extract_login_form
from common.csv_utils import split_csv_generator, count_chunks, count_csv_lines
from common.managers import FlowManager, UserPool
from common.metrics import (
    REQUEST_COUNT,
    AUTH_ATTEMPTS,
    CHUNK_UPLOADS,
    FLOW_CREATIONS,
    ACTIVE_USERS,
    CHUNKS_IN_PROGRESS,
    UPLOAD_PROGRESS,
    SESSION_STATUS,
    REQUEST_DURATION,
    AUTH_DURATION,
    CHUNK_UPLOAD_DURATION,
    FLOW_PROCESSING_DURATION,
    COUNT_VALIDATION_RESULT,
    DB_ROW_COUNT,
    EXPECTED_ROWS,
    start_metrics_server,
)
from config import CONFIG

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ★★★ ЗАПУСКАЕМ СЕРВЕР МЕТРИК ★★★
start_metrics_server(CONFIG.get("metrics_port", 9090))


class LoadFlow(SequentialTaskSet):
    wait_time = between(min_wait=1, max_wait=5)

    def __init__(self, parent):
        super().__init__(parent)
        self.session_id = f"{random.randint(1000, 9999)}"
        self.logged_in = False
        self.session_valid = False
        self.total_chunks = count_chunks(CONFIG["csv_file_path"], CONFIG["chunk_size"])
        self.total_lines = count_csv_lines(CONFIG["csv_file_path"])
        self.worker_id = 0
        self.username = None
        self.password = None
        self.flow_id = None

        # ★★★ УСТАНАВЛИВАЕМ МЕТРИКУ ОЖИДАЕМЫХ СТРОК ★★★
        EXPECTED_ROWS.set(self.total_lines)

    def log(self, message, level=logging.INFO):
        """Logging with session context"""
        if not CONFIG.get("log_verbose") and level not in [
            logging.ERROR,
            logging.CRITICAL,
        ]:
            return

        level_name = logging.getLevelName(level)
        timestamp = datetime.now().strftime("%Y-%m-%d")

        log_message = (
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - "
            f"SupersetLoadTest - {level_name} - "
            f"[Session {self.session_id}] {message}\n"
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
        """Retry mechanism with timeouts"""
        timeout = kwargs.pop("timeout", CONFIG["request_timeout"])
        start_time = time.time()  # ★★★ ЗАПОМИНАЕМ ВРЕМЯ НАЧАЛА ★★★

        for attempt in range(CONFIG["max_retries"]):
            try:
                kwargs["timeout"] = timeout
                with method(url, name=name, catch_response=True, **kwargs) as response:
                    if response.status_code < 400:
                        # ★★★ ЗАПИСЫВАЕМ МЕТРИКИ УСПЕШНОГО ЗАПРОСА ★★★
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
                        # ★★★ ЗАПИСЫВАЕМ МЕТРИКИ ОШИБКИ КЛИЕНТА ★★★
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
                        # ★★★ ЗАПИСЫВАЕМ МЕТРИКИ ОШИБКИ СЕРВЕРА ★★★
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
                # ★★★ ЗАПИСЫВАЕМ МЕТРИКИ ОШИБКИ ЗАПРОСА ★★★
                REQUEST_COUNT.labels(
                    method=method.__name__.upper(), endpoint=name, status="error"
                ).inc()

            if attempt < CONFIG["max_retries"] - 1:
                delay = CONFIG["retry_delay"] * (2**attempt)
                time.sleep(min(delay, 10))

        self.log(f"All attempts for {name} failed", logging.ERROR)
        return None

    def establish_session(self):
        """Establish user session with authentication"""
        auth_start_time = time.time()  # ★★★ ЗАПОМИНАЕМ ВРЕМЯ НАЧАЛА АУТЕНТИФИКАЦИИ ★★★

        for attempt in range(CONFIG["max_retries"]):
            try:
                self.client.cookies.clear()

                # 1) GET login page
                resp = self._retry_request(
                    self.client.get, url="/", name="Get login page", timeout=10
                )
                if not resp or resp.status_code != 200:
                    AUTH_ATTEMPTS.labels(
                        username=self.username, success="false"
                    ).inc()  # ★★★ МЕТРИКА ★★★
                    continue

                form = extract_login_form(resp.text, self.username, self.password)
                if not form:
                    AUTH_ATTEMPTS.labels(
                        username=self.username, success="false"
                    ).inc()  # ★★★ МЕТРИКА ★★★
                    continue

                # 2) POST credentials
                resp = self._retry_request(
                    self.client.post,
                    form["action"],
                    name="Submit credentials",
                    data=form["payload"],
                    allow_redirects=False,
                    timeout=15,
                )
                if not resp or resp.status_code != 302:
                    AUTH_ATTEMPTS.labels(
                        username=self.username, success="false"
                    ).inc()  # ★★★ МЕТРИКА ★★★
                    continue

                location = resp.headers.get("Location")
                if not location:
                    AUTH_ATTEMPTS.labels(
                        username=self.username, success="false"
                    ).inc()  # ★★★ МЕТРИКА ★★★
                    continue

                # 3) Complete redirect
                resp = self._retry_request(
                    self.client.get,
                    urljoin(form["action"], location),
                    name="Complete auth redirect",
                    timeout=10,
                )
                if resp and resp.status_code == 200:
                    self.logged_in = True
                    self.session_valid = True
                    self.log(f"Authentication successful for {self.username}")

                    # ★★★ ЗАПИСЫВАЕМ МЕТРИКИ УСПЕШНОЙ АУТЕНТИФИКАЦИИ ★★★
                    auth_duration = time.time() - auth_start_time
                    AUTH_DURATION.observe(auth_duration)
                    AUTH_ATTEMPTS.labels(username=self.username, success="true").inc()
                    SESSION_STATUS.labels(username=self.username).set(1)
                    ACTIVE_USERS.inc()

                    return

            except Exception as e:
                self.log(
                    f"Auth attempt {attempt + 1} failed: {str(e)}", logging.WARNING
                )
                AUTH_ATTEMPTS.labels(
                    username=self.username, success="false"
                ).inc()  # ★★★ МЕТРИКА ★★★
                time.sleep(CONFIG["retry_delay"])

        self.log("Authentication failed", logging.ERROR)
        SESSION_STATUS.labels(username=self.username).set(0)  # ★★★ МЕТРИКА ★★★
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

    # ★★★ ДОБАВЛЯЕМ НОВЫЙ МЕТОД ДЛЯ ОЧИСТКИ МЕТРИК ★★★
    def on_stop(self):
        """Clean up metrics when user stops"""
        if self.logged_in:
            ACTIVE_USERS.dec()
            SESSION_STATUS.labels(username=self.username).set(0)

    def _get_user_database_id(self):
        resp = self._retry_request(
            self.client.get,
            url="/api/v1/database/",
            name="Get databases list",
            timeout=15,
        )
        if not resp or not resp.ok:
            return None
        normalized_username = self.username.replace("_", "")
        for db in resp.json().get("result", []):
            db_name = db.get("database_name", "")
            created_by = db.get("created_by")
            if (
                created_by
                and db_name.startswith("SberProcessMiningDB_spm")
                and normalized_username in db_name
            ):
                return db.get("id")
        return None

    def _create_flow(self):
        flow_id = FlowManager.get_next_id(worker_id=self.worker_id)
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
            FLOW_CREATIONS.labels(status="failed").inc()  # ★★★ МЕТРИКА ★★★
            return None, None
        new_flow_id = resp.json().get("id")
        FLOW_CREATIONS.labels(status="success").inc()  # ★★★ МЕТРИКА ★★★
        return flow_name, new_flow_id

    def _get_dag_params(self, flow_id):
        url = f"/etl/api/v1/flow/dag_params/v2/spm_file_loader_v2?q=(active:!f,block_id:0,enum_limit:20,flow_id:{flow_id})"
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
        count_chunks_val=0,
    ):
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
                "parent_id": None,
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

    def _upload_chunks(self, flow_id, db_id, target_schema):
        """Загрузка чанков с метриками"""
        uploaded_chunks = 0
        chunk_timeout = 30

        # ★★★ УВЕЛИЧИВАЕМ СЧЕТЧИК АКТИВНЫХ ЗАГРУЗОК ★★★
        CHUNKS_IN_PROGRESS.inc()

        for chunk in split_csv_generator(CONFIG["csv_file_path"], CONFIG["chunk_size"]):
            if not chunk or not chunk["chunk_text"]:
                continue

            chunk_start_time = time.time()  # ★★★ ЗАПОМИНАЕМ ВРЕМЯ НАЧАЛА ★★★
            success = False

            for attempt in range(CONFIG["max_retries"]):
                try:
                    data_payload = {
                        "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
                        "database_id": str(db_id),
                        "schema": target_schema,
                        "table_name": f"Tube_{flow_id}",
                        "part_num": str(chunk["chunk_number"]),
                        "total_chunks": str(self.total_chunks),
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

                        # ★★★ ЗАПИСЫВАЕМ МЕТРИКИ УСПЕШНОЙ ЗАГРУЗКИ ★★★
                        chunk_duration = time.time() - chunk_start_time
                        CHUNK_UPLOAD_DURATION.observe(chunk_duration)
                        CHUNK_UPLOADS.labels(
                            flow_id=str(flow_id), status="success"
                        ).inc()

                        # ★★★ ОБНОВЛЯЕМ ПРОГРЕСС ★★★
                        progress = (uploaded_chunks / self.total_chunks) * 100
                        UPLOAD_PROGRESS.labels(flow_id=str(flow_id)).set(progress)

                        self.log(
                            f"Chunk {chunk['chunk_number']}/{self.total_chunks} uploaded"
                        )
                        break

                except Exception as e:
                    self.log(
                        f"Chunk {chunk['chunk_number']} upload failed: {str(e)}",
                        logging.WARNING,
                    )
                    CHUNK_UPLOADS.labels(
                        flow_id=str(flow_id), status="failed"
                    ).inc()  # ★★★ МЕТРИКА ★★★

                if not success and attempt < CONFIG["max_retries"] - 1:
                    time.sleep(CONFIG["retry_delay"] * (attempt + 1))

            if not success:
                self.log(
                    f"Failed to upload chunk {chunk['chunk_number']} after {CONFIG['max_retries']} attempts",
                    logging.ERROR,
                )

        # ★★★ УМЕНЬШАЕМ СЧЕТЧИК АКТИВНЫХ ЗАГРУЗОК ★★★
        CHUNKS_IN_PROGRESS.dec()
        return uploaded_chunks

    def _validate_row_count(self, db_id, target_schema, flow_id):
        """Проверка количества строк с метриками"""
        try:
            self.log(f"Start validating data for flow {flow_id}")

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

            with resp:
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("data") and data["data"]:
                        db_count = data["data"][0].get("count()", 0)

                        # ★★★ ЗАПИСЫВАЕМ МЕТРИКИ ВАЛИДАЦИИ ★★★
                        DB_ROW_COUNT.labels(flow_id=str(flow_id)).set(db_count)

                        validation_success = db_count == self.total_lines
                        COUNT_VALIDATION_RESULT.labels(flow_id=str(flow_id)).set(
                            1 if validation_success else 0
                        )

                        self.log(
                            f"Rows in DB: {db_count}, expected: {self.total_lines}"
                        )
                        return validation_success

                return False

        except Exception:
            # ★★★ ЗАПИСЫВАЕМ МЕТРИКУ НЕУДАЧНОЙ ВАЛИДАЦИИ ★★★
            COUNT_VALIDATION_RESULT.labels(flow_id=str(flow_id)).set(0)
            return False

    @task
    def create_and_upload_flow(self):
        # ★★★ ЗАПОМИНАЕМ ВРЕМЯ НАЧАЛА ОБРАБОТКИ ★★★
        flow_processing_start = time.time()

        if not self.logged_in:
            self.establish_session()
            if not self.logged_in:
                return

        flow_name, flow_id = self._create_flow()
        self.flow_id = flow_id

        if not flow_id:
            self.log("Failed to create flow", logging.ERROR)
            return

        target_connection, target_schema = self._get_dag_params(flow_id)
        if not target_connection or not target_schema:
            self.log("Missing DAG parameters", logging.ERROR)
            return

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

        # Start upload
        start_data = {
            "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
            "database_id": str(db_id),
            "table_name": f"Tube_{flow_id}",
            "schema": target_schema,
            "flow_id": str(flow_id),
            "block_id": CONFIG["block"]["block_id"],
            "total_chunks": str(self.total_chunks),
        }

        self._retry_request(
            self.client.post,
            url="/etl/api/v1/file/start_upload",
            name="Start file upload",
            json=start_data,
            timeout=timeout,
        )

        uploaded_chunks = self._upload_chunks(flow_id, db_id, target_schema)

        # Finalize upload
        finalize_data = {
            "count_chunks": uploaded_chunks,
            "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
        }

        self._retry_request(
            self.client.post,
            url="/etl/api/v1/file/finalize",
            name="Finalize file upload",
            json=finalize_data,
            timeout=timeout,
        )

        # Start processing
        final_data = {
            "flow_id": int(flow_id),
            "block_id": CONFIG["block"]["block_id"],
            "config": {
                **CONFIG["upload_settings"],
                "count_chunks": str(self.total_chunks),
                "fileUploaded": False,
                "target_connection": target_connection,
                "target_schema": target_schema,
                "target_table": f"Tube_{flow_id}",
                "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
                "preview": {},
                "columns": CONFIG["upload_columns"],
            },
        }

        final = self._retry_request(
            self.client.post,
            url="/etl/api/v1/file/start",
            name="Final file",
            json=final_data,
            timeout=timeout,
        )

        if not final or not final.ok:
            self.log("Failed to start file processing", logging.ERROR)
            return

        run_id = final.json().get("run_id")
        if not run_id:
            self.log("No run_id in response", logging.ERROR)
            return

        self.log(
            f"File upload process completed, {uploaded_chunks}/{self.total_chunks} chunks uploaded"
        )

        # Poll status with timeout
        encoded_string = quote(str(run_id))
        status_url = f"/etl/api/v1/file/status/{encoded_string}"
        max_wait_time = timeout
        start_time = time.time()
        poll_count = 0

        while time.time() - start_time < max_wait_time:
            poll_count += 1
            status_response = self._retry_request(
                self.client.get, url=status_url, name="Status", timeout=15
            )

            if status_response and status_response.ok:
                status_data = status_response.json()
                current_status = status_data.get("status")

                if current_status == "success":
                    self.log("Status 'success' received! Task completed.")
                    # Валидация количества строк
                    self._validate_row_count(db_id, target_schema, flow_id)

                    # ★★★ ЗАПИСЫВАЕМ ОБЩЕЕ ВРЕМЯ ОБРАБОТКИ ★★★
                    flow_processing_time = time.time() - flow_processing_start
                    FLOW_PROCESSING_DURATION.labels(flow_id=str(flow_id)).observe(
                        flow_processing_time
                    )

                    return

                elif current_status == "failed":
                    self.log("The task ended with an error", logging.ERROR)
                    return

                else:
                    if poll_count % 5 == 0:  # Логируем каждые 5 проверок
                        self.log(
                            f"Current status: {current_status}. Expectation: {int(time.time() - start_time)}с"
                        )

            time.sleep(CONFIG["upload_control"]["pool_interval"])

        self.log(f"Status wait timeout ({max_wait_time}с) expired", logging.ERROR)