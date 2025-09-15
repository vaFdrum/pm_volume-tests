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
