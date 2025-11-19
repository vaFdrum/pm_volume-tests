import locust.runners
locust.runners.MASTER_HEARTBEAT_TIMEOUT = 900
locust.runners.HEARTBEAT_INTERVAL = 750

from locust import HttpUser, between


from config import CONFIG
from scenarios.load_test import LoadFlow
from scenarios.process_metrics import ProcessMetricsCalculator


class SupersetUser(HttpUser):
    host = CONFIG["api"]["base_url"]
    tasks = [ProcessMetricsCalculator]
    wait_time = between(min_wait=1, max_wait=5)