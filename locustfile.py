from locust import HttpUser, between

from config import CONFIG
from scenarios.load_test import LoadFlow


class SupersetUser(HttpUser):
    host = CONFIG["api"]["base_url"]
    tasks = [LoadFlow]
    wait_time = between(min_wait=1, max_wait=5)
