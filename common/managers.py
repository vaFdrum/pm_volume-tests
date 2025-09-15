"""Manager classes for flows and users"""

from threading import Lock

from config import CONFIG


class FlowManager:
    _lock = Lock()
    _counter = 0

    @classmethod
    def get_next_id(cls, worker_id=0):
        with cls._lock:
            cls._counter += 1
            return worker_id * 100000 + cls._counter


class UserPool:
    _lock = Lock()
    _index = 0

    @classmethod
    def get_credentials(cls):
        with cls._lock:
            creds = CONFIG["users"][cls._index % len(CONFIG["users"])]
            cls._index += 1
            return creds
