"""Manager classes for flows and users"""
import threading
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


class StopManager:
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not StopManager._initialized:
            self._lock = threading.Lock()
            self._should_stop = False
            self._completed_iterations = 0
            self._max_iterations = 1
            self._stop_called = False
            StopManager._initialized = True

    def set_max_iterations(self, max_iterations):
        with self._lock:
            self._max_iterations = max_iterations

    def increment_iteration(self):
        with self._lock:
            self._completed_iterations += 1
            if self._completed_iterations >= self._max_iterations:
                self._should_stop = True
            return self._should_stop

    def should_stop(self):
        with self._lock:
            return self._should_stop and not self._stop_called

    def set_stop_called(self):
        with self._lock:
            self._stop_called = True

    def is_stop_called(self):
        with self._lock:
            return self._stop_called

    def get_stats(self):
        with self._lock:
            return {
                "completed": self._completed_iterations,
                "max": self._max_iterations,
                "should_stop": self._should_stop,
                "stop_called": self._stop_called
            }

stop_manager = StopManager()