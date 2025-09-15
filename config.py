"""Configuration module for the Locust load test.

This module contains all the settings and constants required for the test,
such as user credentials, API endpoints, and file upload parameters.
"""

import os
from typing import Dict, Any

import yaml
from dotenv import load_dotenv

load_dotenv()


def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Loads configuration from YAML file and substitutes secrets from .env
    """
    if config_path is None:
        config_path = os.getenv("CONFIG_PATH", "config.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if "users" in config:
        for user in config["users"]:
            if user["password"] == "FROM_ENV":
                env_var_name = f"{user['username'].upper()}_PASSWORD"
                env_password = os.getenv(env_var_name)
                if env_password:
                    user["password"] = env_password

    return config


try:
    CONFIG = load_config()
except FileNotFoundError as e:
    print(f"Error: {e}")
    print("Falling back to default config...")

    CONFIG = {
        "users": [
            {
                "username": os.getenv("USER1_USERNAME"),
                "password": os.getenv("USER1_PASSWORD"),
            },
            {
                "username": os.getenv("USER2_USERNAME"),
                "password": os.getenv("USER2_PASSWORD"),
            },
            {
                "username": os.getenv("USER3_USERNAME"),
                "password": os.getenv("USER3_PASSWORD"),
            },
            {
                "username": os.getenv("USER4_USERNAME"),
                "password": os.getenv("USER4_PASSWORD"),
            },
            {
                "username": os.getenv("USER5_USERNAME"),
                "password": os.getenv("USER5_PASSWORD"),
            },
        ],
        "api": {
            "base_url": os.getenv("BASE_URL"),
            "flow_endpoint": "/etl/api/v1/flow/",
        },
        "upload_control": {
            "timeout_small": 300,  # 5 minutes
            "timeout_large": 1800,  # 30 minutes
            "chunk_threshold": 200,  # Chunks threshold for large files
            "pool_interval": 5,  # Status check interval
        },
        "log_verbose": True,
        "log_debug": False,
        "log_level": "INFO",
        "update_columns": [
            {"column_type": "int", "column_name": "id", "id": 1, "column_use": True},
            {
                "column_type": "str",
                "column_name": "process",
                "id": 2,
                "column_use": True,
            },
            {
                "column_type": "str",
                "column_name": "activity",
                "id": 3,
                "column_use": True,
            },
            {
                "column_type": "date",
                "column_name": "start",
                "id": 4,
                "column_use": True,
                "date_format": "yyyy-MM-dd HH:mm:ss",
            },
            {
                "column_type": "date",
                "column_name": "end",
                "id": 5,
                "column_use": True,
                "date_format": "yyyy-MM-dd HH:mm:ss",
            },
            {
                "column_type": "str",
                "column_name": "user_id",
                "id": 6,
                "column_use": True,
            },
            {
                "column_type": "str",
                "column_name": "department",
                "id": 7,
                "column_use": True,
            },
            {
                "column_type": "str",
                "column_name": "priority",
                "id": 8,
                "column_use": True,
            },
            {"column_type": "int", "column_name": "cost", "id": 9, "column_use": True},
            {
                "column_type": "str",
                "column_name": "comment",
                "id": 10,
                "column_use": True,
            },
        ],
        "flow_template": {
            "config": {"blocks": []},
            "dag": "spm_orchestrator",
            "flow_type": "spm_orchestrator",
            "retries": 1,
            "schedule": "@never",
            "is_active": False,
            "config_inactive": {"blocks": [], "chart_height": 288},
        },
        "csv_file_path": os.getenv("CSV_FILE_PATH"),
        "chunk_size": 4 * 1024 * 1024,  # 4MB
        "max_retries": 3,
        "retry_delay": 2,
        "request_timeout": 30,
        "upload_settings": {
            "date_convert": True,
            "default_timezone": "Europe/Moscow",
            "delimiter": ",",
            "encoding": "UTF-8",
            "file_type": "CSV",
            "if_exists": "replace",
            "skip_rows": 0,
        },
        "upload_columns": [
            {"column_type": "int", "column_name": "id", "id": 1, "column_use": True},
            {
                "column_type": "str",
                "column_name": "process",
                "id": 2,
                "column_use": True,
            },
            {
                "column_type": "str",
                "column_name": "activity",
                "id": 3,
                "column_use": True,
            },
            {
                "column_type": "date",
                "column_name": "start",
                "id": 4,
                "column_use": True,
                "date_format": "yyyy-MM-dd HH:mm:ss",
            },
            {
                "column_type": "date",
                "column_name": "end",
                "id": 5,
                "column_use": True,
                "date_format": "yyyy-MM-dd HH:mm:ss",
            },
            {
                "column_type": "str",
                "column_name": "user_id",
                "id": 6,
                "column_use": True,
            },
            {
                "column_type": "str",
                "column_name": "department",
                "id": 7,
                "column_use": True,
            },
            {
                "column_type": "str",
                "column_name": "priority",
                "id": 8,
                "column_use": True,
            },
            {"column_type": "int", "column_name": "cost", "id": 9, "column_use": True},
            {
                "column_type": "str",
                "column_name": "comment",
                "id": 10,
                "column_use": True,
            },
        ],
        "block": {
            "block_id": "spm_file_loader_v2[0]",
            "dag_id": "spm_file_loader_v2",
        },
        "monitoring": {
            "enable_metrics": True,
            "metrics_port": 9090,
            "metrics_prefix": "superset_loadtest",
            "collect_interval": 15,  # seconds
        },
    }
