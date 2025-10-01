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
        config_path = os.getenv("CONFIG_PATH")
        if config_path is None:
            if os.path.exists("config_multi.yaml"):
                config_path = "config_multi.yaml"
                print("Auto-detected config: config_multi.yaml")
            elif os.path.exists("config_ift.yaml"):
                config_path = "config_ift.yaml"
                print("Auto-detected config: config_ift.yaml")
            else:
                print("No config file found, using fallback configuration")
                return get_fallback_config()

    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}, using fallback configuration")
        return get_fallback_config()

    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    # Обработка base_url из .env
    if config.get("api", {}).get("base_url") == "FROM_ENV":
        env_base_url = os.getenv("BASE_URL")
        if env_base_url:
            config["api"]["base_url"] = env_base_url
            print("Successfully substituted BASE_URL from environment")
        else:
            print("WARNING: BASE_URL is set to FROM_ENV but environment variable is not set")

    # Обработка max_iterations из .env
    if config.get("max_iterations") == "FROM_ENV":
        env_max_iterations = os.getenv("MAX_ITERATIONS")
        if env_max_iterations:
            try:
                config["max_iterations"] = int(env_max_iterations)
                print(f"Successfully substituted MAX_ITERATIONS from environment: {config['max_iterations']}")
            except ValueError:
                config["max_iterations"] = 1
                print(f"WARNING: MAX_ITERATIONS must be integer, using default: 1")
        else:
            config["max_iterations"] = 1
            print("WARNING: max_iterations is set to FROM_ENV but MAX_ITERATIONS environment variable is not set, using default: 1")

    # Обработка csv_file_path
    if config.get("csv_file_path") == "FROM_ENV":
        env_csv_path = os.getenv("CSV_FILE_PATH")
        if env_csv_path:
            config["csv_file_path"] = env_csv_path
            print("Successfully substituted CSV_FILE_PATH from environment")
        else:
            print("WARNING: csv_file_path is set to FROM_ENV but CSV_FILE_PATH environment variable is not set")

    # Проверяем существование CSV файла
    if config.get("csv_file_path") and isinstance(config["csv_file_path"], str):
        if not os.path.exists(config["csv_file_path"]):
            print(f"WARNING: CSV file not found at {config['csv_file_path']}")

    # Обработка паролей пользователей
    if "users" in config:
        for user in config["users"]:
            if user.get("password") == "FROM_ENV":
                env_password = os.getenv("PASSWORD")
                if env_password:
                    user["password"] = env_password
                    print(f"Successfully substituted password for user: {user.get('username')}")
                else:
                    print(f"WARNING: Password for user {user.get('username')} is FROM_ENV but PASSWORD environment variable is not set")

    print(f"Successfully loaded configuration from: {config_path}")
    return config


def get_fallback_config() -> Dict[str, Any]:
    """Return fallback configuration when no config files are found"""
    # Получаем max_iterations из env или используем значение по умолчанию
    try:
        max_iterations = int(os.getenv("MAX_ITERATIONS", "1"))
    except ValueError:
        max_iterations = 1
        print("WARNING: MAX_ITERATIONS must be integer, using default: 1")

    return {
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
            "base_url": os.getenv("BASE_URL", ""),
            "flow_endpoint": "/etl/api/v1/flow/",
        },
        "upload_control": {
            "timeout_small": 300,
            "timeout_large": 3600,
            "chunk_threshold": 200,
            "pool_interval": 5,
        },
        "max_iterations": max_iterations,
        "log_verbose": True,
        "log_debug": False,
        "log_level": "INFO",
        "csv_file_path": os.getenv("CSV_FILE_PATH", ""),
        "chunk_size": 4 * 1024 * 1024,
        "max_retries": 3,
        "retry_delay": 2,
        "request_timeout": 30,
    }


try:
    CONFIG = load_config()
except Exception as e:
    print(f"Error loading configuration: {e}")
    print("Falling back to default config...")
    CONFIG = get_fallback_config()