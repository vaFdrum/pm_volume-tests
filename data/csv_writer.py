import csv
from typing import List, Dict
from datetime import datetime
import random
import string
import os
from constants import CSV_FIELD_NAMES, DEPARTMENTS, PRIORITIES


class CSVWriter:
    def __init__(self, logger):
        self.logger = logger

    def generate_comment(self, min_chars: int = 100, max_chars: int = 800) -> str:
        """Генерирует правдоподобные комментарии с разным размером"""
        words = [
            "process", "system", "analysis", "data", "mining", "business", "workflow",
            "optimization", "efficiency", "performance", "monitoring", "tracking",
            "compliance", "audit", "validation", "verification", "automation",
            "integration", "transformation", "migration", "synchronization",
            "deployment", "configuration", "implementation", "maintenance",
            "support", "development", "testing", "quality", "assurance", "security",
            "networking", "infrastructure", "cloud", "database", "application",
            "service", "platform", "framework",
        ]

        num_words = random.randint(60, 250)  # Увеличено количество слов
        comment = " ".join(random.choices(words, k=num_words))

        if len(comment) < max_chars:
            additional_chars = random.randint(0, max_chars - len(comment))
            comment += " " + "".join(
                random.choices(
                    string.ascii_letters + string.digits + " ,.-!?", k=additional_chars
                )
            )

        return comment[:max_chars]

    def generate_additional_data(self) -> Dict:
        cost_type = random.choices(
            ['normal', 'high', 'very_high'],
            weights=[0.95, 0.04, 0.01],
            k=1
        )[0]

        if cost_type == 'normal':
            cost = round(random.uniform(15.0, 500.0), 2)
        elif cost_type == 'high':
            cost = round(random.uniform(500.0, 2000.0), 2)
        else:  # very_high
            cost = round(random.uniform(2000.0, 7500.0), 2)

        return {
            "user_id": f"user_{random.randint(1, 8000)}",
            "department": random.choice(DEPARTMENTS),
            "priority": random.choice(PRIORITIES),
            "cost": cost,
            "comment": self.generate_comment(250, 650),
            "resource_usage": round(random.uniform(2.0, 150.0), 2),
            "processing_time": random.randint(2, 900),
            "queue_time": random.randint(0, 450),
            "success_rate": round(random.uniform(80.0, 99.9), 2),
            "error_count": random.randint(0, 8),
            "client_id": f"CLIENT_{random.randint(1, 10000):08d}",
            "product_id": f"PROD_{random.randint(1, 5000):05d}",
            "contract_id": f"CNTR_{random.randint(1, 100000):07d}",
            "branch_code": random.choice(["NYC", "LAX", "LON", "TOK", "BER", "SFO", "PAR", "SYD"]),
            "service_tier": random.choice(["basic", "premium", "enterprise", "vip"]),
        }

    def write_events_to_csv(self, events: List[Dict], filepath: str, mode: str = "w"):
        is_append = mode == "a" and os.path.exists(filepath)

        if self.logger:
            self.logger.info("Запись %d событий в CSV (mode: %s)...", len(events), mode)

        with open(filepath, mode, newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=CSV_FIELD_NAMES,
                lineterminator="\n"
            )

            if not is_append:
                writer.writeheader()

            for i, event in enumerate(events):
                formatted_event = self._format_event(event)
                writer.writerow(formatted_event)

                if (i + 1) % 50000 == 0 and self.logger:
                    self.logger.info("Записано %d событий...", i + 1)

    def _format_event(self, event: Dict) -> Dict:
        formatted_event = event.copy()

        # Конвертируем datetime в строки
        for time_field in ["timestamp_start", "timestamp_end"]:
            if time_field in formatted_event and isinstance(formatted_event[time_field], datetime):
                formatted_event[time_field] = formatted_event[time_field].strftime("%Y-%m-%d %H:%M:%S")

        # Заполняем все обязательные поля
        for field in CSV_FIELD_NAMES:
            if field not in formatted_event:
                formatted_event[field] = self._get_default_value(field)

        # Добавляем дополнительные данные
        additional_data = self.generate_additional_data()
        formatted_event.update(additional_data)

        return formatted_event

    def _get_default_value(self, field: str):
        defaults = {
            "anomaly": False,
            "rework": False,
            "anomaly_type": None,
            "role": "",
            "resource": "",
            "duration_minutes": 0,
            "user_id": "",
            "department": "",
            "priority": "",
            "cost": 0.0,
            "comment": "",
            "resource_usage": 0.0,
            "processing_time": 0,
            "queue_time": 0,
            "success_rate": 0.0,
            "error_count": 0,
            "client_id": "",
            "product_id": "",
            "contract_id": "",
            "branch_code": "",
            "service_tier": "",
        }
        return defaults.get(field, "")
