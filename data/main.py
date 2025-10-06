import argparse
import os
import json
import time
import random
import shutil
from datetime import datetime, timedelta
from typing import List

from case_generator import CaseGenerator
from csv_writer import CSVWriter
from utils import distribute_processes, is_holiday
from config import CONFIG_20GB, CONFIG_30GB, CONFIG_50GB, CONFIG_CUSTOM
from logger import get_logger


class ProcessMiningGenerator:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.generator = CaseGenerator(start_case_id=1, logger=logger)
        self.csv_writer = CSVWriter(logger)

    def check_disk_space(self, required_gb: float):
        total, used, free = shutil.disk_usage(self.config["output_dir"])
        free_gb = free / (1024 ** 3)
        if free_gb < required_gb * 1.2:
            raise Exception(
                f"Недостаточно места: {free_gb:.1f} GB, требуется: {required_gb * 1.2:.1f} GB"
            )
        self.logger.info("Свободного места: %.1f GB", free_gb)

    def create_output_directory(self):
        os.makedirs(self.config["output_dir"], exist_ok=True)

    def _generate_case_timestamps(self, start_date: datetime, num_cases: int, time_range_days: int) -> List[datetime]:
        """Генерирует реалистичные временные метки для кейсов"""
        timestamps = []
        current_date = start_date
        days_generated = 0

        while len(timestamps) < num_cases and days_generated < time_range_days:
            # Пропускаем выходные и праздники
            if current_date.weekday() >= 5 or is_holiday(current_date):
                current_date += timedelta(days=1)
                days_generated += 1
                continue

            # Генерируем от 1 до 20 кейсов в рабочий день
            cases_today = random.randint(1, 20)
            for _ in range(min(cases_today, num_cases - len(timestamps))):
                # Случайное время в течение дня (преимущественно рабочие часы)
                if random.random() < 0.8:
                    hour = random.randint(8, 19)  # Рабочие часы
                else:
                    hour = random.randint(0, 23)  # Ночные часы

                minute = random.randint(0, 59)
                second = random.randint(0, 59)

                timestamp = datetime(
                    year=current_date.year,
                    month=current_date.month,
                    day=current_date.day,
                    hour=hour,
                    minute=minute,
                    second=second
                )
                timestamps.append(timestamp)

            current_date += timedelta(days=1)
            days_generated += 1

        # Если не хватило дней, заполняем случайными датами
        while len(timestamps) < num_cases:
            random_day = random.randint(0, time_range_days - 1)
            random_date = start_date + timedelta(days=random_day)

            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)

            timestamp = datetime(
                year=random_date.year,
                month=random_date.month,
                day=random_date.day,
                hour=hour,
                minute=minute,
                second=second
            )
            timestamps.append(timestamp)

        return sorted(timestamps)

    def generate_data(self):
        self.logger.info("Запуск генерации %.1fGB данных...", self.config["target_size_gb"])
        self.logger.info("Выходная директория: %s", self.config["output_dir"])

        self.create_output_directory()
        self.check_disk_space(self.config["target_size_gb"])

        target_bytes = self.config["target_size_gb"] * 1024 * 1024 * 1024
        size_str = str(self.config["target_size_gb"]).replace(".", "_")
        final_filename = os.path.join(self.config["output_dir"], f"process_log_{size_str}GB.csv")

        # Размер строки
        avg_row_size = 800
        events_needed = int(target_bytes / avg_row_size) * 1.2  # +20% событий
        events_needed = int(events_needed)  # Конвертируем в integer

        self.logger.info("Оценочный размер строки: %d байт", avg_row_size)
        self.logger.info("Необходимо событий: %d", events_needed)

        process_distribution = distribute_processes(
            self.config["process_distribution"], events_needed // 10
        )
        # Убедимся, что все значения в process_distribution являются integers
        process_distribution = {k: int(v) for k, v in process_distribution.items()}

        total_events = 0
        total_cases = 0
        start_date = datetime.strptime(self.config["start_date"], "%Y-%m-%d")
        time_range_days = self.config.get("time_range_days", 365 * 2)

        start_time = time.time()
        total_events_planned = sum(process_distribution.values()) * 12  # Увеличено множитель

        if hasattr(self.logger, 'start_progress'):
            self.logger.start_progress(total_events_planned, "Общая генерация событий")

        first_chunk = True
        for process_name, num_cases in process_distribution.items():
            if num_cases <= 0:
                continue

            self.logger.info("Генерация для процесса: %s", process_name)
            self.logger.info("Кейсов: %d", num_cases)

            # Генерируем "реалистичные" временные метки для всех кейсов процесса
            case_timestamps = self._generate_case_timestamps(
                start_date, num_cases, time_range_days
            )

            process_start_time = time.time()
            cases_generated = 0
            batch_size = 10000

            while cases_generated < num_cases:
                current_batch = min(batch_size, num_cases - cases_generated)

                # Для каждого кейса в батче используем свое время
                process_events = []
                for i in range(current_batch):
                    case_time = case_timestamps[cases_generated + i]
                    case_events = self.generator.generate_case_with_transitions(
                        process_name=process_name,
                        start_time=case_time,
                        anomaly_rate=self.config["anomaly_rate"],
                        rework_rate=self.config["rework_rate"],
                    )
                    process_events.extend(case_events)

                mode = "a" if not first_chunk else "w"
                self.csv_writer.write_events_to_csv(process_events, final_filename, mode=mode)
                first_chunk = False

                total_events += len(process_events)
                cases_generated += current_batch
                total_cases += current_batch

                if hasattr(self.logger, 'update_progress'):
                    self.logger.update_progress(len(process_events))

                if cases_generated % 50000 == 0:
                    process_elapsed = time.time() - process_start_time
                    self.logger.info(
                        "Процесс %s: %d/%d кейсов (%.1f%%) за %.1f сек",
                        process_name,
                        cases_generated,
                        num_cases,
                        (cases_generated / num_cases) * 100,
                        process_elapsed,
                    )

                del process_events

            process_elapsed = time.time() - process_start_time
            self.logger.info(
                "Процесс %s завершен: %d кейсов за %.1f сек",
                process_name,
                cases_generated,
                process_elapsed,
            )

        if hasattr(self.logger, 'close_progress'):
            self.logger.close_progress()


        actual_size = os.path.getsize(final_filename)
        actual_size_gb = actual_size / (1024 ** 3)
        total_time = time.time() - start_time

        self.logger.info("Генерация завершена!")
        self.logger.info("Статистика:")
        self.logger.info("Файл: %s", final_filename)
        self.logger.info("Целевой размер: %.1f GB", self.config["target_size_gb"])
        self.logger.info("Фактический размер: %.3f GB", actual_size_gb)
        self.logger.info("Кейсов: %d", total_cases)
        self.logger.info("Событий: %d", total_events)
        self.logger.info("Время выполнения: %.2f сек", total_time)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Генератор логов процессов для Process Mining")
    parser.add_argument("--config", type=str, default="custom",
                        choices=["20GB", "30GB", "50GB", "custom"],
                        help="Конфигурация для генерации")
    parser.add_argument("--size", type=float, help="Кастомный размер в GB (только для --config custom)")
    parser.add_argument("--output", type=str, help="Кастомная выходная директория")
    return parser.parse_args()


def main():
    args = parse_arguments()
    logger = get_logger()

    if args.config == "20GB":
        config = CONFIG_20GB.copy()
    elif args.config == "30GB":
        config = CONFIG_30GB.copy()
    elif args.config == "50GB":
        config = CONFIG_50GB.copy()
    else:
        config = CONFIG_CUSTOM.copy()
        if args.size:
            config["target_size_gb"] = args.size

    if args.output:
        config["output_dir"] = args.output
    else:
        size_label = str(config["target_size_gb"]).replace(".", "_")
        config["output_dir"] = f"./dataset/"

    start_time = time.time()
    try:
        generator = ProcessMiningGenerator(config, logger)
        generator.generate_data()
    except Exception as e:
        logger.error("Ошибка: %s", e)
        import traceback
        traceback.print_exc()
    finally:
        end_time = time.time()
        logger.info("Общее время: %.2f секунд", end_time - start_time)


if __name__ == "__main__":
    main()
