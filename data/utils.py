import random
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from config import Season, SEASONAL_MULTIPLIERS, WAITING_TIMES
from constants import (
    ACTIVITY_DURATIONS,
    ANOMALY_DURATIONS,
    ANOMALY_ACTIVITIES,
    REWORK_ACTIVITIES,
)


def get_season(dt: datetime) -> Season:
    month = dt.month
    if month in [1, 2, 3]:
        return Season.Q1
    elif month in [4, 5, 6]:
        return Season.Q2
    elif month in [7, 8, 9]:
        return Season.Q3
    return Season.Q4


def is_holiday(date: datetime) -> bool:
    """Проверяет является ли день праздничным"""
    holidays = {
        (1, 1), (1, 2), (1, 7), (2, 23), (3, 8), (5, 1), (5, 9),
        (6, 12), (11, 4)
    }
    return (date.month, date.day) in holidays


def generate_realistic_timestamps(start_date: datetime, num_events: int, time_range_days: int) -> List[datetime]:
    """Генерирует реалистичные временные метки с кластеризацией"""
    timestamps = []
    current_date = start_date
    days_generated = 0

    while len(timestamps) < num_events and days_generated < time_range_days:
        # Пропускаем выходные и праздники
        if current_date.weekday() >= 5 or is_holiday(current_date):
            current_date += timedelta(days=1)
            days_generated += 1
            continue

        # Генерируем от 5 до 50 событий в рабочий день
        events_today = random.randint(5, 50)
        for _ in range(min(events_today, num_events - len(timestamps))):
            # 80% событий в рабочие часы (9-18)
            if random.random() < 0.8:
                hour = random.randint(9, 17)
            else:
                hour = random.randint(0, 23)

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
    while len(timestamps) < num_events:
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


def get_activity_duration(activity: str, process_name: str, current_time: datetime) -> int:
    if activity in ACTIVITY_DURATIONS:
        min_dur, max_dur = ACTIVITY_DURATIONS[activity]
        base_duration = random.randint(min_dur, max_dur)
    else:
        base_duration = random.randint(1, 8)

    # Применяем сезонный множитель
    season = get_season(current_time)
    multiplier = SEASONAL_MULTIPLIERS.get(process_name, {}).get(season, 1.0)

    return max(1, int(base_duration * multiplier))


def get_waiting_time(process_name: str, current_time: datetime) -> int:
    min_wait, max_wait = WAITING_TIMES.get(process_name, (5, 60))

    # Увеличиваем вариативность в межсезонье
    season = get_season(current_time)
    if season != Season.Q4:
        max_wait = int(max_wait * 0.6)
        variation = random.randint(-15, 40)
    else:
        variation = random.randint(-8, 20)

    base_wait = random.randint(min_wait, max_wait) + variation
    base_wait = max(1, base_wait)

    multiplier = SEASONAL_MULTIPLIERS.get(process_name, {}).get(season, 1.0)

    return max(1, int(base_wait * multiplier))


def should_add_anomaly(anomaly_rate: float) -> bool:
    return random.random() < anomaly_rate


def should_add_rework(rework_rate: float) -> bool:
    return random.random() < rework_rate


def get_anomaly_for_activity(activity: str) -> Optional[str]:
    possible_anomalies = []
    for anomaly, activities in ANOMALY_ACTIVITIES.items():
        if activity in activities:
            possible_anomalies.append(anomaly)

    if possible_anomalies:
        return random.choice(possible_anomalies)
    return None


def get_rework_for_activity(activity: str) -> Optional[str]:
    possible_rework = []
    for rework, activities in REWORK_ACTIVITIES.items():
        if activity in activities:
            possible_rework.append(rework)

    if possible_rework:
        return random.choice(possible_rework)
    return None


def get_anomaly_duration(anomaly: str) -> int:
    if anomaly in ANOMALY_DURATIONS:
        min_dur, max_dur = ANOMALY_DURATIONS[anomaly]
        return random.randint(min_dur, max_dur)
    return random.randint(30, 180)


def get_rework_duration() -> int:
    return random.randint(20, 120)


def distribute_processes(process_distribution: Dict[str, float], num_cases: int) -> Dict[str, int]:
    """Распределяет кейсы по процессам с использованием алгоритма largest remainder method"""
    if num_cases <= 0:
        return {process: 0 for process in process_distribution}

    # Нормализуем веса
    total_weight = sum(process_distribution.values())
    normalized_weights = {
        process: weight / total_weight
        for process, weight in process_distribution.items()
    }

    # Рассчитываем гарантированное распределение и дробные части
    items = []
    total_allocated = 0

    for process, weight in normalized_weights.items():
        exact_count = num_cases * weight
        integer_part = int(exact_count)
        fractional_part = exact_count - integer_part

        items.append({
            'process': process,
            'integer': integer_part,
            'fractional': fractional_part
        })
        total_allocated += integer_part

    # Сортируем по убыванию дробной части для распределения остатка
    items.sort(key=lambda x: x['fractional'], reverse=True)

    # Распределяем оставшиеся кейсы
    remaining = num_cases - total_allocated
    for i in range(remaining):
        items[i]['integer'] += 1

    return {item['process']: item['integer'] for item in items}
