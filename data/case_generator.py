import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from config import PROCESS_MODELS
from utils import (
    get_activity_duration,
    get_waiting_time,
    should_add_anomaly,
    should_add_rework,
    get_anomaly_for_activity,
    get_rework_for_activity,
    get_anomaly_duration,
    get_rework_duration,
    get_season,
    Season
)
from constants import get_activity_name, INTER_SEASON_ACTIVITIES


class CaseGenerator:
    def __init__(self, start_case_id: int = 1, logger=None):
        self.current_case_id = start_case_id - 1
        self.logger = logger

    def _log_debug(self, message: str, *args):
        if self.logger:
            self.logger.info(message, *args)

    def _add_inter_season_events(self, events: List[Dict], process_name: str, current_time: datetime) -> List[Dict]:
        """Добавляет межсезонные события только в межсезонье"""
        season = get_season(current_time)

        # Добавляем события ТОЛЬКО в межсезонье (Q1-Q3)
        if season != Season.Q4 and random.random() < 0.3:  # Reduced probability
            inter_season_activities = list(INTER_SEASON_ACTIVITIES.keys())

            # Добавляем 1-2 события, не 2-5
            num_extra_events = random.randint(1, 2)

            for activity in random.sample(inter_season_activities, k=num_extra_events):
                duration = random.randint(30, 240)
                inter_event = {
                    "case_id": events[0]["case_id"],
                    "timestamp_start": current_time,
                    "timestamp_end": current_time + timedelta(minutes=duration),
                    "process": process_name,
                    "activity": activity,
                    "duration_minutes": duration,
                    "role": "Analyst",
                    "resource": "System",
                    "anomaly": False,
                    "anomaly_type": None,
                    "rework": False
                }
                events.append(inter_event)
                current_time += timedelta(minutes=duration + random.randint(5, 30))

        return events

    def _add_peak_season_events(self, events: List[Dict], process_name: str, current_time: datetime) -> List[Dict]:
        """В пиковый сезон НЕ добавляем дополнительные события - только основную деятельность"""
        # В пиковый сезон (Q4) все ресурсы на основной работе
        # Не добавляем аудиты, обучение и т.д.
        return events

    def _add_anomalies_and_rework(self, events: List[Dict], process_name: str,
                                  current_time: datetime, anomaly_rate: float, rework_rate: float) -> List[Dict]:
        """Добавляет аномалии и переделки в события"""
        has_anomaly = should_add_anomaly(anomaly_rate)
        has_rework = should_add_rework(rework_rate)
        anomaly_added = False
        rework_added = False
        new_events = []

        for event in events:
            new_events.append(event)
            current_time = event["timestamp_end"]

            # Аномалия
            if has_anomaly and not anomaly_added and random.random() < 0.4:
                anomaly_type = get_anomaly_for_activity(event["activity"])
                if anomaly_type:
                    anomaly_duration = get_anomaly_duration(anomaly_type)
                    anomaly_event = {
                        "case_id": event["case_id"],
                        "timestamp_start": current_time,
                        "timestamp_end": current_time + timedelta(minutes=anomaly_duration),
                        "process": process_name,
                        "activity": f"{event['activity']} - {anomaly_type}",
                        "duration_minutes": anomaly_duration,
                        "role": "Specialist",
                        "resource": "System",
                        "anomaly": True,
                        "anomaly_type": anomaly_type,
                        "rework": False,
                    }
                    new_events.append(anomaly_event)
                    current_time += timedelta(minutes=anomaly_duration)
                    anomaly_added = True

            # Переделка
            if has_rework and not rework_added and random.random() < 0.3:
                rework_type = get_rework_for_activity(event["activity"])
                if rework_type:
                    rework_duration = get_rework_duration()
                    rework_event = {
                        "case_id": event["case_id"],
                        "timestamp_start": current_time,
                        "timestamp_end": current_time + timedelta(minutes=rework_duration),
                        "process": process_name,
                        "activity": f"{event['activity']} - {rework_type}",
                        "duration_minutes": rework_duration,
                        "role": self._get_role_for_activity(event["activity"], process_name),
                        "resource": self._get_resource_for_activity(event["activity"], process_name),
                        "anomaly": False,
                        "anomaly_type": None,
                        "rework": True,
                    }
                    new_events.append(rework_event)
                    current_time += timedelta(minutes=rework_duration)
                    rework_added = True

        return new_events

    def generate_case_with_transitions(self, process_name: str, start_time: Optional[datetime] = None,
                                       anomaly_rate: float = 0.03, rework_rate: float = 0.08) -> List[Dict]:
        """Генерирует кейс с учетом матрицы переходов - ОСНОВНАЯ ФУНКЦИЯ"""
        if process_name not in PROCESS_MODELS:
            raise ValueError(f"Unknown process: {process_name}")

        model = PROCESS_MODELS[process_name]
        weights = model["weights"]
        transition_matrix = model["transition_matrix"]

        # Выбираем сценарий по весам
        scenario_idx = random.choices(range(len(model["scenarios"])), weights=weights, k=1)[0]
        base_scenario = model["scenarios"][scenario_idx]

        self.current_case_id += 1
        case_id = self.current_case_id
        events = []
        current_time = start_time or (datetime.now() - timedelta(days=random.randint(0, 730)))

        # Генерируем события с учетом вероятностей переходов
        current_activity = base_scenario[0]
        activity_index = 0
        max_activities = len(base_scenario) * 3

        while current_activity and activity_index < max_activities:
            # Добавляем ожидание между активностями
            if events:
                waiting_time = get_waiting_time(process_name, current_time)
                current_time += timedelta(minutes=waiting_time)

            # Генерируем текущее событие
            duration = get_activity_duration(current_activity, process_name, current_time)

            event = {
                "case_id": case_id,
                "timestamp_start": current_time,
                "timestamp_end": current_time + timedelta(minutes=duration),
                "process": process_name,
                "activity": current_activity,
                "duration_minutes": duration,
                "role": self._get_role_for_activity(current_activity, process_name),
                "resource": self._get_resource_for_activity(current_activity, process_name),
                "anomaly": False,
                "anomaly_type": None,
                "rework": False,
            }
            events.append(event)
            current_time += timedelta(minutes=duration)

            # Определяем следующую активность
            if current_activity in transition_matrix:
                next_activities = transition_matrix[current_activity]
                if next_activities:
                    # Выбираем следующую активность по вероятностям
                    activities, probabilities = zip(*next_activities.items())
                    next_activity = random.choices(activities, weights=probabilities, k=1)[0]

                    # Проверяем, есть ли следующая активность в базовом сценарии
                    if (activity_index + 1 < len(base_scenario) and
                            next_activity == base_scenario[activity_index + 1]):
                        activity_index += 1
                    current_activity = next_activity
                else:
                    break  # Конец процесса
            else:
                # Переход к следующей активности в сценарии
                activity_index += 1
                if activity_index < len(base_scenario):
                    current_activity = base_scenario[activity_index]
                else:
                    break

        # Добавляем аномалии и переделки
        events = self._add_anomalies_and_rework(events, process_name, current_time, anomaly_rate, rework_rate)

        # Добавляем межсезонные события (только в межсезонье)
        events = self._add_inter_season_events(events, process_name, current_time)

        # В пиковый сезон НЕ добавляем дополнительные события
        # events = self._add_peak_season_events(events, process_name, current_time)

        return events

    def generate_case(self, process_name: str, start_time: Optional[datetime] = None,
                      anomaly_rate: float = 0.03, rework_rate: float = 0.08) -> List[Dict]:
        """Основной метод генерации кейса"""
        return self.generate_case_with_transitions(
            process_name=process_name,
            start_time=start_time,
            anomaly_rate=anomaly_rate,
            rework_rate=rework_rate
        )

    def _get_role_for_activity(self, activity: str, process_name: str) -> str:
        role_mapping = {
            "OrderFulfillment": {
                "Order Created": "Clerk", "Payment Processing": "System", "Card Verification": "System",
                "Payment Received": "System", "Payment Failed": "System", "Payment Retry": "System",
                "Inventory Check": "Clerk", "Backorder Created": "Clerk", "Inventory Arrival": "Clerk",
                "Pick Items": "Clerk", "Pack Items": "Clerk", "Quality Check": "Specialist",
                "Ship Order": "Coordinator", "Order Completed": "System", "Order Cancelled": "Manager",
                "Return Request": "Coordinator", "Refund Processing": "System", "Order Closed": "System",
                "Re-pick Items": "Clerk", "Re-pack Items": "Clerk"
            },
            "CustomerSupport": {
                "Ticket Created": "System", "Initial Triage": "Support Agent", "Issue Investigation": "Support Agent",
                "Solution Provided": "Support Agent", "Ticket Closed": "System", "Escalated": "Manager",
                "Expert Review": "Specialist", "Customer Feedback": "Support Agent",
                "Additional Support": "Support Agent",
                "Cannot Resolve": "Manager", "Additional Escalation": "Manager"
            },
            "LoanApplication": {
                "Application Submitted": "Clerk", "Document Review": "Analyst", "Credit Check": "System",
                "Loan Approval": "Manager", "Funds Disbursed": "System", "Additional Info Requested": "Analyst",
                "Info Received": "Clerk", "Loan Rejected": "Manager", "Auto-Approval": "System",
                "Underwriting": "Loan Officer", "Conditional Approval": "Manager", "Conditions Met": "Clerk",
                "Application Withdrawn": "System"
            },
            "InvoiceProcessing": {
                "Invoice Received": "Clerk", "Data Entry": "Clerk", "Validation": "Analyst",
                "Approval": "Manager", "Payment Processed": "System", "Archived": "System",
                "Validation Failed": "Analyst", "Correction": "Clerk", "Re-validation": "Analyst",
                "Invoice Rejected": "Manager", "Auto-Processing": "System", "Expedited Review": "Manager",
                "Manual Review": "Analyst"
            },
            "HRRecruitment": {
                "Position Opened": "HR Manager", "Application Review": "HR Manager", "Phone Screen": "HR Manager",
                "Interview": "HR Manager", "Offer Extended": "Manager", "Offer Accepted": "System",
                "Hired": "System", "Candidate Rejected": "HR Manager", "Technical Interview": "Specialist",
                "Cultural Fit": "HR Manager", "Offer Declined": "System"
            }
        }

        activity_name = get_activity_name(activity, process_name)
        return role_mapping.get(process_name, {}).get(
            activity_name,
            random.choice(["Clerk", "Manager", "System", "Analyst", "Specialist"]),
        )

    def _get_resource_for_activity(self, activity: str, process_name: str) -> str:
        resource_mapping = {
            "OrderFulfillment": {
                "Order Created": "System", "Payment Processing": "Finance System",
                "Card Verification": "Finance System",
                "Payment Received": "Finance System", "Payment Failed": "Finance System",
                "Payment Retry": "Finance System",
                "Inventory Check": "System", "Backorder Created": "System", "Inventory Arrival": "System",
                "Pick Items": "R1", "Pack Items": "R2", "Quality Check": "R3", "Ship Order": "External",
                "Order Completed": "System", "Order Cancelled": "System", "Return Request": "System",
                "Refund Processing": "Finance System", "Order Closed": "System", "Re-pick Items": "R1",
                "Re-pack Items": "R2"
            },
            "CustomerSupport": {
                "Ticket Created": "System", "Initial Triage": "Support System", "Issue Investigation": "Support System",
                "Solution Provided": "Support System", "Ticket Closed": "System", "Escalated": "Support System",
                "Expert Review": "R4", "Customer Feedback": "Support System", "Additional Support": "Support System",
                "Cannot Resolve": "System", "Additional Escalation": "Support System"
            },
            "LoanApplication": {
                "Application Submitted": "System", "Document Review": "R1", "Credit Check": "Finance System",
                "Loan Approval": "System", "Funds Disbursed": "Finance System", "Additional Info Requested": "System",
                "Info Received": "System", "Loan Rejected": "System", "Auto-Approval": "System",
                "Underwriting": "R2", "Conditional Approval": "System", "Conditions Met": "System",
                "Application Withdrawn": "System"
            },
            "InvoiceProcessing": {
                "Invoice Received": "System", "Data Entry": "R1", "Validation": "System",
                "Approval": "System", "Payment Processed": "Finance System", "Archived": "System",
                "Validation Failed": "System", "Correction": "R1", "Re-validation": "System",
                "Invoice Rejected": "System", "Auto-Processing": "System", "Expedited Review": "System",
                "Manual Review": "System"
            },
            "HRRecruitment": {
                "Position Opened": "HR System", "Application Review": "HR System", "Phone Screen": "R2",
                "Interview": "R2", "Offer Extended": "System", "Offer Accepted": "HR System",
                "Hired": "HR System", "Candidate Rejected": "HR System", "Technical Interview": "R3",
                "Cultural Fit": "R2", "Offer Declined": "System"
            }
        }

        activity_name = get_activity_name(activity, process_name)
        return resource_mapping.get(process_name, {}).get(
            activity_name,
            random.choice(["System", "R1", "R2", "R3", "Auto", "External"]),
        )

    def generate_multiple_cases(self, process_name: str, num_cases: int,
                                start_time: Optional[datetime] = None,
                                anomaly_rate: float = 0.03, rework_rate: float = 0.08) -> List[Dict]:
        num_cases = int(num_cases)
        all_events = []
        base_time = start_time or (datetime.now() - timedelta(days=random.randint(0, 730)))

        for i in range(num_cases):
            time_offset = timedelta(
                hours=random.randint(0, 24 * 7),
                minutes=random.randint(0, 60),
                seconds=random.randint(0, 60),
            )
            case_start = base_time + time_offset

            events = self.generate_case(
                process_name=process_name,
                start_time=case_start,
                anomaly_rate=anomaly_rate,
                rework_rate=rework_rate,
            )
            all_events.extend(events)

            if num_cases > 10000 and (i + 1) % 10000 == 0 and self.logger:
                self.logger.info("   Сгенерировано %d/%d кейсов", i + 1, num_cases)

        return all_events
