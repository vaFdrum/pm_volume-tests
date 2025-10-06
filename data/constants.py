from enum import Enum


class Role(Enum):
    CLERK = "Clerk"
    MANAGER = "Manager"
    SYSTEM = "System"
    ANALYST = "Analyst"
    SPECIALIST = "Specialist"
    AUDITOR = "Auditor"
    COORDINATOR = "Coordinator"
    SUPPORT_AGENT = "Support Agent"
    LOAN_OFFICER = "Loan Officer"
    HR_MANAGER = "HR Manager"


class Resource(Enum):
    R1 = "R1"
    R2 = "R2"
    R3 = "R3"
    R4 = "R4"
    R5 = "R5"
    SYSTEM = "System"
    AUTO = "Auto"
    EXTERNAL = "External"
    AI_PROCESSOR = "AI_Processor"
    SUPPORT_SYSTEM = "Support System"
    FINANCE_SYSTEM = "Finance System"
    HR_SYSTEM = "HR System"

DEPARTMENTS = ["IT", "HR", "Finance", "Operations", "Sales", "Marketing", "Support", "Logistics", "Procurement"]

PRIORITIES = ["low", "medium", "high", "critical", "urgent"]

# Базовые fieldnames для CSV
BASE_CSV_FIELDS = [
    "case_id", "timestamp_start", "timestamp_end", "process",
    "activity", "duration_minutes", "role", "resource",
    "anomaly", "anomaly_type", "rework"
]

# Дополнительные fieldnames для CSV
EXTENDED_CSV_FIELDS = [
    "user_id", "department", "priority", "cost", "comment",
    "resource_usage", "processing_time", "queue_time",
    "success_rate", "error_count"
]

ADDITIONAL_FIELDS = [
    "client_id", "product_id", "contract_id",
    "branch_code", "service_tier"
]

# Полный список fieldnames
CSV_FIELD_NAMES = BASE_CSV_FIELDS + EXTENDED_CSV_FIELDS + ADDITIONAL_FIELDS

# Диапазоны длительностей
ACTIVITY_DURATIONS = {
    # OrderFulfillment
    "Order Created": (1, 10),
    "Payment Processing": (5, 45),
    "Card Verification": (2, 15),
    "Payment Received": (1, 8),
    "Payment Failed": (1, 8),
    "Payment Retry": (5, 20),
    "Inventory Check": (5, 45),
    "Backorder Created": (1, 8),
    "Inventory Arrival": (1, 8),
    "Pick Items": (10, 60),
    "Pack Items": (5, 35),
    "Quality Check": (8, 50),
    "Ship Order": (3, 20),
    "Order Completed": (1, 8),
    "Order Cancelled": (2, 12),
    "Return Request": (5, 25),
    "Refund Processing": (10, 75),
    "Order Closed": (1, 8),
    "Re-pick Items": (8, 40),
    "Re-pack Items": (5, 30),

    # CustomerSupport
    "Ticket Created": (1, 8),
    "Initial Triage": (5, 40),
    "Issue Investigation": (15, 120),
    "Solution Provided": (5, 35),
    "Ticket Closed": (1, 8),
    "Escalated": (3, 20),
    "Expert Review": (20, 150),
    "Customer Feedback": (5, 30),
    "Additional Support": (10, 60),
    "Cannot Resolve": (10, 40),
    "Additional Escalation": (15, 60),

    # LoanApplication
    "Application Submitted": (2, 15),
    "Document Review": (30, 240),
    "Credit Check": (5, 40),
    "Loan Approval": (15, 80),
    "Funds Disbursed": (1, 8),
    "Additional Info Requested": (5, 25),
    "Info Received": (1, 8),
    "Loan Rejected": (3, 18),
    "Auto-Approval": (1, 8),
    "Underwriting": (60, 300),
    "Conditional Approval": (30, 150),
    "Conditions Met": (15, 75),
    "Application Withdrawn": (5, 20),

    # InvoiceProcessing
    "Invoice Received": (1, 8),
    "Data Entry": (10, 65),
    "Validation": (10, 60),
    "Approval": (15, 75),
    "Payment Processed": (1, 8),
    "Archived": (1, 8),
    "Validation Failed": (5, 25),
    "Correction": (8, 45),
    "Re-validation": (5, 30),
    "Invoice Rejected": (3, 18),
    "Auto-Processing": (1, 8),
    "Expedited Review": (5, 25),
    "Manual Review": (15, 75),

    # HRRecruitment
    "Position Opened": (5, 35),
    "Application Review": (20, 120),
    "Phone Screen": (30, 75),
    "Interview": (45, 150),
    "Offer Extended": (10, 50),
    "Offer Accepted": (1, 8),
    "Hired": (1, 8),
    "Candidate Rejected": (3, 18),
    "Technical Interview": (60, 150),
    "Cultural Fit": (45, 120),
    "Offer Declined": (5, 20),

    # Межсезонные активности
    "Data Analysis": (30, 300),
    "System Maintenance": (60, 360),
    "Quality Audit": (45, 240),
    "Process Optimization": (120, 480),
    "Training Session": (120, 420),
    "Performance Review": (60, 240),
    "Equipment Calibration": (30, 150),
    "Documentation Update": (60, 300)
}

ANOMALY_DURATIONS = {
    "Manual Override": (30, 150),
    "Fraud Investigation": (240, 1800),
    "System Outage": (60, 600),
    "Quality Issue": (15, 240),
    "Technical Problem": (30, 300),
    "Data Inconsistency": (60, 450),
}

ANOMALY_ACTIVITIES = {
    "Manual Override": [
        "Payment Processing", "Document Review", "Invoice Approval", "Loan Approval",
        "Approval", "Underwriting", "Conditional Approval"
    ],
    "Fraud Investigation": [
        "Payment Processing", "Credit Check", "Application Review", "Document Review",
        "Validation", "Underwriting"
    ],
    "System Outage": [
        "Payment Processing", "Data Entry", "Application Review", "Ticket Created",
        "Auto-Processing", "Credit Check"
    ],
    "Quality Issue": [
        "Quality Check", "Pick Items", "Document Review", "Data Entry",
        "Validation", "Phone Screen", "Interview"
    ],
    "Technical Problem": [
        "Issue Investigation", "Credit Check", "Data Entry", "Payment Processing",
        "Underwriting", "Technical Interview"
    ],
    "Data Inconsistency": [
        "Document Review", "Data Entry", "Application Review", "Invoice Received",
        "Validation", "Info Received"
    ],
}

REWORK_ACTIVITIES = {
    "Re-check": [
        "Quality Check", "Document Review", "Data Entry", "Credit Check",
        "Validation", "Phone Screen", "Technical Interview"
    ],
    "Re-approval": [
        "Invoice Approval", "Loan Approval", "Offer Extended", "Approval",
        "Underwriting", "Conditional Approval"
    ],
    "Additional Verification": [
        "Document Review", "Application Review", "Credit Check", "Validation",
        "Underwriting", "Expert Review"
    ],
}

ACTIVITY_ALIASES = {
    "Approval": {
        "OrderFulfillment": "Order Completed",
        "LoanApplication": "Loan Approval",
        "InvoiceProcessing": "Invoice Approval",
        "HRRecruitment": "Offer Extended",
    },
    "Rejected": {
        "LoanApplication": "Loan Rejected",
        "InvoiceProcessing": "Invoice Rejected",
        "HRRecruitment": "Candidate Rejected",
    },
}

INTER_SEASON_ACTIVITIES = {
    "Data Analysis": (30, 300),
    "System Maintenance": (60, 360),
    "Quality Audit": (45, 240),
    "Process Optimization": (120, 480),
    "Training Session": (120, 420),
    "Performance Review": (60, 240),
    "Equipment Calibration": (30, 150),
    "Documentation Update": (60, 300)
}


def get_activity_name(activity: str, process_name: str) -> str:
    """
    Возвращает правильное имя активности с учетом контекста процесса
    """
    if activity in ACTIVITY_ALIASES:
        return ACTIVITY_ALIASES[activity].get(process_name, activity)
    return activity
