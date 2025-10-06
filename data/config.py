from enum import Enum

class Season(Enum):
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4

# Процессные модели с матрицами переходов
PROCESS_MODELS = {
    "OrderFulfillment": {
        "scenarios": [
            ["Order Created", "Payment Processing", "Payment Received", "Inventory Check",
             "Pick Items", "Pack Items", "Quality Check", "Ship Order", "Order Completed"],
            ["Order Created", "Payment Processing", "Card Verification", "Payment Received",
             "Inventory Check", "Pick Items", "Pack Items", "Ship Order", "Order Completed"],
            ["Order Created", "Payment Processing", "Payment Failed", "Payment Retry",
             "Payment Received", "Inventory Check", "Pick Items", "Pack Items", "Ship Order", "Order Completed"],
            ["Order Created", "Payment Processing", "Order Cancelled"],
            ["Order Created", "Payment Processing", "Payment Received", "Inventory Check",
             "Pick Items", "Pack Items", "Ship Order", "Return Request", "Refund Processing", "Order Closed"],
            ["Order Created", "Payment Processing", "Payment Received", "Backorder Created",
             "Inventory Arrival", "Pick Items", "Pack Items", "Ship Order", "Order Completed"]
        ],
        "weights": [0.55, 0.2, 0.1, 0.05, 0.05, 0.05],
        "transition_matrix": {
            "Order Created": {"Payment Processing": 1.0},
            "Payment Processing": {"Payment Received": 0.8, "Card Verification": 0.1, "Payment Failed": 0.07, "Order Cancelled": 0.03},
            "Card Verification": {"Payment Received": 0.9, "Payment Failed": 0.1},
            "Payment Received": {"Inventory Check": 1.0},
            "Payment Failed": {"Payment Retry": 0.7, "Order Cancelled": 0.3},
            "Payment Retry": {"Payment Received": 0.8, "Payment Failed": 0.2},
            "Inventory Check": {"Pick Items": 0.85, "Backorder Created": 0.15},
            "Backorder Created": {"Inventory Arrival": 1.0},
            "Inventory Arrival": {"Pick Items": 1.0},
            "Pick Items": {"Pack Items": 1.0},
            "Pack Items": {"Quality Check": 1.0},
            "Quality Check": {"Ship Order": 0.92, "Re-pick Items": 0.06, "Re-pack Items": 0.02},
            "Ship Order": {"Order Completed": 0.96, "Return Request": 0.04},
            "Return Request": {"Refund Processing": 1.0},
            "Refund Processing": {"Order Closed": 1.0},
            "Order Completed": {},
            "Order Cancelled": {},
            "Order Closed": {}
        }
    },
    "CustomerSupport": {
        "scenarios": [
            ["Ticket Created", "Initial Triage", "Solution Provided", "Ticket Closed"],
            ["Ticket Created", "Initial Triage", "Issue Investigation", "Solution Provided", "Ticket Closed"],
            ["Ticket Created", "Initial Triage", "Escalated", "Expert Review", "Solution Provided", "Ticket Closed"],
            ["Ticket Created", "Initial Triage", "Customer Feedback", "Additional Support", "Solution Provided", "Ticket Closed"],
            ["Ticket Created", "Initial Triage", "Issue Investigation", "Cannot Resolve", "Ticket Closed"]
        ],
        "weights": [0.5, 0.3, 0.1, 0.07, 0.03],
        "transition_matrix": {
            "Ticket Created": {"Initial Triage": 1.0},
            "Initial Triage": {"Solution Provided": 0.4, "Issue Investigation": 0.35, "Escalated": 0.15, "Customer Feedback": 0.1},
            "Issue Investigation": {"Solution Provided": 0.7, "Escalated": 0.2, "Cannot Resolve": 0.1},
            "Escalated": {"Expert Review": 1.0},
            "Expert Review": {"Solution Provided": 0.9, "Additional Escalation": 0.1},
            "Customer Feedback": {"Additional Support": 0.8, "Solution Provided": 0.2},
            "Additional Support": {"Solution Provided": 0.9, "Customer Feedback": 0.1},
            "Solution Provided": {"Ticket Closed": 0.95, "Customer Feedback": 0.05},
            "Cannot Resolve": {"Ticket Closed": 1.0},
            "Ticket Closed": {}
        }
    },
    "LoanApplication": {
        "scenarios": [
            ["Application Submitted", "Document Review", "Credit Check", "Underwriting", "Loan Approval", "Funds Disbursed"],
            ["Application Submitted", "Auto-Approval", "Funds Disbursed"],
            ["Application Submitted", "Document Review", "Additional Info Requested", "Info Received", "Credit Check", "Underwriting", "Loan Approval", "Funds Disbursed"],
            ["Application Submitted", "Document Review", "Credit Check", "Loan Rejected"],
            ["Application Submitted", "Document Review", "Credit Check", "Underwriting", "Conditional Approval", "Conditions Met", "Funds Disbursed"]
        ],
        "weights": [0.4, 0.2, 0.15, 0.15, 0.1],
        "transition_matrix": {
            "Application Submitted": {"Document Review": 0.8, "Auto-Approval": 0.2},
            "Auto-Approval": {"Funds Disbursed": 1.0},
            "Document Review": {"Credit Check": 0.7, "Additional Info Requested": 0.25, "Loan Rejected": 0.05},
            "Additional Info Requested": {"Info Received": 0.9, "Application Withdrawn": 0.1},
            "Info Received": {"Credit Check": 1.0},
            "Credit Check": {"Underwriting": 0.6, "Loan Rejected": 0.3, "Auto-Approval": 0.1},
            "Underwriting": {"Loan Approval": 0.7, "Conditional Approval": 0.2, "Loan Rejected": 0.1},
            "Conditional Approval": {"Conditions Met": 0.8, "Loan Rejected": 0.2},
            "Conditions Met": {"Funds Disbursed": 1.0},
            "Loan Approval": {"Funds Disbursed": 1.0},
            "Funds Disbursed": {},
            "Loan Rejected": {},
            "Application Withdrawn": {}
        }
    },
    "InvoiceProcessing": {
        "scenarios": [
            ["Invoice Received", "Data Entry", "Validation", "Approval", "Payment Processed", "Archived"],
            ["Invoice Received", "Data Entry", "Validation Failed", "Correction", "Re-validation", "Approval", "Payment Processed", "Archived"],
            ["Invoice Received", "Auto-Processing", "Payment Processed", "Archived"],
            ["Invoice Received", "Data Entry", "Validation", "Invoice Rejected"],
            ["Invoice Received", "Expedited Review", "Approval", "Payment Processed", "Archived"]
        ],
        "weights": [0.6, 0.2, 0.1, 0.07, 0.03],
        "transition_matrix": {
            "Invoice Received": {"Data Entry": 0.87, "Auto-Processing": 0.1, "Expedited Review": 0.03},
            "Data Entry": {"Validation": 1.0},
            "Validation": {"Approval": 0.88, "Validation Failed": 0.1, "Invoice Rejected": 0.02},
            "Validation Failed": {"Correction": 1.0},
            "Correction": {"Re-validation": 1.0},
            "Re-validation": {"Approval": 0.95, "Validation Failed": 0.05},
            "Auto-Processing": {"Payment Processed": 0.99, "Manual Review": 0.01},
            "Expedited Review": {"Approval": 0.98, "Validation": 0.02},
            "Approval": {"Payment Processed": 1.0},
            "Payment Processed": {"Archived": 1.0},
            "Archived": {},
            "Invoice Rejected": {},
            "Manual Review": {"Approval": 0.7, "Invoice Rejected": 0.3}
        }
    },
    "HRRecruitment": {
        "scenarios": [
            ["Position Opened", "Application Review", "Phone Screen", "Interview", "Offer Extended", "Offer Accepted", "Hired"],
            ["Position Opened", "Application Review", "Interview", "Offer Extended", "Offer Accepted", "Hired"],
            ["Position Opened", "Application Review", "Phone Screen", "Technical Interview", "Cultural Fit", "Offer Extended", "Offer Accepted", "Hired"],
            ["Position Opened", "Application Review", "Candidate Rejected"],
            ["Position Opened", "Application Review", "Phone Screen", "Interview", "Offer Extended", "Offer Declined"]
        ],
        "weights": [0.5, 0.2, 0.15, 0.1, 0.05],
        "transition_matrix": {
            "Position Opened": {"Application Review": 1.0},
            "Application Review": {"Phone Screen": 0.6, "Interview": 0.2, "Candidate Rejected": 0.2},
            "Phone Screen": {"Interview": 0.7, "Technical Interview": 0.2, "Candidate Rejected": 0.1},
            "Interview": {"Offer Extended": 0.6, "Cultural Fit": 0.3, "Candidate Rejected": 0.1},
            "Technical Interview": {"Cultural Fit": 0.7, "Offer Extended": 0.2, "Candidate Rejected": 0.1},
            "Cultural Fit": {"Offer Extended": 0.8, "Candidate Rejected": 0.2},
            "Offer Extended": {"Offer Accepted": 0.85, "Offer Declined": 0.15},
            "Offer Accepted": {"Hired": 1.0},
            "Hired": {},
            "Candidate Rejected": {},
            "Offer Declined": {}
        }
    }
}

SCENARIO_WEIGHTS = {
    process: model["weights"] for process, model in PROCESS_MODELS.items()
}

WAITING_TIMES = {
    "OrderFulfillment": (5, 180),
    "CustomerSupport": (30, 480),
    "LoanApplication": (120, 1440),
    "InvoiceProcessing": (60, 360),
    "HRRecruitment": (240, 2880)
}

# Сезонные множители
SEASONAL_MULTIPLIERS = {
    "OrderFulfillment": {Season.Q1: 0.8, Season.Q2: 0.9, Season.Q3: 1.0, Season.Q4: 2.0},
    "CustomerSupport": {Season.Q1: 1.3, Season.Q2: 1.1, Season.Q3: 1.0, Season.Q4: 1.8},
    "LoanApplication": {Season.Q1: 1.3, Season.Q2: 1.1, Season.Q3: 0.9, Season.Q4: 0.9},
    "InvoiceProcessing": {Season.Q1: 1.2, Season.Q2: 1.1, Season.Q3: 1.0, Season.Q4: 1.5},
    "HRRecruitment": {Season.Q1: 1.4, Season.Q2: 1.2, Season.Q3: 1.0, Season.Q4: 0.8}
}

CONFIG_20GB = {
    "target_size_gb": 20.0,
    "output_dir": "./dataset/",
    "process_distribution": {
        "OrderFulfillment": 0.4,
        "CustomerSupport": 0.25,
        "LoanApplication": 0.15,
        "InvoiceProcessing": 0.12,
        "HRRecruitment": 0.08,
    },
    "anomaly_rate": 0.03,
    "rework_rate": 0.08,
    "max_cases": None,
    "time_range_days": 365 * 3,
    "start_date": "2022-01-01",
}

CONFIG_30GB = {
    "target_size_gb": 30.0,
    "output_dir": "./dataset/",
    "process_distribution": {
        "OrderFulfillment": 0.35,
        "CustomerSupport": 0.25,
        "LoanApplication": 0.15,
        "InvoiceProcessing": 0.15,
        "HRRecruitment": 0.10,
    },
    "anomaly_rate": 0.025,
    "rework_rate": 0.07,
    "max_cases": None,
    "time_range_days": 365 * 5,
    "start_date": "2021-01-01",
}

CONFIG_50GB = {
    "target_size_gb": 50.0,
    "output_dir": "./dataset/",
    "process_distribution": {
        "OrderFulfillment": 0.3,
        "CustomerSupport": 0.25,
        "LoanApplication": 0.2,
        "InvoiceProcessing": 0.15,
        "HRRecruitment": 0.10,
    },
    "anomaly_rate": 0.02,
    "rework_rate": 0.06,
    "max_cases": None,
    "time_range_days": 365 * 7,
    "start_date": "2019-01-01",
}

CONFIG_CUSTOM = {
    "target_size_gb": 1.0,
    "output_dir": "./dataset/",
    "process_distribution": {
        "OrderFulfillment": 0.5,
        "CustomerSupport": 0.3,
        "LoanApplication": 0.15,
        "InvoiceProcessing": 0.05,
    },
    "anomaly_rate": 0.03,
    "rework_rate": 0.08,
    "max_cases": None,
    "start_date": "2022-01-01",
}
BATCH_SIZE = 10000
