## app/core/celery_app.py

"""
Main Celery Application Configuration

This file sets up the main Celery application instance with Redis as broker and result backend.
It integrates with the existing worker configuration and includes all task modules.
"""

# Third party imports
from celery import Celery

# Create Celery Instance
app = Celery("BAT_scheduler")

# Configure celery from separate config file
app.config_from_object("app.worker.config")

# Auto discover tasks from different modules
# This will look for tasks.py files in specified modules/packages
app.autodiscover_tasks([
    "app.notifications",
    "app.worker",
    "app.curb",
    "app.bpm.sla",
    "app.driver_payments",
    "app.leases", 
    "app.pvb",
    "app.ezpass",
    "app.loans",
    "app.repairs",
    "app.tlc",
])

if __name__ == "__main__":
    app.start() 