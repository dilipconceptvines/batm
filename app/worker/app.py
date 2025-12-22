### app/worker/app.py

"""
Main Celery Application Configuration

This file sets up the Celery application instance with Redis as broker and result backend.
It also handles task discovery from multiple modules and configures timezone settings.
"""

# Third party imports
from celery import Celery

# Import all models to ensure they're registered with SQLAlchemy
# This must happen before any database operations in tasks
import app.drivers.models
import app.leases.models
import app.vehicles.models
import app.medallions.models
import app.curb.models
import app.driver_payments.models
import app.dtr.models
import app.users.models
import app.audit_trail.models

# Create Celery Instance
app = Celery("BAT_scheduler")

# Configure celery from separate config file
app.config_from_object("app.worker.config")

# Auto discover tasks from different modules
# This will look for tasks.py files in specified modules/packages
app.autodiscover_tasks([
    "app.worker",
    "app.curb",
    "app.pvb",
    "app.ezpass",
    "app.loans",
    "app.repairs",
    "app.tlc",
    "app.ledger",
    "app.driver_payments",
    "app.leases",
    # "app.dtr",
])

from app.worker import sunday_chain

if __name__ == "__main__":
    app.start()
