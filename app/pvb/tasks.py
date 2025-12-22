### app/pvb/tasks.py

"""
Celery Task Definitions for the PVB Module.

This file ensures that tasks defined within the pvb module (e.g., in services.py)
are properly registered and discoverable by the Celery worker process.
"""

from app.pvb.services import (
    associate_pvb_violations_task,
    post_pvb_violations_to_ledger_task,
)

# The tasks themselves are defined in the services module using the @shared_task decorator.
# We import them here to provide a single, canonical location for Celery's autodiscovery
# mechanism, keeping the task logic co-located with its related business services.

__all__ = [
    "associate_pvb_violations_task",
    "post_pvb_violations_to_ledger_task",
]