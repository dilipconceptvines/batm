### app/tlc/tasks.py

"""
Celery Task Definitions for the TLC Violations Module.

This file ensures that tasks defined within the tlc module are discoverable by
the Celery worker. Although TLC violations are posted immediately, these tasks
could be used in the future for re-processing failed associations or ledger postings.
"""

# This file is created for consistency and future expansion. Currently, all ledger
# posting for the TLC module is handled synchronously within the service layer
# as per the requirement for immediate posting. If a batch processing or retry
# mechanism is needed in the future, the tasks would be defined and imported here.

__all__ = []