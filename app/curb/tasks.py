### app/curb/tasks.py

"""
Celery Task Definitions for the CURB Module.

This file ensures that tasks defined in other parts of the curb module,
such as services, are discoverable by the Celery worker.

By importing them here, we provide a single, clear entry point for Celery's
autodiscovery mechanism as configured in `app/core/celery_app.py`.
"""

# Local imports
# from app.curb.services import (
#     fetch_and_import_curb_trips_task,
#     post_earnings_to_ledger_task,
#     import_driver_data_task,
#     import_medallion_data_task,
#     import_filtered_data_task,
# )

# from app.curb.import_raw_curb_data import (
#     import_raw_curb_data_task,
#     import_and_process_from_s3_task,
#     fetch_and_process_chained,
#     test_import,
#     map_reconciled_trips_task,
# )
from app.curb.curb_sync_tasks import (
    fetch_trips_to_s3_task,
    parse_and_map_trips_task,
    fetch_transactions_to_s3_task,
    parse_and_map_transactions_task,
    curb_full_sync_chain_task
)

from app.curb.services import (
    post_earnings_to_ledger_task,
)

# The tasks are defined in the services module using the @app.task decorator.
# We simply import them here to make them available to Celery.
# This keeps the task logic co-located with the service that performs the work.

__all__ = [
    # "import_raw_curb_data_task",
    # "import_and_process_from_s3_task",
    # "fetch_and_process_chained",
    # "test_import",
    # "map_reconciled_trips_task",

    "fetch_trips_to_s3_task",
    "parse_and_map_trips_task",
    "fetch_transactions_to_s3_task",
    "parse_and_map_transactions_task",
    "curb_full_sync_chain_task",
    "post_earnings_to_ledger_task",
]
