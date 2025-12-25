### app/worker/config.py

"""
Celery configuration settings

This file contains all the Celery configurations including:
- Broker and result backend settings
- Task serialization settings
- Timezone configuration
- Beat schedule for periodic tasks
"""

# Third party imports
from celery.schedules import crontab

# Local imports
from app.core.config import settings

# Broker and result backend configurations
broker_url = settings.celery_broker
result_backend = settings.celery_backend

# Task serialization
task_serializer = "json"
accept_content = ["json"]
result_serializer = "json"
timezone = "UTC"
enable_utc = True

# Task settings
task_track_started = True
task_time_limit = 30 * 60 * 6  # 180 minutes
task_soft_time_limit = 25 * 60 * 6  # 150 minutes
worker_prefetch_multiplier = 1
task_acks_late = True
worker_disable_rate_limits = False

# Redis connection pool settings to prevent connection exhaustion
broker_connection_retry_on_startup = True
broker_connection_retry = True
broker_connection_max_retries = 10

# Redis connection pool configuration
redis_max_connections = 50
redis_socket_timeout = 10
redis_socket_connect_timeout = 10
redis_retry_on_timeout = True
redis_health_check_interval = 30

# Connection pool settings for both broker and backend
broker_transport_options = {
    "master_name": "localhost",
    "max_connections": 20,
    "socket_timeout": 10,
    "socket_connect_timeout": 10,
    "socket_keepalive": True,
    "socket_keepalive_options": {},
    "retry_on_timeout": True,
    "health_check_interval": 30,
}

result_backend_transport_options = {
    "master_name": "localhost",
    "max_connections": 20,
    "socket_timeout": 10,
    "socket_connect_timeout": 10,
    "socket_keepalive": True,
    "socket_keepalive_options": {},
    "retry_on_timeout": True,
    "health_check_interval": 30,
}


# Beat schedule configuration
# This defines when periodic tasks should run
beat_schedule = {
    # --- CURB Fetch, Upload to S3, and Process Pipeline (Daily) ---
    # ==========================================================================
    # CURB IMPORT - 8 INTERVALS PER DAY (Every 3 hours)
    # ==========================================================================
    # Runs at: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 EST/EDT
    #
    # Import Windows (each job imports the PREVIOUS 3-hour window):
    # - 00:00 run → imports 21:00-23:59:59 (previous day)
    # - 03:00 run → imports 00:00-02:59:59
    # - 06:00 run → imports 03:00-05:59:59
    # - 09:00 run → imports 06:00-08:59:59
    # - 12:00 run → imports 09:00-11:59:59
    # - 15:00 run → imports 12:00-14:59:59
    # - 18:00 run → imports 15:00-17:59:59
    # - 21:00 run → imports 18:00-20:59:59
    # ==========================================================================
    "curb-import-8-intervals-daily": {
        "task": "curb.import_trips_task",
        "schedule": crontab(minute=0, hour='0,3,6,9,12,15,18,21'),  # 8 times per day
        "options": {"timezone": "America/New_York"},
        "kwargs": {
            # Task will automatically use 3-hour window
            # No need to specify from_datetime/to_datetime
        }
    },
    
    # Alternative: If you want ALIGNED windows (run at end of each window)
    # This version runs at 02:59, 05:59, 08:59, etc. to import complete windows
    # "curb-import-aligned-windows": {
    #     "task": "curb.import_trips_aligned_task",  # Use the aligned version
    #     "schedule": crontab(minute=59, hour='2,5,8,11,14,17,20,23'),
    #     "options": {"timezone": "America/New_York"},
    #     "enabled": False,  # Enable this if you prefer aligned windows
    # },
    
    # ========================================================================
    # SUNDAY MORNING FINANCIAL PROCESSING CHAIN (REPLACES 5 INDIVIDUAL TASKS)
    # ========================================================================
    # This single chain orchestrates all Sunday morning financial tasks
    # to run sequentially, ensuring proper execution order and data integrity.
    #
    # Chain sequence:
    #   1. Post CURB earnings to ledger
    #   2. Post lease fees to ledger
    #   3. Post loan installments to ledger
    #   4. Post repair installments to ledger
    #   5. Generate DTRs for all active leases
    #
    # IMPORTANT: Each task waits for the previous task to complete.
    # ========================================================================
    "sunday-financial-chain": {
        "task": "worker.sunday_financial_chain",
        "schedule": crontab(
            hour=4, minute=0, day_of_week="sun"
        ),  # Runs every Sunday at 4:00 AM
        "options": {"timezone": "America/New_York"},
    },
    
    # ========================================================================
    # REMOVED INDIVIDUAL TASKS (Now part of the chain above):
    # ========================================================================
    # - curb-post-earnings-to-ledger (was 4:00 AM)
    # - post-weekly-lease-fees (was 5:00 AM)
    # - post-due-loan-installments (was 5:15 AM)
    # - post-due-repair-installments (was 5:30 AM)
    # - generate-weekly-dtrs (was 6:00 AM)
    # ========================================================================
    
    # --- BPM SLA Processing Task (Daily) ---
    "process-case-sla": {
        "task": "bpm.sla.process_case_sla",
        "schedule": crontab(hour=1, minute=0),  # Runs daily at 1:00 AM
        "options": {"timezone": "America/New_York"},
    },
}

# Worker configuration
worker_hijack_root_logger = False
worker_log_color = False
