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
from app.scheduler.beat_config import generate_beat_schedule_from_yaml

# Broker and result backend configurations
broker_url = settings.celery_broker
result_backend = settings.celery_backend

# Task serialization
task_serializer = "json"
accept_content = ["json"]
result_serializer = "json"
timezone = "America/New_York"
enable_utc = False

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


# ========================================================================
# Beat schedule configuration
# This defines when periodic tasks should run
beat_schedule = generate_beat_schedule_from_yaml()

# Worker configuration
worker_hijack_root_logger = False
worker_log_color = False
