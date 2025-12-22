### app/worker/start_beat.py

"""
Celery Beat Startup Script

This script starts celery beat scheduler which triggers periodic tasks
according to the schedule defined in config.py
"""

# Local imports
from app.worker.app import app
from app.core.config import settings

def start_beat():
    """Start the celery beat scheduler."""

    # Beat configuration
    argv = [
        "beat",
        "--loglevel=info",
        "--scheduler=celery.beat:PersistentScheduler", # Use persistent scheduler for scheduled tasks
        "--schedule=/tmp/celerybeat-schedule", # Schedule file location
        "--pidfile=/tmp/celerybeat.pid", # PID file location
    ]

    print("Starting Celery Beat Scheduler ...")
    print(f"Redis URL: redis://{settings.redis_host}:{settings.redis_port}/0")
    print("Scheduled tasks:")

    # Display the configured schedules
    for task_name, task_config in app.conf.beat_schedule.items():
        schedule = task_config["schedule"]
        task = task_config["task"]
        print(f"- {task_name}: {task} -> {schedule}")

    # Start the beat scheduler
    app.start(argv)

if __name__ == "__main__":
    start_beat()