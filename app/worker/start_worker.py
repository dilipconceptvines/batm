### app/worker/start_worker.py

"""
Celery worker startup script

This script starts the celery worker with appropriate configuration.
Worker processes tasks from the queues.
"""

# Standard library imports
import sys

# Local imports
from app.core.config import settings
from app.worker.app import app


def start_worker():
    """Start the celery worker."""

    # Worker configuration
    argv = [
        "worker",
        "--loglevel=info",  # Loglevel (debug, info, warning, error, critical)
        "--concurrency=4",  # Number of concurrent workers (4 is balanced for mixed workload)
        # Consider: 2-3 for fewer resources, 6-8 if you have 8+ CPU cores and 8GB+ RAM
        "--max-tasks-per-child=100",  # Number of tasks a worker can process before restarting
        "--time-limit=90000",  # Time limit for a task in seconds (25 hours for long imports)
        "--soft-time-limit=86400",  # Soft time limit for a task in seconds (24 hours)
        "--prefetch-multiplier=1",  # Prefetch multiplier for the worker
    ]

    print("Starting Celery worker ...")
    print(f"Redis URL: redis://{settings.redis_host}:{settings.redis_port}/0")
    print("Available task modules:")
    for module in ["app.worker", "app.curb"]:
        print(f"- {module}.tasks")

    # Start the worker
    app.worker_main(argv)


if __name__ == "__main__":
    start_worker()
