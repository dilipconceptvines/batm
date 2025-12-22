### app/worker/run_task.py

"""
Task Runner Script

This script allows you to manually trigger tasks for testing purposes.
"""

# Standard library imports
import sys

# Local imports
from app.worker.app import app

def run_task():
    """Run a task manually."""

    if len(sys.argv) != 2:
        print("Usage: python -m app.worker.run_task <task_name>")
        print("\nAvailable tasks:")
        print("\n-*- -*- -*- CURB -*- -*- -*-")
        print("- app.curb.tasks.fetch_and_reconcile_curb_trips")
        print("- app.curb.tasks.reconcile_curb_trips_only")
        print("- app.curb.tasks.post_curb_trips_only")
        return

    task_name = sys.argv[1]
    task_args = sys.argv[2:] if len(sys.argv) > 2 else []

    try:
        # Send the task to queue
        result = app.send_task(task_name, args=task_args)
        print(f"Task sent: {task_name}")
        print(f"Task ID: {result.id}")
        print(f"Arguments: {task_args}")
        print("Use 'python -m app.worker.result <task_id>' to check the result.")
    
    except Exception as e:
        print(f"Error running task: {e}")

if __name__ == "__main__":
    run_task()
