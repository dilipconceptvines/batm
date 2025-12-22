### app/worker/result.py

"""
Task Result Checker

This script checks the result of a task by its ID.
"""
# Standard library imports
import sys

# Local imports
from app.worker.app import app

def check_result():
    """Check the result of the task."""
    if len(sys.argv) != 2:
        print("Usage: python -m app.worker.result <task_id>")
        sys.exit(1)

    task_id = sys.argv[1]

    try:
        result = app.AsyncResult(task_id)

        print(f"Task ID: {task_id}")
        print(f"Task Status: {result.status}")

        if result.ready():
            if result.successful():
                print(f"Task Result: {result.result}")
            else:
                print(f"Task failed: {result.result}")
                if result.traceback:
                    print(f"Traceback: {result.traceback}")
        else:
            print("Task is still pending ...")
    except Exception as e:
        print(f"Error checking the task result: {e}")

if __name__ == "__main__":
    check_result()

            

