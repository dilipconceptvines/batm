## app/bpm/utils.py

# Standard imports
from datetime import datetime, timedelta


def calculate_time_due(case_created_on, sla_time_limit):
    """Calculate the time due for a case"""
    due_date = case_created_on + timedelta(minutes=sla_time_limit)

    current_time = datetime.now()
    time_left = due_date - current_time

    # If task is overdue ( difference is negative )
    if time_left.total_seconds() < 0:
        return {"due_date": due_date, "time_left": "Action is overdue"}

    # Determine if time left is in days, hours, or minutes
    if time_left > timedelta(days=1):  # More than 1 day left
        days_left = time_left.days
        hours_left = time_left.seconds // 3600
        minutes_left = (time_left.seconds // 60) % 60
        return {
            "due_date": due_date,
            "time_left": f"{days_left} days, {hours_left} hours, {minutes_left} minutes",
        }
    # Less than 1 day, but more than 1 hour left
    elif time_left > timedelta(hours=1):
        hours_left = time_left.seconds // 3600
        minutes_left = (time_left.seconds // 60) % 60
        return {
            "due_date": due_date,
            "time_left": f"{hours_left} hours, {minutes_left} minutes",
        }
    # Less than 1 hour, but more than 1 minute left
    elif time_left > timedelta(minutes=1):
        minutes_left = time_left.seconds // 60
        return {"due_date": due_date, "time_left": f"{minutes_left} minutes"}
    else:  # Less than 1 minute
        return {"due_date": due_date, "time_left": "Less than 1 minute"}
