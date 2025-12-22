import logging

from celery import Celery
from celery.schedules import crontab  # For scheduling periodic tasks

# Setup custom logging
logger = logging.getLogger('celery.beat')  # Specifically for Beat logs
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(message)s')
ch.setFormatter(formatter)

logger.addHandler(ch)


# Initialize Celery app
app = Celery('tasks', broker='redis://localhost:6379/10')

# Configure Celery to run periodic tasks
app.conf.update(
    beat_schedule={
        'run-every-minute': {
            'task': 'app.bpm.sla.tasks.process_case_sla',
            'schedule': crontab(minute='*/1'),  # Run every 1 minutes
        }
    },
    include=['app.bpm.sla.tasks']
)

app.conf.timezone = 'UTC'

# Set the beat scheduler to check every 10 seconds
# app.conf.beat_scheduler = 'celery.beat.PersistentScheduler'
# app.conf.beat_max_loop_interval = 10  # Checks for new tasks every 10 seconds
