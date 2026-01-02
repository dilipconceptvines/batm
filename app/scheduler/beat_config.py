"""
Celery Beat schedule generation from batcron.yaml configuration.

This module reads the batcron.yaml file and generates Celery Beat schedule entries
for all active notification events.
"""

from typing import Any, Dict

from celery.schedules import crontab, schedule

from app.core.config import settings
from app.scheduler.events_config import load_events_yaml
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)


def generate_beat_schedule_from_yaml() -> Dict[str, Dict[str, Any]]:
    """
    Load batcron.yaml and generate Celery Beat schedule configuration.

    Returns:
        Dictionary of Celery Beat schedule entries
    """
    logger.info(f"Loading events yaml from {settings.events_config_path}")

    try:
        yaml_content = s3_utils.download_file(settings.events_config_path)
        if not yaml_content:
            logger.error("YAML file is empty")
            return {}

        cfg = load_events_yaml(yaml_content)
        logger.info(f"Loaded YAML with timezone: {cfg.timezone}, total events: {len(cfg.events)}")

        beat_schedule = {}
        active_count = 0
        inactive_count = 0

        for event_key, event_config in cfg.events.items():
            category = event_config.get("category", "unknown")
            is_active = event_config.get("active", False)

            # Log event parsing start
            logger.info(
                f"Parsing event: {event_key}",
                category=category,
                active=is_active,
            )

            # Skip inactive events
            if not is_active:
                logger.info(f"⏸️  SKIPPED - Inactive event: {event_key} (category: {category})")
                inactive_count += 1
                continue

            # Get schedule configuration
            run_schedule = event_config.get("run_schedule")
            if not run_schedule:
                logger.warning(f"⚠️  SKIPPED - No run_schedule defined for event: {event_key}")
                continue

            schedule_type = run_schedule.get("type")

            # Generate Celery schedule based on type
            celery_schedule = None
            schedule_description = ""

            if schedule_type == "cron":
                cron_config = run_schedule.get("cron", {})
                minute = cron_config.get("minute", "*")
                hour = cron_config.get("hour", "*")
                day_of_week = cron_config.get("day_of_week", "*")
                day_of_month = cron_config.get("day_of_month", "*")
                month_of_year = cron_config.get("month_of_year", "*")

                celery_schedule = crontab(
                    minute=minute,
                    hour=hour,
                    day_of_week=day_of_week,
                    day_of_month=day_of_month,
                    month_of_year=month_of_year,
                )

                # Build human-readable schedule description
                schedule_parts = []
                if day_of_week != "*":
                    schedule_parts.append(f"day_of_week={day_of_week}")
                if day_of_month != "*":
                    schedule_parts.append(f"day_of_month={day_of_month}")
                if month_of_year != "*":
                    schedule_parts.append(f"month={month_of_year}")
                schedule_parts.append(f"hour={hour}")
                schedule_parts.append(f"minute={minute}")

                schedule_description = f"cron({', '.join(schedule_parts)})"

                logger.info(
                    f"📅 CRON schedule created for {event_key}",
                    category=category,
                    schedule=schedule_description,
                    timezone=cfg.timezone,
                )

            elif schedule_type == "interval":
                every_minutes = run_schedule.get("every_minutes")
                if every_minutes:
                    celery_schedule = schedule(run_every=every_minutes * 60)  # Convert to seconds
                    schedule_description = f"interval(every {every_minutes} minutes)"

                    logger.info(
                        f"⏱️  INTERVAL schedule created for {event_key}",
                        category=category,
                        schedule=schedule_description,
                        every_minutes=every_minutes,
                    )
                else:
                    logger.warning(
                        f"⚠️  SKIPPED - No every_minutes defined for interval schedule: {event_key}"
                    )
                    continue
            else:
                logger.warning(
                    f"⚠️  SKIPPED - Unknown schedule type '{schedule_type}' for event: {event_key}"
                )
                continue

            if celery_schedule:
                # Determine the task to run
                task_name = event_config.get("task", "app.tasks.notifications.evaluate_notification")
                task_type = "notification" if task_name == "app.tasks.notifications.evaluate_notification" else "job"

                # Build beat schedule entry
                beat_entry = {
                    "task": task_name,
                    "schedule": celery_schedule,
                    "options": {"timezone": cfg.timezone},
                }

                # Add kwargs if this is a notification task
                if task_name == "app.tasks.notifications.evaluate_notification":
                    beat_entry["kwargs"] = {"event_key": event_key}

                beat_schedule_key = event_key.lower().replace("_", "-")
                beat_schedule[beat_schedule_key] = beat_entry
                active_count += 1

                logger.info(
                    f"✅ REGISTERED - {event_key}",
                    task_type=task_type,
                    task_name=task_name,
                    schedule=schedule_description,
                    beat_key=beat_schedule_key,
                )

        logger.info(
            f"📊 YAML parsing complete",
            total_events=len(cfg.events),
            active_events=active_count,
            inactive_events=inactive_count,
            registered_schedules=len(beat_schedule),
        )

        return beat_schedule

    except Exception as e:
        logger.error(f"Failed to generate beat schedule from YAML: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {}
