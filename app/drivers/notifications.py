from app.notifications.registry import NotificationContext, notification
from app.utils.logger import get_logger

logger = get_logger(__name__)

from datetime import date, timedelta


@notification("DMV_EXPIRY_PRIOR")
def dmv_expiry_prior(ctx: NotificationContext) -> list:
    """
    Collect DMV license expiry notifications for multiple offset days.

    ctx.run_at  -> execution datetime (configured timezone)
    ctx.params  -> YAML kwargs (offsets_days)

    Returns:
        List of NotificationLogDetail objects (to be sent by orchestration layer)
    """
    from app.core.db import SessionLocal
    from app.drivers.notification_services import get_dmv_license_expiry_notifications
    from app.notifications.schemas import NotificationEvent

    offsets = ctx.params.get("offsets_days", [])

    # ctx.run_at is timezone-aware datetime from YAML config
    run_at_tz = ctx.run_at
    today = run_at_tz.date()

    db = SessionLocal()
    try:
        # Collect all notifications for the given offsets
        all_notifications = []
        for offset in offsets:
            target_date = today + timedelta(days=offset)

            logger.info(
                f"Collecting DMV license expiry notifications for {target_date} (today + {offset} days)"
            )

            notifications = get_dmv_license_expiry_notifications(
                db,
                target_date,
                run_at_tz=run_at_tz,
                event=NotificationEvent.DMV_EXPIRY_PRIOR,
            )

            logger.info(
                f"Found {len(notifications)} valid drivers with DMV license expiring on {target_date}"
            )
            all_notifications.extend(notifications)

        return all_notifications

    finally:
        db.close()


@notification("TLC_EXPIRY_PRIOR")
def tlc_expiry_prior(ctx: NotificationContext) -> list:
    """
    Collect TLC license expiry notifications for multiple offset days.

    ctx.run_at  -> execution datetime (configured timezone)
    ctx.params  -> YAML kwargs (offsets_days)

    Returns:
        List of NotificationLogDetail objects (to be sent by orchestration layer)
    """
    from app.core.db import SessionLocal
    from app.drivers.notification_services import get_tlc_license_expiry_notifications
    from app.notifications.schemas import NotificationEvent

    offsets = ctx.params.get("offsets_days", [])

    # ctx.run_at is timezone-aware datetime from YAML config
    run_at_tz = ctx.run_at
    today = run_at_tz.date()

    db = SessionLocal()
    try:
        # Collect all notifications for the given offsets
        all_notifications = []
        for offset in offsets:
            target_date = today + timedelta(days=offset)

            logger.info(
                f"Collecting TLC license expiry notifications for {target_date} (today + {offset} days)"
            )

            notifications = get_tlc_license_expiry_notifications(
                db,
                target_date,
                run_at_tz=run_at_tz,
                event=NotificationEvent.TLC_EXPIRY_PRIOR,
            )

            logger.info(
                f"Found {len(notifications)} valid drivers with TLC license expiring on {target_date}"
            )
            all_notifications.extend(notifications)

        return all_notifications

    finally:
        db.close()


@notification("TLC_EXPIRY_DATE")
def tlc_expiry_date(ctx: NotificationContext) -> list:
    """
    Collect TLC license notifications for licenses expiring today.

    ctx.run_at  -> execution datetime (configured timezone)
    ctx.params  -> YAML kwargs (offsets_days should be [0])

    Returns:
        List of NotificationLogDetail objects (to be sent by orchestration layer)
    """
    from app.core.db import SessionLocal
    from app.drivers.notification_services import get_tlc_license_expiry_notifications
    from app.notifications.schemas import NotificationEvent

    offsets = ctx.params.get("offsets_days", [0])

    # ctx.run_at is timezone-aware datetime from YAML config
    run_at_tz = ctx.run_at
    today = run_at_tz.date()

    db = SessionLocal()
    try:
        # Collect all notifications for the given offsets
        all_notifications = []
        for offset in offsets:
            target_date = today + timedelta(days=offset)

            logger.info(
                f"Collecting TLC license expiry notifications for {target_date} (today + {offset} days)"
            )

            notifications = get_tlc_license_expiry_notifications(
                db,
                target_date,
                run_at_tz=run_at_tz,
                event=NotificationEvent.TLC_EXPIRY_DATE,
            )

            logger.info(
                f"Found {len(notifications)} valid drivers with TLC license expiring on {target_date}"
            )
            all_notifications.extend(notifications)

        return all_notifications

    finally:
        db.close()


@notification("TLC_EXPIRY_POST")
def tlc_expiry_post(ctx: NotificationContext) -> list:
    """
    Collect TLC license notifications for licenses that have already expired.

    ctx.run_at  -> execution datetime (configured timezone)
    ctx.params  -> YAML kwargs (offsets_days with negative values, e.g., [-1, -3, -7, -10])

    Returns:
        List of NotificationLogDetail objects (to be sent by orchestration layer)
    """
    from app.core.db import SessionLocal
    from app.drivers.notification_services import get_tlc_license_expiry_notifications
    from app.notifications.schemas import NotificationEvent

    offsets = ctx.params.get("offsets_days", [-1, -7, -10])

    # ctx.run_at is timezone-aware datetime from YAML config
    run_at_tz = ctx.run_at
    today = run_at_tz.date()

    db = SessionLocal()
    try:
        # Collect all notifications for the given offsets
        all_notifications = []
        for offset in offsets:
            target_date = today + timedelta(days=offset)

            logger.info(
                f"Collecting TLC license expiry notifications for {target_date} (today + {offset} days)"
            )

            notifications = get_tlc_license_expiry_notifications(
                db,
                target_date,
                run_at_tz=run_at_tz,
                event=NotificationEvent.TLC_EXPIRY_POST,
            )

            logger.info(
                f"Found {len(notifications)} valid drivers with TLC license expiring on {target_date}"
            )
            all_notifications.extend(notifications)

        return all_notifications

    finally:
        db.close()


@notification("DMV_EXPIRY_DATE")
def dmv_expiry_date(ctx: NotificationContext) -> list:
    """
    Collect DMV license notifications for licenses expiring today.

    ctx.run_at  -> execution datetime (configured timezone)
    ctx.params  -> YAML kwargs (offsets_days should be [0])

    Returns:
        List of NotificationLogDetail objects (to be sent by orchestration layer)
    """
    from app.core.db import SessionLocal
    from app.drivers.notification_services import get_dmv_license_expiry_notifications
    from app.notifications.schemas import NotificationEvent

    offsets = ctx.params.get("offsets_days", [0])

    # ctx.run_at is timezone-aware datetime from YAML config
    run_at_tz = ctx.run_at
    today = run_at_tz.date()

    db = SessionLocal()
    try:
        # Collect all notifications for the given offsets
        all_notifications = []
        for offset in offsets:
            target_date = today + timedelta(days=offset)

            logger.info(
                f"Collecting DMV license expiry notifications for {target_date} (today + {offset} days)"
            )

            notifications = get_dmv_license_expiry_notifications(
                db,
                target_date,
                run_at_tz=run_at_tz,
                event=NotificationEvent.DMV_EXPIRY_DATE,
            )

            logger.info(
                f"Found {len(notifications)} valid drivers with DMV license expiring on {target_date}"
            )
            all_notifications.extend(notifications)

        return all_notifications

    finally:
        db.close()


@notification("DMV_EXPIRY_POST")
def dmv_expiry_post(ctx: NotificationContext) -> list:
    """
    Collect DMV license notifications for licenses that have already expired.

    ctx.run_at  -> execution datetime (configured timezone)
    ctx.params  -> YAML kwargs (offsets_days with negative values, e.g., [-1, -3, -7, -14])

    Returns:
        List of NotificationLogDetail objects (to be sent by orchestration layer)
    """
    from app.core.db import SessionLocal
    from app.drivers.notification_services import get_dmv_license_expiry_notifications
    from app.notifications.schemas import NotificationEvent

    offsets = ctx.params.get("offsets_days", [-1, -7, -14])

    # ctx.run_at is timezone-aware datetime from YAML config
    run_at_tz = ctx.run_at
    today = run_at_tz.date()

    db = SessionLocal()
    try:
        # Collect all notifications for the given offsets
        all_notifications = []
        for offset in offsets:
            target_date = today + timedelta(days=offset)

            logger.info(
                f"Collecting DMV license expiry notifications for {target_date} (today + {offset} days)"
            )

            notifications = get_dmv_license_expiry_notifications(
                db,
                target_date,
                run_at_tz=run_at_tz,
                event=NotificationEvent.DMV_EXPIRY_POST,
            )

            logger.info(
                f"Found {len(notifications)} valid drivers with DMV license expiring on {target_date}"
            )
            all_notifications.extend(notifications)

        return all_notifications

    finally:
        db.close()
