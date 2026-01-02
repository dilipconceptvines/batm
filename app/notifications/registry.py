from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict

from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class NotificationContext:
    event_key: str
    run_at: datetime  # execution time (timezone-aware)
    params: Dict[str, Any]  # kwargs from YAML


Handler = Callable[[NotificationContext], None]

_NOTIFICATION_HANDLERS: Dict[str, Handler] = {}


def notification(event_key: str) -> Callable[[Handler], Handler]:
    """
    Usage:

      @notification("DMV_EXPIRY_PRIOR")
      def handle_dmv_prior(ctx: NotificationContext):
          ...
    """

    def decorator(fn: Handler) -> Handler:
        if event_key in _NOTIFICATION_HANDLERS:
            raise ValueError(
                f"Duplicate notification handler for event_key={event_key}"
            )
        _NOTIFICATION_HANDLERS[event_key] = fn
        return fn

    return decorator


def get_notification_handler(event_key: str) -> Handler:
    try:
        return _NOTIFICATION_HANDLERS[event_key]
    except KeyError as e:
        raise ValueError(
            f"No notification handler defined for event_key={event_key}"
        ) from e


def import_notification_jobs():
    """
    Auto-discover and import all notifications.py files in app subdirectories.
    This ensures all @notification decorators are executed and handlers are registered.
    """
    app_dir = os.path.dirname(os.path.dirname(__file__))

    logger.info("Starting notification handler discovery", app_dir=app_dir)

    # Look for notifications.py in all direct subdirectories of app/
    for item in os.listdir(app_dir):
        item_path = os.path.join(app_dir, item)

        # Skip non-directories and special directories
        if not os.path.isdir(item_path) or item.startswith("__") or item.startswith("."):
            continue

        # Check for notifications.py in this subdirectory
        notifications_file = os.path.join(item_path, "notifications.py")
        if os.path.isfile(notifications_file):
            module_name = f"app.{item}.notifications"
            logger.info("Importing notification module", module=item, full_path=module_name)
            try:
                importlib.import_module(module_name)
            except Exception as e:
                logger.error(
                    "Failed to import notification module",
                    module=module_name,
                    error=str(e)
                )

    logger.info(
        "Notification handler discovery complete",
        total_handlers=len(_NOTIFICATION_HANDLERS),
        registered_handlers=list(_NOTIFICATION_HANDLERS.keys())
    )
