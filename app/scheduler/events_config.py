from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass(frozen=True)
class EventsConfig:
    timezone: str
    events: Dict[str, Dict[str, Any]]


def load_events_yaml(events_yaml: str | bytes | Path) -> EventsConfig:
    """
    Load events configuration from YAML content.

    Args:
        events_yaml: Can be a Path object, string content, or bytes content
    """
    if isinstance(events_yaml, Path):
        raw = yaml.safe_load(events_yaml.read_text()) or {}
    elif isinstance(events_yaml, bytes):
        raw = yaml.safe_load(events_yaml.decode('utf-8')) or {}
    else:  # str
        raw = yaml.safe_load(events_yaml) or {}

    timezone = raw.get("timezone") or "UTC"
    events = raw.get("events") or {}

    if not isinstance(events, dict):
        raise ValueError("'events' must be a mapping")

    # Normalize event blocks
    normalized: Dict[str, Dict[str, Any]] = {}
    for event_key, event in events.items():
        if not isinstance(event, dict):
            raise ValueError(f"Event '{event_key}' must be a mapping")
        normalized[event_key] = event

    return EventsConfig(timezone=timezone, events=normalized)
