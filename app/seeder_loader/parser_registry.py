### app/data_loader/parser_registry.py

"""
Parser Registry - Decorator-based registry for data loader parsers

Parsers register themselves using the @parser decorator and are automatically
discovered at application startup.
"""

from dataclasses import dataclass
from functools import wraps
from typing import Callable, Dict, List, Optional

from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ParserMetadata:
    """Metadata for a registered parser"""

    name: str
    function: Callable
    sheet_names: List[str]
    version: str
    deprecated: bool
    description: Optional[str] = None


# Global registry storage
PARSER_REGISTRY: Dict[str, ParserMetadata] = {}


def parser(
    name: str,
    sheet_names: List[str],
    version: str = "1.0",
    deprecated: bool = False,
    description: Optional[str] = None,
):
    """
    Decorator to register a parser function.

    The parser will be automatically discovered and registered at startup.
    The decorated function receives pre-parsed DataFrames and should NOT handle sheet parsing.

    Args:
        name: Unique name for the parser (e.g., "roles")
        sheet_names: List of sheet names required by this parser (from settings)
        version: Version number (default: "1.0")
        deprecated: Whether this parser is deprecated (default: False)
        description: Optional description

    Example:
        from app.core.data_loader_config import data_loader_settings

        @parser(
            name="roles",
            sheet_names=[data_loader_settings.parser_roles_sheet],
            version="1.0",
            description="Process roles from Excel"
        )
        def process_roles(db, roles_df):
            # Function just processes the DataFrame, no sheet parsing logic
            pass
    """

    def decorator(func: Callable) -> Callable:
        if name in PARSER_REGISTRY:
            logger.warning(f"Parser '{name}' is already registered. Overwriting.")

        PARSER_REGISTRY[name] = ParserMetadata(
            name=name,
            function=func,
            sheet_names=sheet_names,
            version=version,
            deprecated=deprecated,
            description=description,
        )

        logger.debug(
            f"Registered parser: '{name}' (version={version}, deprecated={deprecated}, sheets={sheet_names})"
        )

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator


def get_parser(name: str) -> Optional[ParserMetadata]:
    """Get parser metadata by name."""
    return PARSER_REGISTRY.get(name)


def list_parsers(include_deprecated: bool = False) -> List[ParserMetadata]:
    """List all registered parsers."""
    if include_deprecated:
        return list(PARSER_REGISTRY.values())
    return [p for p in PARSER_REGISTRY.values() if not p.deprecated]


def get_parser_names(include_deprecated: bool = False) -> List[str]:
    """Get list of all parser names."""
    parsers = list_parsers(include_deprecated=include_deprecated)
    return [p.name for p in parsers]


def import_parsers():
    """
    Import all parser modules to trigger registration.
    Called at application startup.
    """
    logger.info("🔍 Discovering and registering parsers...")

    # Import BPM parsers
    try:
        from app.seeder.bpm import (
            parse_case_first_step_config,
            parse_case_status,
            parse_case_step,
            parse_case_step_config,
            parse_case_step_config_paths,
            parse_case_types,
            parse_roles,
            parse_users_and_roles,
        )
    except ImportError as e:
        logger.warning(f"Failed to import BPM parsers: {e}")

    # Import BAT parsers
    try:
        from app.seeder.bat import (
            parse_address,
            parse_bank_accounts,
            parse_corporation,
            parse_dealers,
            parse_drivers,
            parse_entity,
            parse_individuals,
            parse_lease_driver,
            parse_leases,
            parse_medallions,
            parse_mo_lease,
            parse_vehicle_entity,
            parse_vehicle_expenses,
            parse_vehicle_hackups,
            parse_vehicle_inspections,
            parse_vehicle_registration,
            parse_vehicles,
        )
    except ImportError as e:
        logger.warning(f"Failed to import BAT parsers: {e}")

    logger.info(
        f"✅ Registered {len(PARSER_REGISTRY)} parsers: {list(PARSER_REGISTRY.keys())}"
    )
