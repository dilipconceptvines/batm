# app/utils/logger.py

import uuid
import sys
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from contextvars import ContextVar

import structlog
from fastapi import Request, FastAPI
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware


# Context variable for request ID tracking
request_id_var: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


class LogConfig:
    """Centralized logging configuration."""

    # Log levels
    LOG_LEVEL = "INFO"

    # Format options
    USE_JSON = False

    # File output
    LOG_FILE: Optional[str] = None

    # Application Information
    APP_NAME = "Big Apple Taxi Management System"
    APP_VERSION = "2.0.0"
    ENVIRONMENT = "development"


def setup_logging(
    log_level: str = "INFO",
    use_json: bool = True,
    log_file: Optional[str] = None,
    app_name: str = "fastapi-app",
    environment: str = "development"
) -> None:
    """
    Configure structlog for the application
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        use_json: Use JSON format (True) or colored console output (False)
        log_file: Optional path to log file
        app_name: Application name for log context
        environment: Environment name (development, staging, production)
    """
    
    # Store configuration
    LogConfig.LOG_LEVEL = log_level
    LogConfig.USE_JSON = use_json
    LogConfig.LOG_FILE = log_file
    LogConfig.APP_NAME = app_name
    LogConfig.ENVIRONMENT = environment
    
    # Clear any existing handlers to avoid conflicts
    logging.root.handlers = []
    
    # Common processors for structlog
    shared_processors: List[Any] = [
        structlog.contextvars.merge_contextvars,
        add_request_id,
        add_app_context,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    # Setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Console renderer based on format preference
    if use_json:
        console_renderer = structlog.processors.JSONRenderer()
    else:
        console_renderer = structlog.dev.ConsoleRenderer(
            colors=False, 
            exception_formatter=structlog.dev.plain_traceback
        )
    
    # Setup console formatter
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processors=shared_processors + [console_renderer],
        foreign_pre_chain=shared_processors,
    )
    console_handler.setFormatter(console_formatter)
    
    # Configure root logger
    logging.root.setLevel(getattr(logging, log_level.upper()))
    logging.root.addHandler(console_handler)
    
    # Setup file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        
        # File always gets JSON format (no colors in files)
        file_formatter = structlog.stdlib.ProcessorFormatter(
            processors=shared_processors + [structlog.processors.JSONRenderer()],
            foreign_pre_chain=shared_processors,
        )
        file_handler.setFormatter(file_formatter)
        logging.root.addHandler(file_handler)
    
    # Configure structlog
    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def setup_file_logging(log_file: str, log_level: str, use_json: bool) -> None:
    """Setup file logging handler."""
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Create Formatter
    if use_json:
        processors = [
            structlog.contextvars.merge_contextvars,
            add_request_id,
            add_app_context,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ]
    else:
        processors = [
            structlog.contextvars.merge_contextvars,
            add_request_id,
            add_app_context,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.dev.ConsoleRenderer(colors=False)
        ]

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(colors=True) if not use_json else structlog.processors.JSONRenderer(),
        foreign_pre_chain=processors,
    )

    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)


def add_request_id(logger: logging.Logger, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Add request ID to log context."""
    request_id = request_id_var.get()
    if request_id:
        event_dict["request_id"] = request_id
    return event_dict

def add_app_context(logger: logging.Logger, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Add application context to logs."""
    event_dict["app"] = LogConfig.APP_NAME
    event_dict["environment"] = LogConfig.ENVIRONMENT
    return event_dict


def get_logger(name: Optional[str] = None) -> structlog.BoundLogger:
    """
    Get a configured logger instance.

    Args:
        name (Optional[str]): Name of the logger.

    Returns:
        structlog.BoundLogger: Configured logger instance.
    """
    return structlog.get_logger(name)


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for request/response logging and request ID injection
    """
    
    async def dispatch(self, request: Request, call_next):
        # Generate or extract request ID
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        request_id_var.set(request_id)
        
        # Get logger
        logger = get_logger("api.access")
        
        # Process request
        start_time = datetime.now(timezone.utc)
        response = None
        
        try:
            # Log request start
            logger.debug(
                "request_started",
                method=request.method,
                path=request.url.path,
                client_host=request.client.host if request.client else None,
                headers=dict(request.headers) if LogConfig.LOG_LEVEL == "DEBUG" else None
            )

            # Call the actual endpoint
            response = await call_next(request)

            # Calculate duration
            duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

            # Log successful response
            logger.debug(
                "request_completed",
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                duration_ms=round(duration_ms, 2)
            )
            
            # Add request ID to response headers
            response.headers["X-Request-ID"] = request_id
            
            return response
            
        except Exception as e:
            # Calculate duration
            duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            
            # Log error
            logger.error(
                "request_failed",
                method=request.method,
                path=request.url.path,
                duration_ms=round(duration_ms, 2),
                error=str(e),
                exc_info=True
            )
            
            # Return error response
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal server error",
                    "request_id": request_id
                },
                headers={"X-Request-ID": request_id}
            )
        finally:
            # Clear request ID from context
            request_id_var.set(None)


def setup_app_logging(
    app: FastAPI,
    log_level: str = "INFO",
    use_json: bool = True,
    log_file: Optional[str] = None,
    app_name: str = "Big Apple Taxi Management System",
    environment: str = "development",
) -> None:
    """
    Setup logging for a FastAPI application.

    Args:
        app (FastAPI): FastAPI application instance.
        log_level (str): Logging level.
        use_json (bool): Whether to use JSON formatting.
        log_file (Optional[str]): File path for logging output.
        app_name (str): Name of the application.
        environment (str): Application environment (e.g., development, production).
    """

    # Use app title if no name provided
    if not app_name:
        app_name = app.title or "Big Apple Taxi Management System"

    # Setup logging
    setup_logging(
        log_level=log_level,
        use_json=use_json,
        log_file=log_file,
        app_name=app_name,
        environment=environment,
    )

    # Add logging middleware
    app.add_middleware(LoggingMiddleware)