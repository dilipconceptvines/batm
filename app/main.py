# app/main.py

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import app.audit_trail.models

# Import all models to ensure SQLAlchemy class registry is populated
import app.bpm.models
import app.curb.models
import app.deposits.models
import app.driver_payments.models
import app.drivers.models
import app.dtr.models
import app.entities.models
import app.esign.models
import app.ezpass.models
import app.exports.models  # Import export models
import app.interim_payments.models
import app.leases.models
import app.ledger.models
import app.loans.models
import app.medallions.models
import app.misc_expenses.models
import app.notes.models
import app.pvb.models
import app.repairs.models
import app.reports.models
import app.tlc.models
import app.uploads.models
import app.users.models
import app.vehicles.models
from app.audit_trail.router import router as audit_trail_routes
from app.bpm.router import router as bpm_routes
from app.bpm.step_info import STEP_REGISTRY, import_bpm_flows
from app.core.config import settings
from app.curb.router import router as curb_routes
from app.current_balances.router import router as current_balances_routes
from app.dashboard.router import router as dashboard_routes
from app.deposits.router import router as deposits_routes
from app.driver_payments.router import router as driver_payments_routes
from app.drivers.router import router as driver_routes
from app.dtr.router import router as dtr_routes
from app.entities.router import router as entity_routes
from app.esign.router import router as esign_routes
from app.ezpass.router import router as ezpass_routes
from app.exports.router import router as exports_routes  # Import exports router
from app.interim_payments.router import router as interim_payment_routes
from app.leases.router import router as lease_routes

# --- Payment Engine Routes ---
from app.ledger.router import router as ledger_routes
from app.loans.router import router as loans_routes
from app.medallions.router import router as medallion_routes
from app.misc_expenses.router import router as misc_expense_routes
from app.pvb.router import router as pvb_routes
from app.repairs.router import router as repairs_routes
from app.reports.router import router as reports_routes
from app.tlc.router import router as tlc_routes
from app.uploads.router import router as upload_routes

# Local application imports - Routes
from app.users.router import router as user_routes
from app.utils.logger import get_logger, setup_app_logging
from app.vehicles.router import router as vehicle_routes
from app.seeder_loader.router import router as data_loader_routes


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for app startup and shutdown tasks
    """
    # Import BPM flows
    import_bpm_flows()

    # Import and register parsers
    from app.seeder_loader.parser_registry import import_parsers
    import_parsers()

    yield


# Create the FastAPI app
bat_app = FastAPI(
    title=f"Big Apple Taxi - {settings.environment}",
    description="Big Apple Taxi API",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Configure logging
if settings.environment.lower() in ["production", "staging"]:
    setup_app_logging(
        bat_app,
        log_level=settings.uvicorn_log_level,
        use_json=True,
        log_file="/var/log/batm_app.log",
        app_name="Big Apple Taxi Management System",
        environment=settings.environment,
    )
else:
    setup_app_logging(
        bat_app,
        log_level=settings.uvicorn_log_level,
        use_json=False,
        log_file="app.log",
        app_name="Big Apple Taxi Management System",
        environment=settings.environment,
    )
logger = get_logger(__name__)

# Add CORS middleware
bat_app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_cors_urls.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
bat_app.include_router(user_routes)
bat_app.include_router(bpm_routes)
bat_app.include_router(dashboard_routes)
bat_app.include_router(entity_routes)
bat_app.include_router(medallion_routes)
bat_app.include_router(vehicle_routes)
bat_app.include_router(driver_routes)
bat_app.include_router(lease_routes)
bat_app.include_router(audit_trail_routes)
bat_app.include_router(upload_routes)
bat_app.include_router(reports_routes)
bat_app.include_router(esign_routes)
# Payment Routes
bat_app.include_router(ledger_routes)
bat_app.include_router(curb_routes)
bat_app.include_router(ezpass_routes)
bat_app.include_router(pvb_routes)
bat_app.include_router(repairs_routes)
bat_app.include_router(loans_routes)
bat_app.include_router(tlc_routes)
bat_app.include_router(interim_payment_routes)
bat_app.include_router(misc_expense_routes)
bat_app.include_router(driver_payments_routes)
bat_app.include_router(dtr_routes)
bat_app.include_router(current_balances_routes)
bat_app.include_router(deposits_routes)
bat_app.include_router(data_loader_routes)
bat_app.include_router(exports_routes)  # Add exports router


# Root API to check if the server is up
@bat_app.get("/", tags=["Base"])
async def health_check():
    """
    Root API to check if the server is up
    """
    logger.info("Calling root API for testing")
    return {"status": "ok"}


@bat_app.get("/version", tags=["Base"])
async def get_version():
    """
    Get application version from .version file
    """
    return {"version": settings.app_version}
