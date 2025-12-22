### app/misc_expenses/router.py

import math
from datetime import date
from io import BytesIO
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.bpm.services import bpm_service
from app.core.db import get_db
from app.core.dependencies import get_db_with_current_user
from app.misc_expenses.exceptions import MiscellaneousExpenseError
from app.misc_expenses.schemas import (
    MiscellaneousExpenseResponse,
    PaginatedMiscellaneousExpenseResponse,
)
from app.misc_expenses.services import MiscellaneousExpenseService
from app.users.models import User
from app.users.utils import get_current_user
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/payments/miscellaneous-expenses", tags=["Miscellaneous Expenses"])

# Dependency to inject the MiscellaneousExpenseService
def get_misc_expense_service(db: Session = Depends(get_db)) -> MiscellaneousExpenseService:
    """Provides an instance of MiscellaneousExpenseService with the current DB session."""
    return MiscellaneousExpenseService(db)


@router.post("/create-case", summary="Create a New Miscellaneous Expense Case", status_code=status.HTTP_201_CREATED)
def create_misc_expense_case(
    db: Session = Depends(get_db_with_current_user),
    current_user: User = Depends(get_current_user),
):
    """
    Initiates a new BPM workflow for creating a Miscellaneous Expense.
    """
    try:
        new_case = bpm_service.create_case(db, prefix="MISCEXP", user=current_user)
        return {
            "message": "New Miscellaneous Expense case started successfully.",
            "case_no": new_case.case_no,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Error creating miscellaneous expense case: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Could not start a new miscellaneous expense case.") from e


@router.get("", response_model=PaginatedMiscellaneousExpenseResponse, summary="List Miscellaneous Expenses")
def list_misc_expenses(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    sort_by: Optional[str] = Query("date"),
    sort_order: str = Query("desc", enum=["asc", "desc"]),
    # Filters - Text fields (comma-separated support)
    expense_id: Optional[str] = Query(None, description="Comma-separated expense IDs"),
    reference_no: Optional[str] = Query(None, description="Reference number filter"),
    category: Optional[str] = Query(None, description="Category filter"),
    driver_name: Optional[str] = Query(None, description="Driver name filter"),
    lease_id: Optional[str] = Query(None, description="Comma-separated lease IDs"),
    vin_no: Optional[str] = Query(None, description="Comma-separated VIN numbers"),
    plate_no: Optional[str] = Query(None, description="Comma-separated plate numbers"),
    medallion_no: Optional[str] = Query(None, description="Comma-separated medallion numbers"),
    vehicle: Optional[str] = Query(None, description="Comma-separated vehicle make/model/year"),
    # Date range filters
    from_date: Optional[date] = Query(None, description="Start date for expense date range"),
    to_date: Optional[date] = Query(None, description="End date for expense date range"),
    # Amount range filters
    from_amount: Optional[Decimal] = Query(None, ge=0, description="Minimum amount"),
    to_amount: Optional[Decimal] = Query(None, ge=0, description="Maximum amount"),
    expense_service: MiscellaneousExpenseService = Depends(get_misc_expense_service),
    current_user: User = Depends(get_current_user),
):
    """
    Retrieves a paginated and filterable list of all miscellaneous expenses.
    
    Supports:
    - Comma-separated filters for: expense_id, lease_id, vin_no, plate_no, medallion_no, vehicle
    - Date range filtering with from_date and to_date
    - Amount range filtering with from_amount and to_amount
    - Text search for driver_name, reference_no, category
    - Sorting by any column with asc/desc order
    
    Returns:
    - Paginated list of expenses
    - Available categories for filter dropdown population
    """
    try:
        expenses, total_items = expense_service.repo.list_expenses(
            page=page,
            per_page=per_page,
            sort_by=sort_by,
            sort_order=sort_order,
            expense_id=expense_id,
            reference_no=reference_no,
            category=category,
            from_date=from_date,
            to_date=to_date,
            from_amount=from_amount,
            to_amount=to_amount,
            driver_name=driver_name,
            lease_id=lease_id,
            vin=vin_no,
            plate_no=plate_no,
            medallion_no=medallion_no,
            vehicle=vehicle,
        )

        response_items = [
            MiscellaneousExpenseResponse(
                expense_id=exp.expense_id,
                reference_number=exp.reference_number,
                category=exp.category,
                expense_date=exp.expense_date,
                amount=exp.amount,
                notes=exp.notes,
                driver_name=exp.driver.full_name if exp.driver else "N/A",
                lease_id=exp.lease.lease_id if exp.lease else "N/A",
                vin_no=exp.vehicle.vin if exp.vehicle else "N/A",
                vehicle_name=f"{exp.vehicle.year} {exp.vehicle.make} {exp.vehicle.model}" if exp.vehicle else "N/A",
                plate_no=exp.vehicle.registrations[0].plate_number if exp.vehicle and exp.vehicle.registrations else "N/A",
                medallion_no=exp.medallion.medallion_number if exp.medallion else "N/A",
                documents=getattr(exp, '_documents', []),
            ) for exp in expenses
        ]
        
        total_pages = math.ceil(total_items / per_page) if per_page > 0 else 0

        # Get available categories for filter dropdown
        available_categories = expense_service.repo.get_distinct_categories()

        return PaginatedMiscellaneousExpenseResponse(
            items=response_items,
            total_items=total_items,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
            available_categories=available_categories,
        )
    except Exception as e:
        logger.error("Error fetching miscellaneous expenses: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while fetching miscellaneous expenses.") from e


@router.get("/export", summary="Export Miscellaneous Expenses Data")
def export_misc_expenses(
    export_format: str = Query("excel", enum=["excel", "pdf"], alias="format"),
    # Pass through all filters from the list endpoint
    sort_by: Optional[str] = Query("date"),
    sort_order: str = Query("desc"),
    # Text filters (comma-separated support)
    expense_id: Optional[str] = Query(None, description="Comma-separated expense IDs"),
    reference_no: Optional[str] = Query(None, description="Reference number filter"),
    category: Optional[str] = Query(None, description="Category filter"),
    driver_name: Optional[str] = Query(None, description="Driver name filter"),
    lease_id: Optional[str] = Query(None, description="Comma-separated lease IDs"),
    vin_no: Optional[str] = Query(None, description="Comma-separated VIN numbers"),
    plate_no: Optional[str] = Query(None, description="Comma-separated plate numbers"),
    medallion_no: Optional[str] = Query(None, description="Comma-separated medallion numbers"),
    vehicle: Optional[str] = Query(None, description="Comma-separated vehicle make/model/year"),
    # Date range filters
    from_date: Optional[date] = Query(None, description="Start date for expense date range"),
    to_date: Optional[date] = Query(None, description="End date for expense date range"),
    # Amount range filters
    from_amount: Optional[Decimal] = Query(None, ge=0, description="Minimum amount"),
    to_amount: Optional[Decimal] = Query(None, ge=0, description="Maximum amount"),
    expense_service: MiscellaneousExpenseService = Depends(get_misc_expense_service),
    current_user: User = Depends(get_current_user),
):
    """
    Exports filtered miscellaneous expense data to the specified format (Excel or PDF).
    Applies the same filters as the list endpoint.
    """
    try:
        # Fetch up to 100,000 records for export
        expenses, _ = expense_service.repo.list_expenses(
            page=1,
            per_page=100000,
            sort_by=sort_by,
            sort_order=sort_order,
            expense_id=expense_id,
            reference_no=reference_no,
            category=category,
            from_date=from_date,
            to_date=to_date,
            from_amount=from_amount,
            to_amount=to_amount,
            driver_name=driver_name,
            lease_id=lease_id,
            vin=vin_no,
            plate_no=plate_no,
            medallion_no=medallion_no,
            vehicle=vehicle,
        )

        if not expenses:
            raise HTTPException(status_code=404, detail="No data available for export with the given filters.")

        # Prepare export data
        export_data = []
        for exp in expenses:
            export_data.append({
                "Expense ID": exp.expense_id,
                "Reference No": exp.reference_number or "",
                "Category": exp.category,
                "Date": exp.expense_date.strftime("%Y-%m-%d"),
                "Amount": float(exp.amount),
                "Notes": exp.notes or "",
                "Driver": exp.driver.full_name if exp.driver else "N/A",
                "Lease ID": exp.lease.lease_id if exp.lease else "N/A",
                "VIN No": exp.vehicle.vin if exp.vehicle else "N/A",
                "Vehicle": f"{exp.vehicle.year} {exp.vehicle.make} {exp.vehicle.model}" if exp.vehicle else "N/A",
                "Plate No": exp.vehicle.registrations[0].plate_number if exp.vehicle and exp.vehicle.registrations else "N/A",
                "Medallion No": exp.medallion.medallion_number if exp.medallion else "N/A",
            })
        
        filename = f"miscellaneous_expenses_{date.today()}.{'xlsx' if export_format == 'excel' else 'pdf'}"
        
        # Use ExporterFactory to get the appropriate exporter
        exporter = ExporterFactory.get_exporter(export_format, export_data)
        file_content = exporter.export()
        
        # Set media type based on export format
        media_types = {
            "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pdf": "application/pdf",
        }
        media_type = media_types.get(export_format, "application/octet-stream")
        
        headers = {"Content-Disposition": f"attachment; filename={filename}"}
        return StreamingResponse(file_content, media_type=media_type, headers=headers)

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error exporting miscellaneous expense data: %s", e, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred during the export process."
        ) from e