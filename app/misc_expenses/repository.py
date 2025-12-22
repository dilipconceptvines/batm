### app/misc_expenses/repository.py

from datetime import date, datetime
from typing import List, Optional, Tuple
from decimal import Decimal

from sqlalchemy import func, update, or_
from sqlalchemy.orm import Session, joinedload

from app.drivers.models import Driver
from app.interim_payments.models import InterimPayment
from app.leases.models import Lease
from app.medallions.models import Medallion
from app.misc_expenses.models import MiscellaneousExpense, MiscellaneousExpenseStatus
from app.uploads.models import Document
from app.vehicles.models import Vehicle, VehicleRegistration
from app.utils.logger import get_logger

logger = get_logger(__name__)


class MiscellaneousExpenseRepository:
    """
    Data Access Layer for Miscellaneous Expenses.
    Handles all database interactions for the MiscellaneousExpense model.
    """

    def __init__(self, db: Session):
        self.db = db

    def create_expense(self, expense: MiscellaneousExpense) -> MiscellaneousExpense:
        """Adds a new MiscellaneousExpense record to the session."""
        self.db.add(expense)
        self.db.flush()
        self.db.refresh(expense)
        logger.info("Created new MiscellaneousExpense", expense_id=expense.expense_id)
        return expense

    def get_expense_by_id(self, expense_pk_id: int) -> Optional[MiscellaneousExpense]:
        """Fetches a single miscellaneous expense by its primary key."""
        return self.db.query(MiscellaneousExpense).filter(MiscellaneousExpense.id == expense_pk_id).first()

    def get_expense_by_expense_id(self, expense_id: str) -> Optional[MiscellaneousExpense]:
        """Fetches a single miscellaneous expense by the system-generated Expense ID."""
        return self.db.query(MiscellaneousExpense).filter(MiscellaneousExpense.expense_id == expense_id).first()
    
    def get_last_expense_id_for_year(self, year: int) -> Optional[str]:
        """Finds the last used expense_id for a given year to determine the next sequence number."""
        prefix = f"MISC-{year}-"
        return (
            self.db.query(MiscellaneousExpense.expense_id)
            .filter(MiscellaneousExpense.expense_id.like(f"{prefix}%"))
            .order_by(MiscellaneousExpense.expense_id.desc())
            .first()
        )

    def list_expenses(
        self,
        page: int,
        per_page: int,
        sort_by: str,
        sort_order: str,
        expense_id: Optional[str] = None,
        reference_no: Optional[str] = None,
        category: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        from_amount: Optional[Decimal] = None,
        to_amount: Optional[Decimal] = None,
        driver_name: Optional[str] = None,
        lease_id: Optional[str] = None,
        vin: Optional[str] = None,
        plate_no: Optional[str] = None,
        medallion_no: Optional[str] = None,
        vehicle: Optional[str] = None,
    ) -> Tuple[List[MiscellaneousExpense], int]:
        """
        Retrieves a paginated, sorted, and filtered list of Miscellaneous Expenses.
        
        Supports:
        - Comma-separated filters for: expense_id, lease_id, vin, plate_no, medallion_no, vehicle
        - Date range filtering with from_date and to_date
        - Amount range filtering with from_amount and to_amount
        - Text search for driver_name, reference_no, category
        """
        query = (
            self.db.query(MiscellaneousExpense)
            .options(
                joinedload(MiscellaneousExpense.driver),
                joinedload(MiscellaneousExpense.lease),
                joinedload(MiscellaneousExpense.vehicle).joinedload(Vehicle.registrations),
                joinedload(MiscellaneousExpense.medallion),
            )
            .join(Driver, MiscellaneousExpense.driver_id == Driver.id)
            .join(Lease, MiscellaneousExpense.lease_id == Lease.id)
            .join(Vehicle, MiscellaneousExpense.vehicle_id == Vehicle.id)
            .outerjoin(Medallion, MiscellaneousExpense.medallion_id == Medallion.id)
        )

        # Apply filters with comma-separated support
        if expense_id:
            ids = [id.strip() for id in expense_id.split(',') if id.strip()]
            if ids:
                query = query.filter(or_(*[MiscellaneousExpense.expense_id.ilike(f"%{id}%") for id in ids]))

        if reference_no:
            query = query.filter(MiscellaneousExpense.reference_number.ilike(f"%{reference_no}%"))

        if category:
            query = query.filter(MiscellaneousExpense.category.ilike(f"%{category}%"))

        # Date range filters
        if from_date:
            from_datetime = datetime.combine(from_date, datetime.min.time())
            query = query.filter(MiscellaneousExpense.expense_date >= from_datetime)

        if to_date:
            to_datetime = datetime.combine(to_date, datetime.max.time())
            query = query.filter(MiscellaneousExpense.expense_date <= to_datetime)

        # Amount range filters
        if from_amount is not None:
            query = query.filter(MiscellaneousExpense.amount >= from_amount)

        if to_amount is not None:
            query = query.filter(MiscellaneousExpense.amount <= to_amount)

        if driver_name:
            query = query.filter(Driver.full_name.ilike(f"%{driver_name}%"))

        if lease_id:
            lease_ids = [id.strip() for id in lease_id.split(',') if id.strip()]
            if lease_ids:
                query = query.filter(or_(*[Lease.lease_id.ilike(f"%{id}%") for id in lease_ids]))

        if vin:
            vins = [v.strip() for v in vin.split(',') if v.strip()]
            if vins:
                query = query.filter(or_(*[Vehicle.vin.ilike(f"%{v}%") for v in vins]))

        if plate_no:
            plates = [p.strip() for p in plate_no.split(',') if p.strip()]
            if plates:
                # Join with VehicleRegistration if not already joined
                query = query.join(
                    VehicleRegistration,
                    Vehicle.id == VehicleRegistration.vehicle_id,
                    isouter=True
                ).filter(or_(*[VehicleRegistration.plate_number.ilike(f"%{p}%") for p in plates]))

        if medallion_no:
            medallion_nos = [m.strip() for m in medallion_no.split(',') if m.strip()]
            if medallion_nos:
                query = query.filter(or_(*[Medallion.medallion_number.ilike(f"%{m}%") for m in medallion_nos]))

        if vehicle:
            # Filter by vehicle make, model, or year (comma-separated)
            vehicle_terms = [v.strip() for v in vehicle.split(',') if v.strip()]
            if vehicle_terms:
                vehicle_filters = []
                for term in vehicle_terms:
                    vehicle_filters.extend([
                        Vehicle.make.ilike(f"%{term}%"),
                        Vehicle.model.ilike(f"%{term}%"),
                        Vehicle.year.ilike(f"%{term}%"),
                    ])
                query = query.filter(or_(*vehicle_filters))

        total_items = query.with_entities(func.count(MiscellaneousExpense.id)).scalar()

        # Apply sorting
        sort_column_map = {
            "expense_id": MiscellaneousExpense.expense_id,
            "reference_no": MiscellaneousExpense.reference_number,
            "category": MiscellaneousExpense.category,
            "date": MiscellaneousExpense.expense_date,
            "amount": MiscellaneousExpense.amount,
            "driver": Driver.full_name,
            "lease_id": Lease.lease_id,
            "vin_no": Vehicle.vin,
            "vehicle": Vehicle.make,  # Default vehicle sorting by make
            "plate_no": VehicleRegistration.plate_number,
            "medallion_no": Medallion.medallion_number,
        }
        
        sort_column = sort_column_map.get(sort_by, MiscellaneousExpense.expense_date)
        if sort_order.lower() == "desc":
            query = query.order_by(sort_column.desc())
        else:
            query = query.order_by(sort_column.asc())

        query = query.offset((page - 1) * per_page).limit(per_page)

        expenses = query.all()
        
        # Fetch documents for each expense
        for expense in expenses:
            # Get documents associated with this miscellaneous expense
            documents = (
                self.db.query(Document)
                .filter(
                    Document.object_type == "miscellaneous_expense",
                    Document.object_lookup_id == str(expense.id)
                )
                .order_by(Document.created_on.desc())
                .all()
            )
            
            # Convert documents to dictionaries with presigned URLs
            expense_documents = []
            for doc in documents:
                expense_documents.append({
                    "document_id": doc.id,
                    "document_name": doc.document_name,
                    "document_type": doc.document_type,
                    "document_format": doc.document_format,
                    "document_date": doc.document_date.strftime("%Y-%m-%d") if doc.document_date else None,
                    "document_size": doc.document_actual_size,
                    "document_note": doc.document_note,
                    "presigned_url": doc.presigned_url,
                    "object_type": doc.object_type,
                    "created_on": doc.created_on.strftime("%Y-%m-%d %H:%M:%S") if doc.created_on else None,
                })
            
            # Attach documents to the expense object as a custom attribute
            expense._documents = expense_documents

        return expenses, total_items
    
    def get_distinct_categories(self) -> List[str]:
        """
        Retrieves all distinct expense categories from the database.
        Returns a sorted list of unique category names.
        """
        categories = (
            self.db.query(MiscellaneousExpense.category)
            .distinct()
            .filter(MiscellaneousExpense.category.isnot(None))
            .order_by(MiscellaneousExpense.category)
            .all()
        )
        return [cat[0] for cat in categories if cat[0]]