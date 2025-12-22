### app/loans/repository.py

from datetime import date
from typing import List, Optional, Tuple
from decimal import Decimal

from sqlalchemy import func, update
from sqlalchemy.orm import Session, joinedload

from app.drivers.models import Driver , TLCLicense
from app.leases.models import Lease
from app.medallions.models import Medallion
from app.loans.models import (
    DriverLoan,
    LoanInstallment,
    LoanInstallmentStatus,
    LoanStatus,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


class LoanRepository:
    """
    Data Access Layer for Driver Loans.
    Handles all database interactions for DriverLoan and LoanInstallment models.
    """

    def __init__(self, db: Session):
        self.db = db

    def create_loan(self, loan: DriverLoan) -> DriverLoan:
        """Adds a new DriverLoan record to the session."""
        self.db.add(loan)
        self.db.flush()
        self.db.refresh(loan)
        logger.info("Created new DriverLoan", loan_id=loan.loan_id)
        return loan

    def get_loan_by_id(self, loan_pk_id: int) -> Optional[DriverLoan]:
        """Fetches a single driver loan by its primary key."""
        return self.db.query(DriverLoan).filter(DriverLoan.id == loan_pk_id).first()

    def get_loan_by_loan_id(self, loan_id: str) -> Optional[DriverLoan]:
        """Fetches a single driver loan by the system-generated Loan ID."""
        return self.db.query(DriverLoan).filter(DriverLoan.loan_id == loan_id).first()

    def get_last_loan_id_for_year(self, year: int) -> Optional[str]:
        """Finds the last used loan_id for a given year to determine the next sequence number."""
        prefix = f"DLN-{year}-"
        return (
            self.db.query(DriverLoan.loan_id)
            .filter(DriverLoan.loan_id.like(f"{prefix}%"))
            .order_by(DriverLoan.loan_id.desc())
            .first()
        )

    def bulk_insert_installments(self, installments: List[LoanInstallment]):
        """Performs a bulk insert of new LoanInstallment records."""
        if installments:
            self.db.add_all(installments)

    def update_loan(self, loan_id: int, updates: dict):
        """Updates specific fields of a single loan record."""
        stmt = (
            update(DriverLoan)
            .where(DriverLoan.id == loan_id)
            .values(**updates)
        )
        self.db.execute(stmt)

    def get_due_installments_to_post(self, post_date: date) -> List[LoanInstallment]:
        """
        Fetches all loan installments that are scheduled and due on or before
        the specified posting date for all OPEN loans.
        """
        return (
            self.db.query(LoanInstallment)
            .join(DriverLoan)
            .filter(
                LoanInstallment.status == LoanInstallmentStatus.SCHEDULED,
                LoanInstallment.week_start_date <= post_date,
                DriverLoan.status == LoanStatus.OPEN,
            )
            .all()
        )

    def update_installment(self, installment_id: int, updates: dict):
        """Updates specific fields of a single installment record."""
        stmt = (
            update(LoanInstallment)
            .where(LoanInstallment.id == installment_id)
            .values(**updates)
        )
        self.db.execute(stmt)

    def list_loans(
        self,
        page: int,
        per_page: int,
        sort_by: str,
        sort_order: str,
        tlc_number: Optional[str] = None,
        lease_id: Optional[str] = None,
        loan_id: Optional[str] = None,
        status: Optional[List[str]] = None,
        driver_name: Optional[str] = None,
        medallion_no: Optional[str] = None,
        lease_type: Optional[str] = None,
        start_week_from: Optional[date] = None,
        start_week_to: Optional[date] = None,
        min_principal: Optional[Decimal] = None,
        max_principal: Optional[Decimal] = None,
        min_interest_rate: Optional[Decimal] = None,
        max_interest_rate: Optional[Decimal] = None,
    ) -> Tuple[List[DriverLoan], int]:
        """
        Retrieves a paginated, sorted, and filtered list of Driver Loans.
        Now includes filters for principal amount, interest rate ranges, and start week date range.
        """
        query = (
            self.db.query(DriverLoan)
            .options(
                joinedload(DriverLoan.driver).joinedload(Driver.tlc_license),
                joinedload(DriverLoan.medallion),
                joinedload(DriverLoan.lease),
            )
            .outerjoin(Driver, DriverLoan.driver_id == Driver.id)
            .outerjoin(Medallion, DriverLoan.medallion_id == Medallion.id)
            .outerjoin(Lease, DriverLoan.lease_id == Lease.id)
        )

        # Apply existing filters
        if tlc_number:
            query = (
                query.join(TLCLicense, TLCLicense.id == Driver.tlc_license_number_id)
                .filter(TLCLicense.tlc_license_number.ilike(f"%{tlc_number}%"))
            )
        if lease_id:
            query = query.filter(Lease.lease_id == lease_id)
        if loan_id:
            query = query.filter(DriverLoan.loan_id.ilike(f"%{loan_id}%"))
        if status:
            try:
                status_enums = [LoanStatus[s.upper()] for s in status]
                query = query.filter(DriverLoan.status.in_(status_enums))
            except KeyError:
                logger.warning(f"Invalid status filter for loans: {status}")
        if driver_name:
            query = query.filter(Driver.full_name.ilike(f"%{driver_name}%"))
        if medallion_no:
            query = query.filter(Medallion.medallion_number.ilike(f"%{medallion_no}%"))
        if lease_type:
            query = query.filter(Lease.lease_type.ilike(f"%{lease_type}%"))
        
        # UPDATED: Start week date range filter
        if start_week_from:
            query = query.filter(DriverLoan.start_week >= start_week_from)
        if start_week_to:
            query = query.filter(DriverLoan.start_week <= start_week_to)
        
        # Principal amount range
        if min_principal is not None:
            query = query.filter(DriverLoan.principal_amount >= min_principal)
        if max_principal is not None:
            query = query.filter(DriverLoan.principal_amount <= max_principal)
        
        # Interest rate range
        if min_interest_rate is not None:
            query = query.filter(DriverLoan.interest_rate >= min_interest_rate)
        if max_interest_rate is not None:
            query = query.filter(DriverLoan.interest_rate <= max_interest_rate)

        total_items = query.with_entities(func.count(DriverLoan.id)).scalar()

        # Apply sorting
        sort_column_map = {
            "loan_id": DriverLoan.loan_id,
            "status": DriverLoan.status,
            "driver": Driver.full_name,
            "driver_id": Driver.driver_id,
            "tlc_license": TLCLicense.tlc_license_number,
            "medallion_no": Medallion.medallion_number,
            "medallion_owner": Medallion.owner,
            "lease_type": Lease.lease_type,
            "amount": DriverLoan.principal_amount,
            "rate": DriverLoan.interest_rate,
            "start_week": DriverLoan.start_week,
        }
        
        sort_column = sort_column_map.get(sort_by, DriverLoan.start_week)
        if sort_order.lower() == "desc":
            query = query.order_by(sort_column.desc())
        else:
            query = query.order_by(sort_column.asc())

        query = query.offset((page - 1) * per_page).limit(per_page)

        return query.all(), total_items
    
    def list_installments(
        self,
        page: int,
        per_page: int,
        sort_by: str,
        sort_order: str,
        loan_id: Optional[str] = None,
        lease_id: Optional[int] = None,
        driver_id: Optional[int] = None,
        medallion_id: Optional[int] = None,
        vehicle_id: Optional[int] = None,
        status: Optional[str] = None,
    ) -> Tuple[List[LoanInstallment], int]:
        """
        Retrieves a paginated, sorted, and filtered list of Loan Installments.
        Supports filtering by loan_id, lease_id, driver_id, medallion_id, vehicle_id, and status.
        """
        from app.vehicles.models import Vehicle
        
        query = (
            self.db.query(LoanInstallment)
            .join(DriverLoan, LoanInstallment.loan_id == DriverLoan.id)
            .options(
                joinedload(LoanInstallment.loan)
                .joinedload(DriverLoan.driver),
                joinedload(LoanInstallment.loan)
                .joinedload(DriverLoan.medallion),
                joinedload(LoanInstallment.loan)
                .joinedload(DriverLoan.lease),
            )
            .outerjoin(Driver, DriverLoan.driver_id == Driver.id)
            .outerjoin(Medallion, DriverLoan.medallion_id == Medallion.id)
            .outerjoin(Lease, DriverLoan.lease_id == Lease.id)
            .outerjoin(Vehicle, Lease.vehicle_id == Vehicle.id)
        )

        # Apply filters
        if loan_id:
            query = query.filter(DriverLoan.loan_id.ilike(f"%{loan_id}%"))
        
        if lease_id:
            query = query.filter(DriverLoan.lease_id == lease_id)
        
        if driver_id:
            query = query.filter(DriverLoan.driver_id == driver_id)
        
        if medallion_id:
            query = query.filter(DriverLoan.medallion_id == medallion_id)
        
        if vehicle_id:
            query = query.filter(Lease.vehicle_id == vehicle_id)
        
        if status:
            try:
                status_enums = [LoanInstallmentStatus[s.upper()] for s in status.split(',')]
                query = query.filter(LoanInstallment.status.in_(status_enums))
            except KeyError:
                logger.warning(f"Invalid status filter for installments: {status}")
        
        total_items = query.with_entities(func.count(LoanInstallment.id)).scalar()

        # Apply sorting
        sort_column_map = {
            "installment_id": LoanInstallment.installment_id,
            "loan_id": DriverLoan.loan_id,
            "driver_name": Driver.full_name,
            "medallion_no": Medallion.medallion_number,
            "week_start_date": LoanInstallment.week_start_date,
            "principal_amount": LoanInstallment.principal_amount,
            "interest_amount": LoanInstallment.interest_amount,
            "total_due": LoanInstallment.total_due,
            "status": LoanInstallment.status,
            "posted_on": LoanInstallment.posted_on,
        }
        
        sort_column = sort_column_map.get(sort_by, LoanInstallment.week_start_date)
        if sort_order.lower() == "desc":
            query = query.order_by(sort_column.desc())
        else:
            query = query.order_by(sort_column.asc())

        query = query.offset((page - 1) * per_page).limit(per_page)

        return query.all(), total_items
    
    def get_installment_by_installment_id(
        self, installment_id: str
    ) -> Optional[LoanInstallment]:
        """Fetches a single loan installment by its installment id (e.g., DLN-2025-001-01)"""
        return (
            self.db.query(LoanInstallment)
            .join(DriverLoan)
            .filter(LoanInstallment.installment_id == installment_id)
            .options(joinedload(LoanInstallment.loan))
            .first()
        )
    
    def get_installments_by_ids(
        self, installment_ids: List[str]
    ) -> List[LoanInstallment]:
        """Fetches multiple loan installments by their installment_ids"""
        return (
            self.db.query(LoanInstallment)
            .join(DriverLoan)
            .filter(LoanInstallment.installment_id.in_(installment_ids))
            .options(joinedload(LoanInstallment.loan))
            .all()
        )
    
    def get_installment_by_installment_id(self, installment_id: str) -> Optional[LoanInstallment]:
        """Get installment by its system-generated ID."""
        return (
            self.db.query(LoanInstallment)
            .filter(LoanInstallment.installment_id == installment_id)
            .first()
        )
