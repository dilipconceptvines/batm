### app/interim_payments/repository.py

from datetime import date, datetime
from typing import List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.drivers.models import Driver
from app.interim_payments.models import InterimPayment
from app.leases.models import Lease
from app.medallions.models import Medallion
from app.utils.logger import get_logger

logger = get_logger(__name__)


class InterimPaymentRepository:
    """
    Data Access Layer for Interim Payments.
    Handles all database interactions for the InterimPayment model.
    """

    def __init__(self, db: Session):
        self.db = db

    def create_payment(self, payment: InterimPayment) -> InterimPayment:
        """Adds a new InterimPayment record to the session and commits."""
        self.db.add(payment)
        self.db.flush()
        self.db.refresh(payment)
        logger.info("Created new InterimPayment", payment_id=payment.payment_id)
        return payment

    def get_payment_by_id(self, payment_pk_id: int) -> Optional[InterimPayment]:
        """Fetches a single interim payment by its primary key."""
        return self.db.query(InterimPayment).filter(InterimPayment.id == payment_pk_id).first()

    def get_payment_by_payment_id(self, payment_id: str) -> Optional[InterimPayment]:
        """Fetches a single interim payment by the system-generated Payment ID."""
        return self.db.query(InterimPayment).filter(InterimPayment.payment_id == payment_id).first()
    
    def get_last_payment_id_for_year(self, year: int) -> Optional[str]:
        """Finds the last used payment_id for a given year to determine the next sequence number."""
        prefix = f"INTPAY-{year}-"
        return (
            self.db.query(InterimPayment.payment_id)
            .filter(InterimPayment.payment_id.like(f"{prefix}%"))
            .order_by(InterimPayment.payment_id.desc())
            .first()
        )

    def list_payments(
        self,
        page: int,
        per_page: int,
        sort_by: str,
        sort_order: str,
        payment_id: Optional[str] = None,
        driver_name: Optional[str] = None,
        tlc_license: Optional[str] = None,
        lease_id: Optional[str] = None,
        medallion_no: Optional[str] = None,
        payment_date: Optional[date] = None,
        # New filters
        category: Optional[str] = None,
        reference_id: Optional[str] = None,
        amount_from: Optional[float] = None,
        amount_to: Optional[float] = None,
        payment_date_from: Optional[date] = None,
        payment_date_to: Optional[date] = None,
        payment_method: Optional[str] = None,
    ) -> Tuple[List[InterimPayment], int]:
        """
        Retrieves a paginated, sorted, and filtered list of Interim Payments.
        """
        query = (
            self.db.query(InterimPayment)
            .options(
                joinedload(InterimPayment.driver),
                joinedload(InterimPayment.lease).joinedload(Lease.medallion),
            )
            .join(Driver, InterimPayment.driver_id == Driver.id)
            .join(Lease, InterimPayment.lease_id == Lease.id)
            .outerjoin(Medallion, Lease.medallion_id == Medallion.id)
        )

        # Apply filters
        if payment_id:
            query = query.filter(InterimPayment.payment_id.ilike(f"%{payment_id}%"))
        if driver_name:
            query = query.filter(Driver.full_name.ilike(f"%{driver_name}%"))
        if tlc_license:
            # Assuming TLC License is on the Driver model via a relationship
            from app.drivers.models import TLCLicense
            query = query.join(TLCLicense, Driver.tlc_license_number_id == TLCLicense.id).filter(TLCLicense.tlc_license_number.ilike(f"%{tlc_license}%"))
        if lease_id:
            query = query.filter(Lease.lease_id.ilike(f"%{lease_id}%"))
        if medallion_no:
            query = query.filter(Medallion.medallion_number.ilike(f"%{medallion_no}%"))
        if payment_date:
            start_of_day = datetime.combine(payment_date, datetime.min.time())
            end_of_day = datetime.combine(payment_date, datetime.max.time())
            query = query.filter(InterimPayment.payment_date.between(start_of_day, end_of_day))
        
        # New filter implementations
        if payment_method:
            query = query.filter(InterimPayment.payment_method == payment_method)
            
        if payment_date_from:
            start_of_day = datetime.combine(payment_date_from, datetime.min.time())
            query = query.filter(InterimPayment.payment_date >= start_of_day)
            
        if payment_date_to:
            end_of_day = datetime.combine(payment_date_to, datetime.max.time())
            query = query.filter(InterimPayment.payment_date <= end_of_day)
            
        # JSON allocation filters
        if category:
            query = query.filter(func.json_extract(InterimPayment.allocations, '$[*].category').like(f'%"{category}"%'))
            
        if reference_id:
            query = query.filter(func.json_extract(InterimPayment.allocations, '$[*].reference_id').like(f'%"{reference_id}"%'))
            
        if amount_from is not None:
            query = query.filter(func.json_extract(InterimPayment.allocations, '$[*].amount') >= amount_from)
            
        if amount_to is not None:
            query = query.filter(func.json_extract(InterimPayment.allocations, '$[*].amount') <= amount_to)

        # Get total count before applying pagination  
        total_items = query.count()

        # Apply sorting
        sort_column_map = {
            "payment_id": InterimPayment.payment_id,
            "driver_name": Driver.full_name,
            "lease_id": Lease.lease_id,
            "medallion_no": Medallion.medallion_number,
            "payment_date": InterimPayment.payment_date,
            "amount": InterimPayment.total_amount,
            "payment_method": InterimPayment.payment_method,
        }
        
        sort_column = sort_column_map.get(sort_by, InterimPayment.payment_date)
        if sort_order.lower() == "desc":
            query = query.order_by(sort_column.desc())
        else:
            query = query.order_by(sort_column.asc())

        query = query.offset((page - 1) * per_page).limit(per_page)

        return query.all(), total_items

    def get_available_categories(self) -> List[str]:
        """
        Retrieves a list of unique categories from all allocations.
        """
        # Query all payments with non-null allocations
        payments = self.db.query(InterimPayment).filter(
            InterimPayment.allocations.isnot(None),
            InterimPayment.allocations != '[]'
        ).all()
        
        categories = set()
        for payment in payments:
            if payment.allocations:
                for allocation in payment.allocations:
                    if 'category' in allocation:
                        categories.add(allocation['category'])
        
        return sorted(list(categories))