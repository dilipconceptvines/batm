### app/repairs/repository.py

from datetime import date , datetime
from typing import List, Optional, Tuple
from decimal import Decimal


from sqlalchemy import func, update , or_
from sqlalchemy.orm import Session, joinedload

from app.drivers.models import Driver
from app.medallions.models import Medallion
from app.leases.models import Lease
from app.vehicles.models import Vehicle
from app.repairs.models import (
    RepairInstallment,
    RepairInstallmentStatus,
    RepairInvoice,
    RepairInvoiceStatus,
)
from app.utils.logger import get_logger

logger = get_logger(__name__)


class RepairRepository:
    """
    Data Access Layer for Vehicle Repairs.
    Handles all database interactions for RepairInvoice and RepairInstallment models.
    """

    def __init__(self, db: Session):
        self.db = db

    def create_invoice(self, invoice: RepairInvoice) -> RepairInvoice:
        """Adds a new RepairInvoice record to the session."""
        self.db.add(invoice)
        self.db.flush()
        self.db.refresh(invoice)
        logger.info("Created new RepairInvoice", repair_id=invoice.repair_id)
        return invoice

    def get_invoice_by_id(self, invoice_id: int) -> Optional[RepairInvoice]:
        """Fetches a single repair invoice by its primary key."""
        return self.db.query(RepairInvoice).filter(RepairInvoice.id == invoice_id).first()

    def get_invoice_by_repair_id(self, repair_id: str) -> Optional[RepairInvoice]:
        """Fetches a single repair invoice by the system-generated Repair ID."""
        return self.db.query(RepairInvoice).filter(RepairInvoice.repair_id == repair_id).first()

    def get_last_repair_id_for_year(self, year: int) -> Optional[str]:
        """Finds the last used repair_id for a given year to determine the next sequence number."""
        prefix = f"RPR-{year}-"
        return (
            self.db.query(RepairInvoice.repair_id)
            .filter(RepairInvoice.repair_id.like(f"{prefix}%"))
            .order_by(RepairInvoice.repair_id.desc())
            .first()
        )

    def bulk_insert_installments(self, installments: List[RepairInstallment]):
        """Performs a bulk insert of new RepairInstallment records."""
        if installments:
            self.db.add_all(installments)

    def update_invoice(self, invoice_id: int, updates: dict):
        """Updates specific fields of a single invoice record."""
        stmt = (
            update(RepairInvoice)
            .where(RepairInvoice.id == invoice_id)
            .values(**updates)
        )
        self.db.execute(stmt)

    def get_due_installments_to_post(self, post_date: date) -> List[RepairInstallment]:
        """
        Fetches all repair installments that are scheduled and due on or before
        the specified posting date.
        """
        return (
            self.db.query(RepairInstallment)
            .join(RepairInvoice)
            .filter(
                RepairInstallment.status == RepairInstallmentStatus.SCHEDULED,
                RepairInstallment.week_start_date <= post_date,
                RepairInvoice.status == RepairInvoiceStatus.OPEN,
            )
            .all()
        )

    def update_installment(self, installment_id: int, updates: dict):
        """Updates specific fields of a single installment record."""
        stmt = (
            update(RepairInstallment)
            .where(RepairInstallment.id == installment_id)
            .values(**updates)
        )
        self.db.execute(stmt)

    def get_installment_by_installment_id(self, installment_id: str) -> Optional[RepairInstallment]:
        """Fetches a single repair installment by its installment_id."""
        return self.db.query(RepairInstallment).filter(RepairInstallment.installment_id == installment_id).first()

    def list_invoices(
        self,
        page: int,
        per_page: int,
        sort_by: str,
        sort_order: str,
        repair_id: Optional[str] = None,
        invoice_number: Optional[str] = None,
        from_invoice_date: Optional[date] = None,
        to_invoice_date: Optional[date] = None,
        lease_type: Optional[str] = None,
        workshop_type: Optional[str] = None,
        from_total_amount: Optional[Decimal] = None,
        to_total_amount: Optional[Decimal] = None,
        status: Optional[str] = None,
        driver_name: Optional[str] = None,
        medallion_no: Optional[str] = None,
        lease_id: Optional[str] = None,
        vin: Optional[str] = None,
    ) -> Tuple[List[RepairInvoice], int]:
        """
        Retrieves a paginated, sorted, and filtered list of Repair Invoices.
        """
        query = (
            self.db.query(RepairInvoice)
            .options(
                joinedload(RepairInvoice.driver),
                joinedload(RepairInvoice.medallion),
                joinedload(RepairInvoice.lease),
                joinedload(RepairInvoice.vehicle),
            )
            .outerjoin(Driver, RepairInvoice.driver_id == Driver.id)
            .outerjoin(Medallion, RepairInvoice.medallion_id == Medallion.id)
            .outerjoin(Lease, RepairInvoice.lease_id == Lease.id)
            .outerjoin(Vehicle, RepairInvoice.vehicle_id == Vehicle.id)
        
        )

        # Apply filters
        if repair_id:
            ids = [id.strip() for id in repair_id.split(',') if id.strip()]
            query = query.filter(or_(*[RepairInvoice.repair_id.ilike(f"%{id}%") for id in ids]))

        if invoice_number:
            numbers = [number.strip() for number in invoice_number.split(',') if number.strip()]
            query = query.filter(or_(*[RepairInvoice.invoice_number.ilike(f"%{number}%") for number in numbers]))

        if from_invoice_date:
            from_invoice_date = datetime.combine(from_invoice_date, datetime.min.time())
            query = query.filter(RepairInvoice.invoice_date >= from_invoice_date)

        if to_invoice_date:
            to_invoice_date = datetime.combine(to_invoice_date, datetime.max.time())
            query = query.filter(RepairInvoice.invoice_date <= to_invoice_date)

        if lease_type:
            types = [type.strip() for type in lease_type.split(',') if type.strip()]
            query = query.filter(Lease.lease_type.in_(types))

        if workshop_type:
            types = [type.strip() for type in workshop_type.split(',') if type.strip()]
            query = query.filter(RepairInvoice.workshop_type.in_(types))

        if from_total_amount:
            query = query.filter(RepairInvoice.total_amount >= from_total_amount)

        if to_total_amount:
            query = query.filter(RepairInvoice.total_amount <= to_total_amount)

        if status:
            try:
                sts = [st.strip() for st in status.split(',') if st.strip()]
                query = query.filter(RepairInvoice.status.in_(sts))
            except KeyError:
                logger.warning(f"Invalid status filter for repairs: {status}")

        if driver_name:
            names = [name.strip() for name in driver_name.split(',') if name.strip()]
            query = query.filter(or_(*[Driver.full_name.ilike(f"%{name}%") for name in names]))

        if medallion_no:
            nos = [no.strip() for no in medallion_no.split(',') if no.strip()]
            query = query.filter(or_(*[Medallion.medallion_number.ilike(f"%{no}%") for no in nos]))
            
        if lease_id:
            lease_ids = [id.strip() for id in lease_id.split(',') if id.strip()]
            query = query.filter(or_(*[Lease.lease_id.ilike(f"%{id}%") for id in lease_ids]))
            
        if vin:
            vins = [vin.strip() for vin in vin.split(',') if vin.strip()]
            query = query.filter(or_(*[Vehicle.vin.ilike(f"%{vin}%") for vin in vins]))

        total_items = query.with_entities(func.count(RepairInvoice.id)).scalar()

        # Apply sorting
        sort_column_map = {
            "repair_id": RepairInvoice.repair_id,
            "invoice_number": RepairInvoice.invoice_number,
            "invoice_date": RepairInvoice.invoice_date,
            "total_amount": RepairInvoice.total_amount,
            "workshop_type": RepairInvoice.workshop_type,
            "status": RepairInvoice.status,
            "driver": Driver.full_name,
            "lease_type": Lease.lease_type,
            "medallion_no": Medallion.medallion_number,
            "amount": RepairInvoice.total_amount,
        }
        
        sort_column = sort_column_map.get(sort_by, RepairInvoice.invoice_date)
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
        repair_id: Optional[str] = None,
        lease_id: Optional[int] = None,
        driver_id: Optional[int] = None,
        medallion_id: Optional[int] = None,
        vehicle_id: Optional[int] = None,
        status: Optional[str] = None,
    ) -> Tuple[List[RepairInstallment], int]:
        """
        Retrieves a paginated, sorted, and filtered list of Repair Installments.
        Supports filtering by repair_id, lease_id, driver_id, medallion_id, vehicle_id, and status.
        """
        from app.leases.models import Lease
        from app.vehicles.models import Vehicle
        
        query = (
            self.db.query(RepairInstallment)
            .join(RepairInvoice, RepairInstallment.invoice_id == RepairInvoice.id)
            .options(
                joinedload(RepairInstallment.invoice)
                .joinedload(RepairInvoice.driver),
                joinedload(RepairInstallment.invoice)
                .joinedload(RepairInvoice.medallion),
                joinedload(RepairInstallment.invoice)
                .joinedload(RepairInvoice.lease),
                joinedload(RepairInstallment.invoice)
                .joinedload(RepairInvoice.vehicle),
            )
            .outerjoin(Driver, RepairInvoice.driver_id == Driver.id)
            .outerjoin(Medallion, RepairInvoice.medallion_id == Medallion.id)
            .outerjoin(Lease, RepairInvoice.lease_id == Lease.id)
            .outerjoin(Vehicle, RepairInvoice.vehicle_id == Vehicle.id)
        )

        # Apply filters
        if repair_id:
            query = query.filter(RepairInvoice.repair_id.ilike(f"%{repair_id}%"))
        
        if lease_id:
            query = query.filter(RepairInvoice.lease_id == lease_id)
        
        if driver_id:
            query = query.filter(RepairInvoice.driver_id == driver_id)
        
        if medallion_id:
            query = query.filter(RepairInvoice.medallion_id == medallion_id)
        
        if vehicle_id:
            query = query.filter(RepairInvoice.vehicle_id == vehicle_id)
        
        if status:
            try:
                sts = [st.strip().upper() for st in status.split(',') if st.strip()]
                query = query.filter(RepairInstallment.status.in_(sts))
            except KeyError:
                logger.warning(f"Invalid status filter for repair installments: {status}")

        total_items = query.with_entities(func.count(RepairInstallment.id)).scalar()

        # Apply sorting
        sort_column_map = {
            "installment_id": RepairInstallment.installment_id,
            "repair_id": RepairInvoice.repair_id,
            "driver_name": Driver.full_name,
            "medallion_no": Medallion.medallion_number,
            "week_start_date": RepairInstallment.week_start_date,
            "principal_amount": RepairInstallment.principal_amount,
            "status": RepairInstallment.status,
            "posted_on": RepairInstallment.posted_on,
        }
        
        sort_column = sort_column_map.get(sort_by, RepairInstallment.week_start_date)
        if sort_order.lower() == "desc":
            query = query.order_by(sort_column.desc())
        else:
            query = query.order_by(sort_column.asc())

        query = query.offset((page - 1) * per_page).limit(per_page)

        return query.all(), total_items
    
    def get_installments_by_invoice(self, invoice_id: int) -> List[RepairInstallment]:
        """Get all installments for a given invoice."""
        return (
            self.db.query(RepairInstallment)
            .filter(RepairInstallment.invoice_id == invoice_id)
            .order_by(RepairInstallment.week_start_date.asc())
            .all()
        )

    # def get_invoice_by_id(self, invoice_id: int) -> Optional[RepairInvoice]:
    #     """Get invoice by primary key."""
    #     return self.db.query(RepairInvoice).filter(RepairInvoice.id == invoice_id).first()