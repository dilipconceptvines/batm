# app/driver_payments/unified_service.py

"""
Unified Driver Payments Service
Combines DTRs, Interim Payments, Driver Loans, and Vehicle Repairs into a single view
"""

from datetime import date, datetime
from typing import List, Tuple, Optional
from decimal import Decimal

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_, and_, func, case

from app.dtr.models import DTR, DTRStatus, PaymentMethod as DTRPaymentMethod
from app.interim_payments.models import InterimPayment, PaymentMethod as InterimPaymentMethod
from app.loans.models import DriverLoan, LoanStatus
from app.repairs.models import RepairInvoice, RepairInvoiceStatus
from app.drivers.models import Driver, TLCLicense
from app.leases.models import Lease
from app.medallions.models import Medallion
from app.vehicles.models import Vehicle, VehicleRegistration
from app.utils.s3_utils import s3_utils
from app.utils.logger import get_logger

logger = get_logger(__name__)


class UnifiedPaymentItem:
    """Unified data structure for all payment types"""
    
    def __init__(
        self,
        id: int,
        receipt_type: str,
        receipt_number: str,
        payment_date: date,
        medallion_number: Optional[str],
        tlc_license: Optional[str],
        driver_name: Optional[str],
        plate_number: Optional[str],
        total_amount: Decimal,
        status: str,
        payment_method: Optional[str],
        ach_batch_number: Optional[str] = None,
        check_number: Optional[str] = None,
        receipt_url: Optional[str] = None,
        week_start_date: Optional[date] = None,
        week_end_date: Optional[str] = None
    ):
        self.id = id
        self.receipt_type = receipt_type
        self.receipt_number = receipt_number
        self.payment_date = payment_date
        self.medallion_number = medallion_number
        self.tlc_license = tlc_license
        self.driver_name = driver_name
        self.plate_number = plate_number
        self.total_amount = total_amount
        self.status = status
        self.payment_method = payment_method
        self.ach_batch_number = ach_batch_number
        self.check_number = check_number
        self.receipt_url = receipt_url
        self.week_start_date = week_start_date
        self.week_end_date = week_end_date


class UnifiedDriverPaymentsService:
    """Service to combine all driver payment types"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def get_unified_payments(
        self,
        page: int = 1,
        per_page: int = 50,
        receipt_number: Optional[str] = None,
        status: Optional[str] = None,
        payment_method: Optional[str] = None,
        week_start_date_from: Optional[date] = None,
        week_start_date_to: Optional[date] = None,
        week_end_date_from: Optional[date] = None,
        week_end_date_to: Optional[date] = None,
        ach_batch_number: Optional[str] = None,
        total_due_min: Optional[float] = None,
        total_due_max: Optional[float] = None,
        receipt_type: Optional[str] = None,
        medallion_number: Optional[str] = None,
        tlc_license: Optional[str] = None,
        driver_name: Optional[str] = None,
        plate_number: Optional[str] = None,
        check_number: Optional[str] = None,
        sort_by: str = 'payment_date',
        sort_order: str = 'desc'
    ) -> Tuple[List[UnifiedPaymentItem], int]:
        """
        Retrieve unified list of all driver payments from multiple sources.
        
        Returns: (list_of_payments, total_count)
        """
        
        # Collect all payments from different sources
        all_payments = []
        
        # 1. Get DTRs
        if not receipt_type or receipt_type == "DTR":
            dtr_payments = self._get_dtr_payments(
                receipt_number=receipt_number,
                status=status,
                payment_method=payment_method,
                week_start_date_from=week_start_date_from,
                week_start_date_to=week_start_date_to,
                week_end_date_from=week_end_date_from,
                week_end_date_to=week_end_date_to,
                ach_batch_number=ach_batch_number,
                total_due_min=total_due_min,
                total_due_max=total_due_max,
                medallion_number=medallion_number,
                tlc_license=tlc_license,
                driver_name=driver_name,
                plate_number=plate_number,
                check_number=check_number
            )
            all_payments.extend(dtr_payments)
        
        # 2. Get Interim Payments
        if not receipt_type or receipt_type == "Interim Payment":
            interim_payments = self._get_interim_payments(
                receipt_number=receipt_number,
                payment_method=payment_method,
                total_due_min=total_due_min,
                total_due_max=total_due_max,
                medallion_number=medallion_number,
                tlc_license=tlc_license,
                driver_name=driver_name
            )
            all_payments.extend(interim_payments)
        
        # 3. Get Driver Loans
        if not receipt_type or receipt_type == "Driver Loan":
            loan_payments = self._get_driver_loan_payments(
                receipt_number=receipt_number,
                status=status,
                total_due_min=total_due_min,
                total_due_max=total_due_max,
                medallion_number=medallion_number,
                tlc_license=tlc_license,
                driver_name=driver_name
            )
            all_payments.extend(loan_payments)
        
        # 4. Get Vehicle Repairs
        if not receipt_type or receipt_type == "Vehicle Repair":
            repair_payments = self._get_vehicle_repair_payments(
                receipt_number=receipt_number,
                status=status,
                total_due_min=total_due_min,
                total_due_max=total_due_max,
                medallion_number=medallion_number,
                tlc_license=tlc_license,
                driver_name=driver_name,
                plate_number=plate_number
            )
            all_payments.extend(repair_payments)
        
        # Sort the combined results
        all_payments = self._sort_payments(all_payments, sort_by, sort_order)
        
        # Get total count before pagination
        total = len(all_payments)
        
        # Apply pagination
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_payments = all_payments[start_idx:end_idx]
        
        return paginated_payments, total
    
    def _get_dtr_payments(self, **filters) -> List[UnifiedPaymentItem]:
        """Get DTR payments"""
        query = self.db.query(DTR).options(
            joinedload(DTR.lease),
            joinedload(DTR.primary_driver).joinedload(Driver.tlc_license),
            joinedload(DTR.vehicle),
            joinedload(DTR.medallion)
        )
        
        # Apply DTR-specific filters
        if filters.get('receipt_number'):
            query = query.filter(DTR.receipt_number.ilike(f'%{filters["receipt_number"]}%'))
        
        if filters.get('status'):
            # Map generic status to DTRStatus enum
            try:
                dtr_status = DTRStatus[filters['status'].upper()]
                query = query.filter(DTR.status == dtr_status)
            except KeyError:
                pass
        
        if filters.get('payment_method'):
            try:
                pm = DTRPaymentMethod[filters['payment_method'].upper()]
                query = query.filter(DTR.payment_method == pm)
            except KeyError:
                pass
        
        # Date filters
        if filters.get('week_start_date_from'):
            query = query.filter(DTR.week_start_date >= filters['week_start_date_from'])
        if filters.get('week_start_date_to'):
            query = query.filter(DTR.week_start_date <= filters['week_start_date_to'])
        if filters.get('week_end_date_from'):
            query = query.filter(DTR.week_end_date >= filters['week_end_date_from'])
        if filters.get('week_end_date_to'):
            query = query.filter(DTR.week_end_date <= filters['week_end_date_to'])
        
        if filters.get('ach_batch_number'):
            query = query.filter(DTR.ach_batch_number == filters['ach_batch_number'])
        
        if filters.get('check_number'):
            query = query.filter(DTR.check_number.ilike(f'%{filters["check_number"]}%'))
        
        # Amount filters
        if filters.get('total_due_min') is not None:
            query = query.filter(DTR.total_due_to_driver >= filters['total_due_min'])
        if filters.get('total_due_max') is not None:
            query = query.filter(DTR.total_due_to_driver <= filters['total_due_max'])
        
        # Join filters for related entities
        if filters.get('medallion_number'):
            med_vals = [m.strip() for m in filters['medallion_number'].split(',') if m.strip()]
            if med_vals:
                query = query.join(DTR.medallion).filter(
                    or_(*[Medallion.medallion_number.ilike(f'%{m}%') for m in med_vals])
                )
        
        if filters.get('tlc_license'):
            tlc_vals = [t.strip() for t in filters['tlc_license'].split(',') if t.strip()]
            if tlc_vals:
                query = query.join(DTR.primary_driver).join(
                    TLCLicense, Driver.tlc_license_number_id == TLCLicense.id, isouter=True
                ).filter(
                    or_(*[TLCLicense.tlc_license_number.ilike(f'%{t}%') for t in tlc_vals])
                )
        
        if filters.get('driver_name'):
            name_vals = [n.strip() for n in filters['driver_name'].split(',') if n.strip()]
            if name_vals:
                exprs = []
                for n in name_vals:
                    like = f'%{n}%'
                    exprs.extend([
                        Driver.first_name.ilike(like),
                        Driver.last_name.ilike(like),
                        func.concat(Driver.first_name, ' ', Driver.last_name).ilike(like)
                    ])
                query = query.join(DTR.primary_driver).filter(or_(*exprs))
        
        if filters.get('plate_number'):
            plate_vals = [p.strip() for p in filters['plate_number'].split(',') if p.strip()]
            if plate_vals:
                query = query.join(DTR.vehicle).join(
                    VehicleRegistration,
                    VehicleRegistration.vehicle_id == Vehicle.id
                ).filter(
                    or_(*[VehicleRegistration.plate_number.ilike(f'%{p}%') for p in plate_vals])
                )
        
        dtrs = query.all()
        
        # Convert to UnifiedPaymentItem
        result = []
        for dtr in dtrs:
            # Get plate number
            plate = None
            if dtr.vehicle:
                if hasattr(dtr.vehicle, 'get_active_plate_number'):
                    plate = dtr.vehicle.get_active_plate_number()
                elif hasattr(dtr.vehicle, 'plate_number'):
                    plate = dtr.vehicle.plate_number
            
            # Generate presigned URL for DTR receipt (DTRs have PDF stored)
            receipt_url = None
            if hasattr(dtr, 'receipt_s3_key') and dtr.receipt_s3_key:
                receipt_url = s3_utils.generate_presigned_url(dtr.receipt_s3_key, expiration=3600)
            
            item = UnifiedPaymentItem(
                id=dtr.id,
                receipt_type="DTR",
                receipt_number=dtr.receipt_number or dtr.dtr_number,
                payment_date=dtr.week_end_date,  # Use week_end_date as payment date
                medallion_number=dtr.medallion.medallion_number if dtr.medallion else None,
                tlc_license=(dtr.primary_driver.tlc_license.tlc_license_number 
                           if dtr.primary_driver and dtr.primary_driver.tlc_license else None),
                driver_name=(f"{dtr.primary_driver.first_name} {dtr.primary_driver.last_name}" 
                           if dtr.primary_driver else None),
                plate_number=plate,
                total_amount=dtr.total_due_to_driver or Decimal('0'),
                status=dtr.status.value if dtr.status else "Unknown",
                payment_method=dtr.payment_method.value if dtr.payment_method else None,
                ach_batch_number=dtr.ach_batch_number,
                check_number=dtr.check_number,
                receipt_url=receipt_url,
                week_start_date=dtr.week_start_date,
                week_end_date=dtr.week_end_date
            )
            result.append(item)
        
        return result
    
    def _get_interim_payments(self, **filters) -> List[UnifiedPaymentItem]:
        """Get Interim Payment receipts"""
        query = self.db.query(InterimPayment).options(
            joinedload(InterimPayment.driver).joinedload(Driver.tlc_license),
            joinedload(InterimPayment.lease).joinedload(Lease.medallion)
        )
        
        # Apply filters
        if filters.get('receipt_number'):
            query = query.filter(InterimPayment.payment_id.ilike(f'%{filters["receipt_number"]}%'))
        
        if filters.get('payment_method'):
            try:
                pm = InterimPaymentMethod[filters['payment_method'].upper()]
                query = query.filter(InterimPayment.payment_method == pm)
            except KeyError:
                pass
        
        # Amount filters
        if filters.get('total_due_min') is not None:
            query = query.filter(InterimPayment.total_amount >= filters['total_due_min'])
        if filters.get('total_due_max') is not None:
            query = query.filter(InterimPayment.total_amount <= filters['total_due_max'])
        
        # Join filters
        if filters.get('medallion_number'):
            med_vals = [m.strip() for m in filters['medallion_number'].split(',') if m.strip()]
            if med_vals:
                query = query.join(InterimPayment.lease).join(Lease.medallion).filter(
                    or_(*[Medallion.medallion_number.ilike(f'%{m}%') for m in med_vals])
                )
        
        if filters.get('tlc_license'):
            tlc_vals = [t.strip() for t in filters['tlc_license'].split(',') if t.strip()]
            if tlc_vals:
                query = query.join(InterimPayment.driver).join(
                    TLCLicense, Driver.tlc_license_number_id == TLCLicense.id, isouter=True
                ).filter(
                    or_(*[TLCLicense.tlc_license_number.ilike(f'%{t}%') for t in tlc_vals])
                )
        
        if filters.get('driver_name'):
            name_vals = [n.strip() for n in filters['driver_name'].split(',') if n.strip()]
            if name_vals:
                exprs = []
                for n in name_vals:
                    like = f'%{n}%'
                    exprs.extend([
                        Driver.first_name.ilike(like),
                        Driver.last_name.ilike(like),
                        func.concat(Driver.first_name, ' ', Driver.last_name).ilike(like)
                    ])
                query = query.join(InterimPayment.driver).filter(or_(*exprs))
        
        payments = query.all()
        
        # Convert to UnifiedPaymentItem
        result = []
        for payment in payments:
            # Generate presigned URL for receipt
            receipt_url = None
            if payment.receipt_s3_key:
                receipt_url = s3_utils.generate_presigned_url(payment.receipt_s3_key, expiration=3600)
            
            item = UnifiedPaymentItem(
                id=payment.id,
                receipt_type="Interim Payment",
                receipt_number=payment.payment_id,
                payment_date=payment.payment_date.date() if payment.payment_date else None,
                medallion_number=(payment.lease.medallion.medallion_number 
                                if payment.lease and payment.lease.medallion else None),
                tlc_license=(payment.driver.tlc_license.tlc_license_number 
                           if payment.driver and payment.driver.tlc_license else None),
                driver_name=payment.driver.full_name if payment.driver else None,
                plate_number=None,  # Interim payments don't have plate info
                total_amount=payment.total_amount or Decimal('0'),
                status="Completed",  # Interim payments are always completed once created
                payment_method=payment.payment_method.value if payment.payment_method else None,
                receipt_url=receipt_url
            )
            result.append(item)
        
        return result
    
    def _get_driver_loan_payments(self, **filters) -> List[UnifiedPaymentItem]:
        """Get Driver Loan receipts"""
        query = self.db.query(DriverLoan).options(
            joinedload(DriverLoan.driver).joinedload(Driver.tlc_license),
            joinedload(DriverLoan.medallion),
            joinedload(DriverLoan.lease)
        )
        
        # Apply filters
        if filters.get('receipt_number'):
            query = query.filter(DriverLoan.loan_id.ilike(f'%{filters["receipt_number"]}%'))
        
        if filters.get('status'):
            try:
                loan_status = LoanStatus[filters['status'].upper()]
                query = query.filter(DriverLoan.status == loan_status)
            except KeyError:
                pass
        
        # Amount filters
        if filters.get('total_due_min') is not None:
            query = query.filter(DriverLoan.principal_amount >= filters['total_due_min'])
        if filters.get('total_due_max') is not None:
            query = query.filter(DriverLoan.principal_amount <= filters['total_due_max'])
        
        # Join filters
        if filters.get('medallion_number'):
            med_vals = [m.strip() for m in filters['medallion_number'].split(',') if m.strip()]
            if med_vals:
                query = query.join(DriverLoan.medallion).filter(
                    or_(*[Medallion.medallion_number.ilike(f'%{m}%') for m in med_vals])
                )
        
        if filters.get('tlc_license'):
            tlc_vals = [t.strip() for t in filters['tlc_license'].split(',') if t.strip()]
            if tlc_vals:
                query = query.join(DriverLoan.driver).join(
                    TLCLicense, Driver.tlc_license_number_id == TLCLicense.id, isouter=True
                ).filter(
                    or_(*[TLCLicense.tlc_license_number.ilike(f'%{t}%') for t in tlc_vals])
                )
        
        if filters.get('driver_name'):
            name_vals = [n.strip() for n in filters['driver_name'].split(',') if n.strip()]
            if name_vals:
                exprs = []
                for n in name_vals:
                    like = f'%{n}%'
                    exprs.extend([
                        Driver.first_name.ilike(like),
                        Driver.last_name.ilike(like),
                        func.concat(Driver.first_name, ' ', Driver.last_name).ilike(like)
                    ])
                query = query.join(DriverLoan.driver).filter(or_(*exprs))
        
        loans = query.all()
        
        # Convert to UnifiedPaymentItem
        result = []
        for loan in loans:
            # Get presigned URL using the property
            receipt_url = loan.presigned_receipt_url
            
            item = UnifiedPaymentItem(
                id=loan.id,
                receipt_type="Driver Loan",
                receipt_number=loan.loan_id,
                payment_date=loan.loan_date,
                medallion_number=loan.medallion.medallion_number if loan.medallion else None,
                tlc_license=(loan.driver.tlc_license.tlc_license_number 
                           if loan.driver and loan.driver.tlc_license else None),
                driver_name=loan.driver.full_name if loan.driver else None,
                plate_number=None,  # Loans don't have plate info
                total_amount=loan.principal_amount or Decimal('0'),
                status=loan.status.value if loan.status else "Unknown",
                payment_method=None,  # Loans don't have payment method
                receipt_url=receipt_url
            )
            result.append(item)
        
        return result
    
    def _get_vehicle_repair_payments(self, **filters) -> List[UnifiedPaymentItem]:
        """Get Vehicle Repair receipts"""
        query = self.db.query(RepairInvoice).options(
            joinedload(RepairInvoice.driver).joinedload(Driver.tlc_license),
            joinedload(RepairInvoice.medallion),
            joinedload(RepairInvoice.vehicle),
            joinedload(RepairInvoice.lease)
        )
        
        # Apply filters
        if filters.get('receipt_number'):
            query = query.filter(RepairInvoice.repair_id.ilike(f'%{filters["receipt_number"]}%'))
        
        if filters.get('status'):
            try:
                repair_status = RepairInvoiceStatus[filters['status'].upper()]
                query = query.filter(RepairInvoice.status == repair_status)
            except KeyError:
                pass
        
        # Amount filters
        if filters.get('total_due_min') is not None:
            query = query.filter(RepairInvoice.total_amount >= filters['total_due_min'])
        if filters.get('total_due_max') is not None:
            query = query.filter(RepairInvoice.total_amount <= filters['total_due_max'])
        
        # Join filters
        if filters.get('medallion_number'):
            med_vals = [m.strip() for m in filters['medallion_number'].split(',') if m.strip()]
            if med_vals:
                query = query.join(RepairInvoice.medallion).filter(
                    or_(*[Medallion.medallion_number.ilike(f'%{m}%') for m in med_vals])
                )
        
        if filters.get('tlc_license'):
            tlc_vals = [t.strip() for t in filters['tlc_license'].split(',') if t.strip()]
            if tlc_vals:
                query = query.join(RepairInvoice.driver).join(
                    TLCLicense, Driver.tlc_license_number_id == TLCLicense.id, isouter=True
                ).filter(
                    or_(*[TLCLicense.tlc_license_number.ilike(f'%{t}%') for t in tlc_vals])
                )
        
        if filters.get('driver_name'):
            name_vals = [n.strip() for n in filters['driver_name'].split(',') if n.strip()]
            if name_vals:
                exprs = []
                for n in name_vals:
                    like = f'%{n}%'
                    exprs.extend([
                        Driver.first_name.ilike(like),
                        Driver.last_name.ilike(like),
                        func.concat(Driver.first_name, ' ', Driver.last_name).ilike(like)
                    ])
                query = query.join(RepairInvoice.driver).filter(or_(*exprs))
        
        if filters.get('plate_number'):
            plate_vals = [p.strip() for p in filters['plate_number'].split(',') if p.strip()]
            if plate_vals:
                query = query.join(RepairInvoice.vehicle).join(
                    VehicleRegistration,
                    VehicleRegistration.vehicle_id == Vehicle.id
                ).filter(
                    or_(*[VehicleRegistration.plate_number.ilike(f'%{p}%') for p in plate_vals])
                )
        
        repairs = query.all()
        
        # Convert to UnifiedPaymentItem
        result = []
        for repair in repairs:
            # Get presigned URL using the property
            receipt_url = repair.presigned_receipt_url
            
            # Get plate number
            plate = None
            if repair.vehicle:
                if hasattr(repair.vehicle, 'get_active_plate_number'):
                    plate = repair.vehicle.get_active_plate_number()
                elif hasattr(repair.vehicle, 'plate_number'):
                    plate = repair.vehicle.plate_number
            
            item = UnifiedPaymentItem(
                id=repair.id,
                receipt_type="Vehicle Repair",
                receipt_number=repair.repair_id,
                payment_date=repair.invoice_date,
                medallion_number=repair.medallion.medallion_number if repair.medallion else None,
                tlc_license=(repair.driver.tlc_license.tlc_license_number 
                           if repair.driver and repair.driver.tlc_license else None),
                driver_name=repair.driver.full_name if repair.driver else None,
                plate_number=plate,
                total_amount=repair.total_amount or Decimal('0'),
                status=repair.status.value if repair.status else "Unknown",
                payment_method=None,  # Repairs don't have payment method
                receipt_url=receipt_url
            )
            result.append(item)
        
        return result
    
    def _sort_payments(
        self, 
        payments: List[UnifiedPaymentItem], 
        sort_by: str, 
        sort_order: str
    ) -> List[UnifiedPaymentItem]:
        """Sort the unified payment list"""
        
        # Define sorting key function
        def get_sort_key(item: UnifiedPaymentItem):
            if sort_by == 'payment_date':
                return item.payment_date or date.min
            elif sort_by == 'receipt_number':
                return item.receipt_number or ''
            elif sort_by == 'total_amount':
                return item.total_amount or Decimal('0')
            elif sort_by == 'driver_name':
                return item.driver_name or ''
            elif sort_by == 'medallion_number':
                return item.medallion_number or ''
            elif sort_by == 'status':
                return item.status or ''
            elif sort_by == 'receipt_type':
                return item.receipt_type or ''
            else:
                return item.payment_date or date.min  # Default to payment_date
        
        reverse = (sort_order.lower() == 'desc')
        return sorted(payments, key=get_sort_key, reverse=reverse)