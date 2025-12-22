# app/dtr/pdf_service.py

import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List
from io import BytesIO

from jinja2 import Environment, FileSystemLoader
from sqlalchemy import func, or_, case
from sqlalchemy.orm import Session

# Try importing WeasyPrint for PDF generation
try:
    from weasyprint import HTML
except ImportError:
    HTML = None

from app.dtr.models import DTR
from app.dtr.repository import DTRRepository
from app.curb.models import CurbTrip, PaymentType
from app.ezpass.models import EZPassTransaction, EZPassTransactionStatus
from app.pvb.models import PVBViolation, PVBViolationStatus
from app.tlc.models import TLCViolation, TLCViolationStatus
from app.drivers.models import Driver
from app.repairs.models import RepairInstallment, RepairInstallmentStatus, RepairInvoice
from app.loans.models import LoanInstallment, LoanInstallmentStatus, DriverLoan
from app.misc_expenses.models import MiscellaneousExpense
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DTRPdfService:
    """
    Service responsible for fetching data, formatting it, and rendering the 
    Driver Transaction Receipt (DTR) PDF.
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = DTRRepository(db)
        # Setup Jinja2 environment
        self.template_dir = os.path.join(os.path.dirname(__file__), "templates")
        self.env = Environment(loader=FileSystemLoader(self.template_dir))

    # --- Formatting Helpers ---

    def _format_currency(self, value: Any) -> str:
        if value is None:
            return "$ 0.00"
        try:
            return f"$ {float(value):,.2f}"
        except (ValueError, TypeError):
            return "$ 0.00"

    def _format_date_short(self, date_obj: Any) -> str:
        if not date_obj:
            return "-"
        if isinstance(date_obj, str):
            return date_obj  # Already a string
        return date_obj.strftime("%m/%d/%Y")

    def _format_datetime_short(self, dt_obj: Any) -> str:
        if not dt_obj:
            return "-"
        if isinstance(dt_obj, str):
            return dt_obj
        return dt_obj.strftime("%m/%d/%Y | %I:%M %p")

    def _format_date_long(self, date_obj: Any) -> str:
        if not date_obj:
            return "-"
        if isinstance(date_obj, str):
            return date_obj
        return date_obj.strftime("%m-%d-%Y")

    # --- Data Fetching Methods ---

    def _get_lease_charges(self, dtr: DTR) -> List[Dict[str, Any]]:
        """
        Prepares data for the Lease Charges section.
        """
        amount = dtr.lease_amount
        prior = dtr.prior_balance
        total_obligation = amount + prior

        # In this logic, if there are net earnings, we assume the lease charge 
        # was deducted (paid). If net earnings are negative, it carries forward.
        # For the receipt display, we show the obligation amount as 'Amount Paid' 
        # if the DTR generated successfully, implying the deduction happened.
        paid = amount 
        balance = Decimal("0.00")

        return [{
            "lease_id": dtr.lease.lease_id if dtr.lease else "N/A",
            "amount": self._format_currency(amount),
            "prior_balance": self._format_currency(prior),
            "paid": self._format_currency(paid),
            "balance": self._format_currency(balance)
        }]

    def _get_earnings(self, dtr: DTR, driver_ids: List[int]) -> Decimal:
        """
        Calculate Credit Card Earnings for specific drivers within the DTR period.
        """
        start_dt = datetime.combine(dtr.week_start_date, datetime.min.time())
        end_dt = datetime.combine(dtr.week_end_date, datetime.max.time())

        total = self.db.query(func.sum(CurbTrip.total_amount)).filter(
            CurbTrip.driver_id.in_(driver_ids),
            CurbTrip.start_time >= start_dt,
            CurbTrip.end_time <= end_dt,
            CurbTrip.payment_type == PaymentType.CREDIT_CARD
        ).scalar() or Decimal("0.00")
        
        return Decimal(total)

    def _get_tax_breakdown(self, dtr: DTR, driver_ids: List[int]) -> Dict[str, Any]:
        """
        Aggregates tax components (MTA, TIF, etc.) from CURB trips for specific drivers.
        """
        start_dt = datetime.combine(dtr.week_start_date, datetime.min.time())
        end_dt = datetime.combine(dtr.week_end_date, datetime.max.time())

        def get_tax_stats(tax_column):
            return self.db.query(
                func.sum(tax_column),
                func.count(CurbTrip.id),
                func.sum(case((CurbTrip.payment_type == PaymentType.CASH, 1), else_=0)),
                func.sum(case((CurbTrip.payment_type == PaymentType.CREDIT_CARD, 1), else_=0))
            ).filter(
                CurbTrip.driver_id.in_(driver_ids),
                CurbTrip.start_time >= start_dt,
                CurbTrip.end_time <= end_dt,
                tax_column > 0
            ).first()

        # Mapping UI labels to Model columns
        tax_map = [
            ("Airport Access Fee", CurbTrip.airport_fee),
            ("CBDT", CurbTrip.cbdt_fee),
            ("Congestion Tax - CPS", CurbTrip.congestion_fee),
            ("MTA Tax (TLC Rule 58-21 (l)(14))", CurbTrip.surcharge),
            ("Improvement Tax - TIF (TLC Rule 54-17(k))", CurbTrip.improvement_surcharge)
        ]

        rows = []
        grand_total_amount = Decimal("0.00")
        grand_total_trips = 0
        grand_cash_trips = 0
        grand_cc_trips = 0

        for label, col in tax_map:
            res = get_tax_stats(col)
            amount = res[0] or Decimal("0.00")
            count = res[1] or 0
            cash = res[2] or 0
            cc = res[3] or 0

            rows.append({
                "type": label,
                "amount": self._format_currency(amount),
                "total_trips": count,
                "cash_trips": cash,
                "cc_trips": cc
            })

            grand_total_amount += amount
            grand_total_trips += count
            grand_cash_trips += cash
            grand_cc_trips += cc

        return {
            "rows": rows,
            "total_amount_val": grand_total_amount,
            "total_amount": self._format_currency(grand_total_amount),
            "total_trips": grand_total_trips,
            "total_cash": grand_cash_trips,
            "total_cc": grand_cc_trips
        }

    def _get_ezpass_details(self, dtr: DTR, driver_ids: List[int]) -> Dict[str, Any]:
        """
        Fetches posted EZPass transactions for the specific drivers/medallion
        associated with the DTR.
        """
        # Note: DTR generation logic usually sets a cutoff. Here we replicate that logic
        # by finding posted items up to the week end date.
        end_dt = datetime.combine(dtr.week_end_date, datetime.max.time())

        txns = self.db.query(EZPassTransaction).filter(
            or_(
                EZPassTransaction.medallion_id == dtr.medallion_id,
                EZPassTransaction.driver_id.in_(driver_ids)
            ),
            EZPassTransaction.transaction_datetime <= end_dt,
            EZPassTransaction.status == EZPassTransactionStatus.POSTED_TO_LEDGER
        ).order_by(EZPassTransaction.transaction_datetime.desc()).all()

        rows = []
        total = Decimal("0.00")
        for t in txns:
            rows.append({
                "date": self._format_datetime_short(t.transaction_datetime),
                "license": t.driver.tlc_license.tlc_license_number if t.driver and t.driver.tlc_license else "-",
                "plate": t.tag_or_plate,
                "agency": t.agency,
                "entry": t.entry_plaza or "-",
                "exit": t.exit_plaza or "-",
                "toll": self._format_currency(t.amount),
                "payment": self._format_currency(t.amount),
                "balance": "-"
            })
            total += t.amount

        return {
            "rows": rows,
            "total_val": total,
            "total": self._format_currency(total)
        }

    def _get_pvb_details(self, dtr: DTR, driver_ids: List[int]) -> Dict[str, Any]:
        """
        Fetches posted PVB violations for the specific drivers/vehicle.
        """
        violations = self.db.query(PVBViolation).filter(
            or_(
                PVBViolation.vehicle_id == dtr.vehicle_id,
                PVBViolation.driver_id.in_(driver_ids)
            ),
            PVBViolation.issue_date <= dtr.week_end_date,
            PVBViolation.status == PVBViolationStatus.POSTED_TO_LEDGER
        ).all()

        rows = []
        total = Decimal("0.00")
        for v in violations:
            # Calculate components
            charge = (v.penalty or 0) + (v.interest or 0) - (v.reduction or 0)
            total_row = v.amount_due

            rows.append({
                "date": self._format_datetime_short(datetime.combine(v.issue_date, v.issue_time)) if v.issue_time else self._format_date_short(v.issue_date),
                "ticket": v.summons,
                "license": v.driver.tlc_license.tlc_license_number if v.driver and v.driver.tlc_license else "-",
                "note": f"{v.type} - {v.state}",
                "fine": self._format_currency(v.fine),
                "charge": self._format_currency(charge),
                "total": self._format_currency(total_row),
                "payment": self._format_currency(total_row),
                "balance": "-"
            })
            total += total_row

        return {
            "rows": rows,
            "total_val": total,
            "total": self._format_currency(total)
        }

    def _get_tlc_details(self, dtr: DTR) -> Dict[str, Any]:
        """
        Fetches TLC Violations. Typically lease/medallion level, but can be driver specific.
        """
        # Include primary and additional drivers
        driver_ids = [dtr.primary_driver_id]
        if dtr.additional_driver_ids:
            driver_ids.extend(dtr.additional_driver_ids)

        violations = self.db.query(TLCViolation).filter(
            or_(
                TLCViolation.medallion_id == dtr.medallion_id,
                TLCViolation.driver_id.in_(driver_ids)
            ),
            TLCViolation.issue_date <= dtr.week_end_date,
            TLCViolation.status == TLCViolationStatus.POSTED
        ).all()

        rows = []
        total = Decimal("0.00")
        for v in violations:
            rows.append({
                "date": self._format_datetime_short(datetime.combine(v.issue_date, v.issue_time)) if v.issue_time else self._format_date_short(v.issue_date),
                "ticket": v.summons_no,
                "license": v.driver.tlc_license.tlc_license_number if v.driver and v.driver.tlc_license else "-",
                "medallion": v.medallion.medallion_number if v.medallion else "-",
                "note": v.description or v.violation_type.value,
                "fine": self._format_currency(v.amount),
                "payment": self._format_currency(v.total_payable),
                "balance": "-"
            })
            total += v.total_payable

        return {
            "rows": rows,
            "total": self._format_currency(total)
        }

    def _get_trip_log(self, dtr: DTR, driver_ids: List[int]) -> List[List[Dict[str, Any]]]:
        """
        Fetches credit card trip logs for specific drivers and formats them into 3 columns
        to match the Figma design.
        """
        start_dt = datetime.combine(dtr.week_start_date, datetime.min.time())
        end_dt = datetime.combine(dtr.week_end_date, datetime.max.time())

        trips = self.db.query(CurbTrip).filter(
            CurbTrip.driver_id.in_(driver_ids),
            CurbTrip.start_time >= start_dt,
            CurbTrip.end_time <= end_dt,
            CurbTrip.payment_type == PaymentType.CREDIT_CARD
        ).order_by(CurbTrip.start_time.asc()).all()

        trip_rows = []
        for t in trips:
            trip_rows.append({
                "date": self._format_date_short(t.start_time),
                "license": t.driver.tlc_license.tlc_license_number if t.driver and t.driver.tlc_license else "-",
                "trip_no": t.curb_trip_id.split('-')[-1] if '-' in t.curb_trip_id else t.curb_trip_id,
                "amount": self._format_currency(t.total_amount)
            })

        # Distribute into 3 columns
        total_trips = len(trip_rows)
        # Calculate split points
        col_size = (total_trips // 3) + (1 if total_trips % 3 > 0 else 0)
        
        col1 = trip_rows[:col_size]
        col2 = trip_rows[col_size:col_size*2]
        col3 = trip_rows[col_size*2:]

        return [col1, col2, col3]

    def _get_repairs(self, dtr: DTR) -> Dict[str, Any]:
        """
        Fetches repair installments posted in this DTR period.
        """
        driver_ids = [dtr.primary_driver_id]
        if dtr.additional_driver_ids:
            driver_ids.extend(dtr.additional_driver_ids)

        # Find installments due in this week range that are POSTED or PAID
        installments = self.db.query(RepairInstallment).filter(
            RepairInstallment.invoice.has(or_(
                RepairInvoice.driver_id.in_(driver_ids),
                RepairInvoice.lease_id == dtr.lease_id
            )),
            RepairInstallment.week_start_date >= dtr.week_start_date,
            RepairInstallment.week_start_date <= dtr.week_end_date,
            RepairInstallment.status.in_([RepairInstallmentStatus.POSTED, RepairInstallmentStatus.PAID])
        ).all()

        rows = []
        total_paid = Decimal("0.00")

        for inst in installments:
            inv = inst.invoice
            # Calculate paid till date logic (sum of all posted installments for this invoice)
            paid_so_far = self.db.query(func.sum(RepairInstallment.principal_amount)).filter(
                RepairInstallment.invoice_id == inv.id,
                RepairInstallment.status.in_([RepairInstallmentStatus.POSTED, RepairInstallmentStatus.PAID]),
                RepairInstallment.week_start_date <= inst.week_start_date
            ).scalar() or Decimal("0.00")

            balance = inv.total_amount - paid_so_far

            rows.append({
                "id": inst.installment_id,
                "due_date": self._format_date_short(inst.week_end_date),
                "due": self._format_currency(inst.principal_amount),
                "payable": self._format_currency(inst.principal_amount),
                "payment": self._format_currency(inst.principal_amount),
                "balance": self._format_currency(balance),
                # Invoice context
                "repair_id": inv.repair_id,
                "invoice_no": inv.invoice_number,
                "invoice_date": self._format_date_short(inv.invoice_date),
                "workshop": inv.workshop_type.value,
                "inv_amount": self._format_currency(inv.total_amount),
                "paid_till_date": self._format_currency(paid_so_far),
                "outstanding": self._format_currency(balance)
            })
            total_paid += inst.principal_amount

        return {
            "rows": rows,
            "total": self._format_currency(total_paid)
        }

    def _get_loans(self, dtr: DTR) -> Dict[str, Any]:
        """
        Fetches loan installments posted in this DTR period.
        """
        driver_ids = [dtr.primary_driver_id]
        if dtr.additional_driver_ids:
            driver_ids.extend(dtr.additional_driver_ids)

        installments = self.db.query(LoanInstallment).filter(
            LoanInstallment.loan.has(DriverLoan.driver_id.in_(driver_ids)),
            LoanInstallment.week_start_date >= dtr.week_start_date,
            LoanInstallment.week_start_date <= dtr.week_end_date,
            LoanInstallment.status.in_([LoanInstallmentStatus.POSTED, LoanInstallmentStatus.PAID])
        ).all()

        rows = []
        total_paid = Decimal("0.00")

        for inst in installments:
            loan = inst.loan
            # Calculate paid till date
            paid_so_far = self.db.query(func.sum(LoanInstallment.total_due)).filter(
                LoanInstallment.loan_id == loan.id,
                LoanInstallment.status.in_([LoanInstallmentStatus.POSTED, LoanInstallmentStatus.PAID]),
                LoanInstallment.week_start_date <= inst.week_start_date
            ).scalar() or Decimal("0.00")

            # Approximating remaining balance logic based on principal + interest
            # In a complex system, this would query the LedgerBalance directly.
            total_loan_obligation = loan.principal_amount * (1 + (loan.interest_rate / 100))
            balance = total_loan_obligation - paid_so_far

            rows.append({
                "id": inst.installment_id,
                "due_date": self._format_date_short(inst.week_end_date),
                "principal": self._format_currency(inst.principal_amount),
                "interest": self._format_currency(inst.interest_amount),
                "total_due": self._format_currency(inst.total_due),
                "payable": self._format_currency(inst.total_due),
                "payment": self._format_currency(inst.total_due),
                "balance": self._format_currency(balance),
                # Loan context
                "loan_id": loan.loan_id,
                "loan_date": self._format_date_short(loan.loan_date),
                "loan_amount": self._format_currency(loan.principal_amount),
                "rate": f"{loan.interest_rate}%",
                "loan_total": self._format_currency(loan.principal_amount), 
                "paid_till_date": self._format_currency(paid_so_far),
                "outstanding": self._format_currency(balance)
            })
            total_paid += inst.total_due

        return {
            "rows": rows,
            "total": self._format_currency(total_paid)
        }

    def _get_misc(self, dtr: DTR) -> Dict[str, Any]:
        """
        Fetches miscellaneous expenses for the period.
        """
        driver_ids = [dtr.primary_driver_id]
        if dtr.additional_driver_ids:
            driver_ids.extend(dtr.additional_driver_ids)

        expenses = self.db.query(MiscellaneousExpense).filter(
            or_(
                MiscellaneousExpense.driver_id.in_(driver_ids),
                MiscellaneousExpense.lease_id == dtr.lease_id
            ),
            MiscellaneousExpense.expense_date >= dtr.week_start_date,
            MiscellaneousExpense.expense_date <= dtr.week_end_date,
            # We include OPEN expenses that fall in the date range,
            # as they are deducted in the DTR.
        ).all()

        rows = []
        total = Decimal("0.00")
        for exp in expenses:
            rows.append({
                "type": exp.category,
                "invoice": exp.reference_number or "-",
                "amount": self._format_currency(exp.amount),
                "prior": "-", 
                "payment": self._format_currency(exp.amount),
                "balance": "-"
            })
            total += exp.amount

        return {
            "rows": rows,
            "total": self._format_currency(total)
        }

    def _get_alerts(self, dtr: DTR) -> Dict[str, Any]:
        """
        Fetches alerts for Vehicle, Primary Driver, and Additional Drivers.
        """
        vehicle = dtr.vehicle
        driver = dtr.primary_driver

        # Vehicle Alerts
        # Fetch active vehicle inspections
        # Note: In a real scenario, we'd query the VehicleInspection model for 'next_due_date'
        # Mocking here based on available data
        tlc_inspection = "-"
        mile_run = "-"
        dmv_registration = "-"
        
        if vehicle:
            # Assuming logic to find next inspection dates exists
            # Here we use a placeholder logic or query inspection table if available
            pass

        # Driver Alerts (Primary)
        tlc_expiry = "-"
        dmv_expiry = "-"
        if driver.tlc_license:
            tlc_expiry = self._format_date_long(driver.tlc_license.tlc_license_expiry_date)
        if driver.dmv_license:
            dmv_expiry = self._format_date_long(driver.dmv_license.dmv_license_expiry_date)

        # Additional Drivers
        additional_drivers = []
        if dtr.additional_driver_ids:
            drivers = self.db.query(Driver).filter(Driver.id.in_(dtr.additional_driver_ids)).all()
            for d in drivers:
                t_exp = "-"
                d_exp = "-"
                if d.tlc_license:
                    t_exp = self._format_date_long(d.tlc_license.tlc_license_expiry_date)
                if d.dmv_license:
                    d_exp = self._format_date_long(d.dmv_license.dmv_license_expiry_date)
                
                additional_drivers.append({
                    "name": f"{d.first_name} {d.last_name}",
                    "tlc_expiry": t_exp,
                    "dmv_expiry": d_exp
                })

        return {
            "vehicle": {
                "tlc_inspection": "December-12-2025", # Placeholder as per mock
                "mile_run": "October-7-2025",
                "dmv_registration": "December-4-2026"
            },
            "driver": {
                "tlc_expiry": tlc_expiry,
                "dmv_expiry": dmv_expiry
            },
            "additional_drivers": additional_drivers
        }

    def _get_driver_alerts(self, driver_id: int) -> Dict[str, str]:
        """Get specific driver alerts"""
        driver = self.db.query(Driver).filter(Driver.id == driver_id).first()
        tlc_expiry = "-"
        dmv_expiry = "-"
        if driver and driver.tlc_license:
            tlc_expiry = self._format_date_long(driver.tlc_license.tlc_license_expiry_date)
        if driver and driver.dmv_license:
            dmv_expiry = self._format_date_long(driver.dmv_license.dmv_license_expiry_date)
        
        return {
            "tlc_expiry": tlc_expiry,
            "dmv_expiry": dmv_expiry
        }

    def _get_additional_driver_pages(self, dtr: DTR) -> List[Dict[str, Any]]:
        """
        Generates the context for each additional driver page.
        """
        if not dtr.additional_driver_ids:
            return []
        
        pages = []
        additional_drivers = self.db.query(Driver).filter(Driver.id.in_(dtr.additional_driver_ids)).all()
        
        for driver in additional_drivers:
            driver_ids = [driver.id]
            
            # 1. Financials specific to this driver
            cc_earnings = self._get_earnings(dtr, driver_ids)
            taxes_data = self._get_tax_breakdown(dtr, driver_ids)
            ezpass_data = self._get_ezpass_details(dtr, driver_ids)
            pvb_data = self._get_pvb_details(dtr, driver_ids)
            trip_logs = self._get_trip_log(dtr, driver_ids)
            alerts = self._get_driver_alerts(driver.id)

            # 2. Calculate Net for this driver (Earnings - Deductions)
            subtotal = (
                taxes_data['total_amount_val'] +
                ezpass_data['total_val'] +
                pvb_data['total_val']
            )
            net_earnings = cc_earnings - subtotal

            # 3. Build Page Context
            page_data = {
                "driver_name": f"{driver.first_name} {driver.last_name}".upper(),
                "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
                
                # Account Balance
                "cc_earnings_fmt": self._format_currency(cc_earnings),
                "taxes_fmt": taxes_data['total_amount'],
                "ezpass_fmt": ezpass_data['total'],
                "pvb_fmt": pvb_data['total'],
                "subtotal_fmt": self._format_currency(subtotal),
                "net_earnings_fmt": self._format_currency(net_earnings),
                "total_due_fmt": self._format_currency(max(net_earnings, Decimal(0))),

                # Details
                "taxes_rows": taxes_data['rows'],
                "taxes_total_trips": taxes_data['total_trips'] or 0,
                "taxes_total_cash": taxes_data['total_cash'] or 0,
                "taxes_total_cc": taxes_data['total_cc'] or 0,
                "ezpass_rows": ezpass_data['rows'],
                "pvb_rows": pvb_data['rows'],
                "trip_log_cols": trip_logs,
                "alerts": alerts
            }
            pages.append(page_data)
        
        return pages

    def generate_dtr_pdf(self, dtr_id: int) -> bytes:
        """
        Generates the full DTR PDF including Summary, Details, and Additional Drivers.
        """
        dtr = self.repo.get_by_id(dtr_id)
        if not dtr:
            raise ValueError(f"DTR {dtr_id} not found")

        # --- 1. Primary Driver / Lease Context (Consolidated View) ---
        
        # Driver Info
        driver_name = "Unknown"
        tlc_license = "N/A"
        if dtr.primary_driver:
            driver_name = f"{dtr.primary_driver.first_name} {dtr.primary_driver.last_name}"
            if dtr.primary_driver.tlc_license:
                tlc_license = dtr.primary_driver.tlc_license.tlc_license_number

        # Payment Info
        payment_type = "Check"
        batch_no = "-"
        account_no = "-"
        if dtr.payment_method == "ACH":
            payment_type = "Direct Deposit"
            batch_no = dtr.ach_batch_number if dtr.ach_batch_number else "Pending"
            if dtr.primary_driver.driver_bank_account:
                full_acc = str(dtr.primary_driver.driver_bank_account.bank_account_number)
                account_no = f"xxxx{full_acc[-4:]}" if len(full_acc) > 4 else full_acc
        else:
            batch_no = dtr.check_number if dtr.check_number else "-"

        # Fetch Consolidated Data (Page 2 & 3)
        # We include primary driver ID + all additional driver IDs for consolidated totals
        all_driver_ids = [dtr.primary_driver_id]
        if dtr.additional_driver_ids:
            all_driver_ids.extend(dtr.additional_driver_ids)

        lease_charges = self._get_lease_charges(dtr)
        taxes_data = self._get_tax_breakdown(dtr, all_driver_ids)
        ezpass_data = self._get_ezpass_details(dtr, all_driver_ids)
        pvb_data = self._get_pvb_details(dtr, all_driver_ids)
        tlc_data = self._get_tlc_details(dtr)
        trip_logs = self._get_trip_log(dtr, all_driver_ids)
        repairs_data = self._get_repairs(dtr)
        loans_data = self._get_loans(dtr)
        misc_data = self._get_misc(dtr)
        alerts_data = self._get_alerts(dtr)

        # --- 2. Additional Driver Pages (Filtered View) ---
        additional_driver_pages = self._get_additional_driver_pages(dtr)

        # --- 3. Construct Context ---
        context = {
            # Header & Meta
            "medallion_number": dtr.medallion.medallion_number if dtr.medallion else "N/A",
            "receipt_number": dtr.receipt_number,
            "driver_name": driver_name.upper(),
            "receipt_date": dtr.generation_date.strftime("%B-%d-%Y"),
            "tlc_license": tlc_license,
            "period_start": dtr.week_start_date.strftime("%B-%d-%Y"),
            "period_end": dtr.week_end_date.strftime("%B-%d-%Y"),
            "today_full": datetime.now().strftime("%m/%d/%Y | %I:%M %p"),

            # Page 1: Financial Summary
            "cc_earnings_fmt": self._format_currency(dtr.credit_card_earnings),
            "lease_fee_fmt": self._format_currency(dtr.lease_amount),
            "taxes_fmt": self._format_currency(dtr.mta_fees_total),
            "ezpass_fmt": self._format_currency(dtr.ezpass_tolls),
            "pvb_fmt": self._format_currency(dtr.pvb_violations),
            "tlc_fmt": self._format_currency(dtr.tlc_tickets),
            "repairs_fmt": self._format_currency(dtr.repairs),
            "loans_fmt": self._format_currency(dtr.driver_loans),
            "misc_fmt": self._format_currency(dtr.misc_charges),
            "subtotal_fmt": self._format_currency(dtr.subtotal_deductions),
            "net_earnings_fmt": self._format_currency(dtr.net_earnings),
            "total_due_fmt": self._format_currency(dtr.total_due_to_driver),
            "payment_type": payment_type,
            "batch_number": batch_no,
            "account_number": account_no,

            # Page 2 & 3: Details
            "lease_charges": lease_charges,
            "taxes_rows": taxes_data['rows'],
            "taxes_total_amt": taxes_data['total_amount'],
            "taxes_total_trips": taxes_data['total_trips'],
            "taxes_total_cash": taxes_data['total_cash'],
            "taxes_total_cc": taxes_data['total_cc'],
            "ezpass_rows": ezpass_data['rows'],
            "ezpass_total": ezpass_data['total'],
            "pvb_rows": pvb_data['rows'],
            "pvb_total": pvb_data['total'],
            "tlc_rows": tlc_data['rows'],
            "tlc_total": tlc_data['total'],
            "trip_log_cols": trip_logs,
            "repairs_rows": repairs_data['rows'],
            "repairs_total": repairs_data['total'],
            "loans_rows": loans_data['rows'],
            "loans_total": loans_data['total'],
            "misc_rows": misc_data['rows'],
            "misc_total": misc_data['total'],
            "alerts": alerts_data,

            # Page 5 & 6: Additional Drivers
            "additional_driver_pages": additional_driver_pages
        }

        # --- 4. Render ---
        template = self.env.get_template("dtr_pdf.html")
        html_content = template.render(context)

        if HTML:
            pdf_file = BytesIO()
            HTML(string=html_content).write_pdf(pdf_file)
            pdf_file.seek(0)
            return pdf_file.read()
        else:
            logger.warning("WeasyPrint not found. Returning HTML instead of PDF.")
            return html_content.encode('utf-8')