# app/loans/pdf_service.py

import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List
from io import BytesIO

from jinja2 import Environment, FileSystemLoader
from sqlalchemy.orm import Session

# Try importing WeasyPrint for PDF generation
try:
    from weasyprint import HTML
except ImportError:
    HTML = None

from app.loans.models import DriverLoan
from app.loans.repository import LoanRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class LoanPdfService:
    """
    Service responsible for generating Driver Loan Receipt PDFs.
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = LoanRepository(db)
        # Setup Jinja2 environment
        self.template_dir = os.path.join(os.path.dirname(__file__), "templates")
        self.env = Environment(loader=FileSystemLoader(self.template_dir))

    # --- Formatting Helpers ---

    def _format_currency(self, value: Any) -> str:
        """Format currency value to $X,XXX.XX format"""
        if value is None:
            return "$0.00"
        try:
            return f"${float(value):,.2f}"
        except (ValueError, TypeError):
            return "$0.00"

    def _format_date(self, date_obj: Any) -> str:
        """Format date to MM/DD/YYYY format"""
        if not date_obj:
            return "-"
        if isinstance(date_obj, str):
            return date_obj
        return date_obj.strftime("%m/%d/%Y")

    def _format_date_receipt_header(self, date_obj: Any) -> str:
        """Format date for receipt header (e.g., December-09-2025)"""
        if not date_obj:
            return "-"
        if isinstance(date_obj, str):
            # Try to parse if it's a string
            try:
                date_obj = datetime.fromisoformat(date_obj.replace('Z', '+00:00'))
            except:
                return date_obj
        return date_obj.strftime("%B-%d-%Y")

    def _format_percentage(self, value: Any) -> str:
        """Format percentage value"""
        if value is None or value == 0:
            return "0%"
        try:
            return f"{float(value):.0f}%"
        except (ValueError, TypeError):
            return "0%"

    # --- Main Receipt Generation ---

    def generate_receipt_pdf(self, loan_id: int) -> bytes:
        """
        Generates the Driver Loan Receipt PDF for the given loan ID.
        
        Args:
            loan_id: The primary key ID of the driver loan
            
        Returns:
            bytes: PDF content
        """
        try:
            # Fetch the loan with relationships
            loan = self.repo.get_loan_by_id(loan_id)
            
            if not loan:
                raise ValueError(f"Driver loan not found with ID {loan_id}")
            
            # Prepare context data for template
            context = self._prepare_receipt_context(loan)
            
            # Render HTML template
            template = self.env.get_template("driver_loan_receipt.html")
            html_content = template.render(context)
            
            # Generate PDF
            if HTML:
                pdf_file = BytesIO()
                HTML(string=html_content).write_pdf(pdf_file)
                pdf_file.seek(0)
                return pdf_file.read()
            else:
                logger.warning("WeasyPrint not found. Returning HTML instead of PDF.")
                return html_content.encode('utf-8')
                
        except ValueError as e:
            logger.error(f"Error generating loan receipt PDF: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error generating loan receipt PDF: {str(e)}", exc_info=True)
            raise

    def _prepare_receipt_context(self, loan: DriverLoan) -> Dict[str, Any]:
        """
        Prepares the context dictionary for the receipt template.
        """
        # Get driver information
        driver = loan.driver
        driver_name = driver.full_name if driver else "N/A"
        tlc_license = (driver.tlc_license.tlc_license_number 
                      if driver and driver.tlc_license else "N/A")
        
        # Get medallion information
        medallion_number = (loan.medallion.medallion_number 
                           if loan.medallion else "N/A")
        
        # Calculate total to repay (principal + all interest)
        total_interest = sum(
            float(inst.interest_amount) 
            for inst in loan.installments
        ) if loan.installments else 0
        total_to_repay = float(loan.principal_amount) + total_interest
        
        # Format installments for display
        installments_list = []
        if loan.installments:
            for inst in loan.installments:
                installments_list.append({
                    'installment_id': inst.installment_id,
                    'payment_date': self._format_date(inst.week_end_date),
                    'principal': self._format_currency(inst.principal_amount),
                    'interest': self._format_currency(inst.interest_amount),
                    'total_due': self._format_currency(inst.total_due)
                })
        
        # Build context
        context = {
            'medallion': medallion_number,
            'driver_name': driver_name,
            'tlc_license': tlc_license,
            'receipt_number': loan.loan_id or "BAT system generated",
            'receipt_date': self._format_date_receipt_header(loan.loan_date),
            'cashier': "BAT User",
            'loan_amount': self._format_currency(loan.principal_amount),
            'loan_date': self._format_date(loan.start_week),
            'interest_rate': self._format_percentage(loan.interest_rate),
            'total_to_repay': self._format_currency(total_to_repay),
            'installments': installments_list,
            'generation_date': datetime.now().strftime("%B %d, %Y at %I:%M %p")
        }
        
        return context