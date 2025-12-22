# app/interim_payments/pdf_service.py

import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict
from io import BytesIO

from jinja2 import Environment, FileSystemLoader
from sqlalchemy.orm import Session

# Try importing WeasyPrint for PDF generation
try:
    from weasyprint import HTML
except ImportError:
    HTML = None

from app.interim_payments.models import InterimPayment
from app.interim_payments.repository import InterimPaymentRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class InterimPaymentPdfService:
    """
    Service responsible for generating Interim Payment Receipt PDFs.
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = InterimPaymentRepository(db)
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
        """Format date for receipt header (e.g., August-10-2025)"""
        if not date_obj:
            return "-"
        if isinstance(date_obj, str):
            # Try to parse if it's a string
            try:
                date_obj = datetime.fromisoformat(date_obj.replace('Z', '+00:00'))
            except:
                return date_obj
        return date_obj.strftime("%B-%d-%Y")

    # --- Main Receipt Generation ---

    def generate_receipt_pdf(self, payment_id: int) -> bytes:
        """
        Generates the Interim Payment Receipt PDF for the given payment ID.
        
        Args:
            payment_id: The primary key ID of the interim payment
            
        Returns:
            bytes: PDF content
        """
        try:
            # Fetch the interim payment with relationships
            payment = self.repo.get_payment_by_id(payment_id)
            
            if not payment:
                raise ValueError(f"Interim payment not found with ID {payment_id}")
            
            # Prepare context data for template
            context = self._prepare_receipt_context(payment)
            
            # Render HTML template
            template = self.env.get_template("interim_payment_receipt.html")
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
            logger.error(f"Error generating receipt PDF: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error generating receipt PDF: {str(e)}", exc_info=True)
            raise

    def _prepare_receipt_context(self, payment: InterimPayment) -> Dict[str, Any]:
        """
        Prepares the context dictionary for the receipt template.
        """
        # Get driver information
        driver = payment.driver
        driver_name = driver.full_name if driver else "N/A"
        tlc_license = (driver.tlc_license.tlc_license_number 
                      if driver and driver.tlc_license else "N/A")
        
        # Get lease/medallion information
        lease = payment.lease
        medallion_number = (lease.medallion.medallion_number 
                           if lease and lease.medallion else "N/A")
        
        # Payment summary
        payment_summary = {
            'amount': self._format_currency(payment.total_amount),
            'type': payment.payment_method.value,
            'reference': payment.payment_id or "-",
            'date': self._format_date(payment.payment_date)
        }
        
        # Payment breakdown from allocations
        payment_breakdown = []
        if payment.allocations:
            for alloc in payment.allocations:
                # Map category to friendly name
                category_map = {
                    'LEASE': 'Lease Amount',
                    'REPAIRS': 'Vehicle Repair',
                    'LOANS': 'Driver Loans',
                    'EZPASS': 'EZPass Tolls',
                    'PVB': 'PVB Violations',
                    'TLC': 'TLC Tickets',
                    'MISC': 'Miscellaneous Payments',
                    'TAXES': 'Taxes'
                }
                
                payment_name = category_map.get(alloc.get('category', ''), 
                                               alloc.get('category', 'Other'))
                
                payment_breakdown.append({
                    'payment': payment_name,
                    'reference_id': alloc.get('reference_id', '-'),
                    'amount': self._format_currency(alloc.get('amount', 0))
                })
        
        # Calculate total
        total_amount = self._format_currency(payment.total_amount)
        
        # Build context
        context = {
            'medallion': medallion_number,
            'driver_name': driver_name,
            'tlc_license': tlc_license,
            'receipt_number': payment.payment_id or "BAT system generated",
            'receipt_date': self._format_date_receipt_header(payment.payment_date),
            'cashier': "BAT User",  # You can make this dynamic if you have user info
            'payment_summary': payment_summary,
            'payment_breakdown': payment_breakdown,
            'total': total_amount,
            'generation_date': datetime.now().strftime("%B %d, %Y at %I:%M %p")
        }
        
        return context