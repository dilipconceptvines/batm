# app/repairs/pdf_service.py

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

from app.repairs.models import RepairInvoice
from app.repairs.repository import RepairRepository
from app.utils.logger import get_logger

logger = get_logger(__name__)


class RepairPdfService:
    """
    Service responsible for generating Vehicle Repair Receipt PDFs.
    """

    def __init__(self, db: Session):
        self.db = db
        self.repo = RepairRepository(db)
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

    # --- Main Receipt Generation ---

    def generate_receipt_pdf(self, invoice_id: int) -> bytes:
        """
        Generates the Vehicle Repair Receipt PDF for the given invoice ID.
        
        Args:
            invoice_id: The primary key ID of the repair invoice
            
        Returns:
            bytes: PDF content
        """
        try:
            # Fetch the invoice with relationships
            invoice = self.repo.get_invoice_by_id(invoice_id)
            
            if not invoice:
                raise ValueError(f"Repair invoice not found with ID {invoice_id}")
            
            # Prepare context data for template
            context = self._prepare_receipt_context(invoice)
            
            # Render HTML template
            template = self.env.get_template("vehicle_repair_receipt.html")
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
            logger.error(f"Error generating repair receipt PDF: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error generating repair receipt PDF: {str(e)}", exc_info=True)
            raise

    def _prepare_receipt_context(self, invoice: RepairInvoice) -> Dict[str, Any]:
        """
        Prepares the context dictionary for the receipt template.
        """
        # Get driver information
        driver = invoice.driver
        driver_name = driver.full_name if driver else "N/A"
        tlc_license = (driver.tlc_license.tlc_license_number 
                      if driver and driver.tlc_license else "N/A")
        
        # Get medallion information
        medallion_number = (invoice.medallion.medallion_number 
                           if invoice.medallion else "N/A")
        
        # Format installments for display with running balance
        installments_list = []
        if invoice.installments:
            total_amount = float(invoice.total_amount)
            remaining_balance = total_amount
            
            for inst in invoice.installments:
                installment_amount = float(inst.principal_amount)
                
                installments_list.append({
                    'installment_id': inst.installment_id,
                    'payment_date': self._format_date(inst.week_end_date),
                    'installment': self._format_currency(inst.principal_amount),
                    'balance': self._format_currency(remaining_balance)
                })
                
                remaining_balance -= installment_amount
        
        # Build context
        context = {
            'medallion': medallion_number,
            'driver_name': driver_name,
            'tlc_license': tlc_license,
            'receipt_number': invoice.repair_id or "BAT system generated",
            'receipt_date': self._format_date_receipt_header(invoice.invoice_date),
            'cashier': "BAT User",
            'repair_amount': self._format_currency(invoice.total_amount),
            'invoice_number': invoice.invoice_number,
            'installments': installments_list,
            'generation_date': datetime.now().strftime("%B %d, %Y at %I:%M %p")
        }
        
        return context