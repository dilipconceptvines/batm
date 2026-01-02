# app/dtr/email_service.py

"""
DTR Email Service

Handles sending DTR PDFs with violation reports (PVB, TLC) to drivers via email.
Supports both weekly automated delivery and on-demand sending.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.drivers.models import Driver
from app.dtr.models import DTR
from app.dtr.pdf_service import DTRPdfService
from app.pvb.models import PVBViolation, PVBViolationStatus
from app.tlc.models import TLCViolation, TLCViolationStatus
from app.utils.email_service import send_templated_email
from app.utils.exporter_utils import ExporterFactory
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DTREmailService:
    """Service for sending DTR emails with attachments"""

    def __init__(self, db: Session):
        self.db = db
        self.pdf_service = DTRPdfService(db)

    async def send_weekly_dtr_email(
        self, dtr_id: int, include_violations: bool = True
    ) -> Dict[str, Any]:
        """
        Send weekly DTR email to primary driver with attachments

        Args:
            dtr_id: DTR ID to send
            include_violations: Whether to include violation reports

        Returns:
            Dictionary with send status and details
        """
        try:
            # Get DTR with relations
            dtr = self.db.query(DTR).filter(DTR.id == dtr_id).first()

            if not dtr:
                return {"success": False, "error": f"DTR {dtr_id} not found"}

            # Get driver
            driver = (
                self.db.query(Driver).filter(Driver.id == dtr.primary_driver_id).first()
            )
            if not driver or not driver.email_address:
                logger.warning(
                    f"DTR {dtr_id}: Driver {dtr.primary_driver_id} has no email address"
                )
                return {"success": False, "error": f"Driver has no email address"}

            # Prepare email context
            driver_name = (
                f"{driver.first_name} {driver.last_name}"
                if driver.first_name and driver.last_name
                else "Driver"
            )
            lease_id = dtr.lease.lease_id if dtr.lease else "N/A"
            medallion_number = (
                dtr.medallion.medallion_number if dtr.medallion else "N/A"
            )
            week_start = dtr.week_start_date.strftime("%m/%d/%Y")
            week_end = dtr.week_end_date.strftime("%m/%d/%Y")

            # Email subject
            subject = f"Your Weekly Driver Trip Receipt (DTR) - Lease ID: {lease_id}, Medallion: {medallion_number}, Period: {week_start} to {week_end}"

            # Prepare attachments
            attachments = []

            # 1. DTR PDF
            dtr_pdf = self._generate_dtr_pdf_attachment(dtr)
            if dtr_pdf:
                attachments.append(dtr_pdf)

            # 2. Violation Reports
            if include_violations:
                violation_attachments = await self._generate_violation_attachments(dtr)
                attachments.extend(violation_attachments)

            # Render email template
            context = {
                "driver_name": driver_name,
                "week_start_date": week_start,
                "week_end_date": week_end,
                "lease_id": lease_id,
                "medallion_number": medallion_number,
                "has_pvb_violations": any(a.get("type") == "pvb" for a in attachments),
                "has_tlc_violations": any(a.get("type") == "tlc" for a in attachments),
            }

            # Send Email
            await send_templated_email(
                to_emails=[driver.email_address],
                subject=subject,
                template_name="weekly_dtr.html",
                context=context,
                attachments=attachments,
            )

            logger.info(
                "Weekly DTR email sent successfully",
                dtr_id=dtr_id,
                driver_id=driver.id,
                email=driver.email_address,
                attachments_count=len(attachments),
            )

            return {
                "success": True,
                "dtr_id": dtr_id,
                "driver_email": driver.email_address,
                "attachments_sent": len(attachments),
            }
        except Exception as e:
            logger.error(
                f"Error sending weekly DTR email for DTR {dtr_id}: {str(e)}",
                exc_info=True,
            )
            return {"success": False, "dtr_id": dtr_id, "error": str(e)}

    async def send_on_demand_dtr_email(
        self,
        dtr_id: int,
        recipient_email: Optional[str] = None,
        include_violations: bool = True,
    ) -> Dict[str, Any]:
        """
        Send on-demand DTR email (can override recipient).

        Args:
            dtr_id: DTR ID to send
            recipient_email: Optional override email (defaults to driver's email)
            include_violations: Whether to include violation reports

        Returns:
            Dictionary with send status and details
        """
        try:
            # Get DTR with relations
            dtr = self.db.query(DTR).filter(DTR.id == dtr_id).first()

            if not dtr:
                return {"success": False, "error": f"DTR {dtr_id} not found"}

            # Get driver
            driver = (
                self.db.query(Driver).filter(Driver.id == dtr.primary_driver_id).first()
            )
            if not driver:
                return {"success": False, "error": f"Driver not found"}

            # Determine recipient
            to_email = recipient_email or driver.email_address
            if not to_email:
                logger.warning(f"DTR {dtr_id}: No email address provided")
                return {"success": False, "error": "No email address provided"}

            # Prepare email context
            driver_name = (
                f"{driver.first_name} {driver.last_name}"
                if driver.first_name and driver.last_name
                else "Driver"
            )
            lease_id = dtr.lease.lease_id if dtr.lease else "N/A"
            medallion_number = (
                dtr.medallion.medallion_number if dtr.medallion else "N/A"
            )
            week_start = dtr.week_start_date.strftime("%m/%d/%Y")
            week_end = dtr.week_end_date.strftime("%m/%d/%Y")
            receipt_type = "DTR"
            receipt_number = dtr.receipt_number

            # Email Subject
            subject = f"Requested Receipt - Lease ID: {lease_id}, Medallion: {medallion_number}, Period: {week_start} to {week_end}, Receipt Type: {receipt_type}, Receipt Number: {receipt_number}"

            # Prepare attachments
            attachments = []

            # 1. DTR PDF
            dtr_pdf = self._generate_dtr_pdf_attachment(dtr)
            if dtr_pdf:
                attachments.append(dtr_pdf)

            # 2. Violation Reports (if applicable)
            if include_violations:
                violation_attachments = await self._generate_violation_attachments(dtr)
                attachments.extend(violation_attachments)

            # Render email template
            context = {
                "driver_name": driver_name,
                "lease_id": lease_id,
                "medallion_number": medallion_number,
                "period": f"{week_start} to {week_end}",
                "receipt_type": receipt_type,
                "receipt_number": receipt_number,
                "has_pvb_violations": any(a.get("type") == "pvb" for a in attachments),
                "has_tlc_violations": any(a.get("type") == "tlc" for a in attachments),
            }

            # Send email
            await send_templated_email(
                to_emails=[to_email],
                subject=subject,
                template_name="on_demand_dtr.html",
                context=context,
                attachments=attachments,
            )

            logger.info(
                "On-demand DTR email sent successfully",
                dtr_id=dtr_id,
                driver_id=driver.id,
                email=to_email,
                attachments_count=len(attachments),
            )

            return {
                "success": True,
                "dtr_id": dtr_id,
                "recipient_email": to_email,
                "attachments_sent": len(attachments),
            }

        except Exception as e:
            logger.error(
                f"Error sending on-demand DTR email for DTR {dtr_id}: {str(e)}",
                exc_info=True,
            )
            return {"success": False, "dtr_id": dtr_id, "error": str(e)}

    def _generate_dtr_pdf_attachment(self, dtr: DTR) -> Optional[Dict[str, Any]]:
        """
        Genrate DTR PDF attachment

        Returns:
            Dictionary with filename and data, or None if generation fails
        """
        try:
            pdf_content = self.pdf_service.generate_dtr_pdf(dtr.id)

            return {
                "filename": f"DTR_{dtr.receipt_number}_{dtr.week_start_date.strftime('%Y%m%d')}.pdf",
                "data": pdf_content,
                "type": "dtr",
            }
        except Exception as e:
            logger.error(
                f"Error generating DTR PDF attachment: {str(e)}", exc_info=True
            )
            return None

    async def _generate_violation_attachments(self, dtr: DTR) -> List[Dict[str, Any]]:
        """
        Generate PVB and TLC violation report attachments for the DTR period.

        Returns:
            List of attachment dictionaries
        """
        attachments = []

        # 1. PVB Violations Report
        try:
            pvb_violations = self._get_pvb_violations_for_dtr(dtr)
            if pvb_violations:
                pvb_pdf = self._generate_pvb_report_pdf(pvb_violations, dtr)
                if pvb_pdf:
                    attachments.append(
                        {
                            "filename": f"PVB_Violations_{dtr.week_start_date.strftime('%Y%m%d')}-{dtr.week_end_date.strftime('%Y%m%d')}.pdf",
                            "data": pvb_pdf,
                            "type": "pvb",
                        }
                    )
        except Exception as e:
            logger.error(
                f"Error generating PVB violations report: {str(e)}", exc_info=True
            )

        # 2. TLC Violations Report
        try:
            tlc_violations = self._get_tlc_violations_for_dtr(dtr)
            if tlc_violations:
                tlc_pdf = self._generate_tlc_report_pdf(tlc_violations, dtr)
                if tlc_pdf:
                    attachments.append(
                        {
                            "filename": f"TLC_Violations_{dtr.week_start_date.strftime('%Y%m%d')}-{dtr.week_end_date.strftime('%Y%m%d')}.pdf",
                            "data": tlc_pdf,
                            "type": "tlc",
                        }
                    )
        except Exception as e:
            logger.error(
                f"Error generating TLC violations report: {str(e)}", exc_info=True
            )

        return attachments

    def _get_pvb_violations_for_dtr(self, dtr: DTR) -> List[PVBViolation]:
        """Get all PVB violations associated with the DTR period"""
        driver_ids = [dtr.primary_driver_id]
        if dtr.additional_driver_ids:
            driver_ids.extend(dtr.additional_driver_ids)

        violations = (
            self.db.query(PVBViolation)
            .filter(
                or_(
                    PVBViolation.vehicle_id == dtr.vehicle_id,
                    PVBViolation.driver_id.in_(driver_ids),
                ),
                PVBViolation.issue_date >= dtr.week_start_date,
                PVBViolation.issue_date <= dtr.week_end_date,
                PVBViolation.status == PVBViolationStatus.POSTED_TO_LEDGER,
            )
            .all()
        )

        return violations

    def _get_tlc_violations_for_dtr(self, dtr: DTR) -> List[TLCViolation]:
        """Get all TLC violations associated with the DTR period"""
        driver_ids = [dtr.primary_driver_id]
        if dtr.additional_driver_ids:
            driver_ids.extend(dtr.additional_driver_ids)

        violations = (
            self.db.query(TLCViolation)
            .filter(
                or_(
                    TLCViolation.medallion_id == dtr.medallion_id,
                    TLCViolation.driver_id.in_(driver_ids),
                ),
                TLCViolation.issue_date >= dtr.week_start_date,
                TLCViolation.issue_date <= dtr.week_end_date,
                TLCViolation.status == TLCViolationStatus.POSTED,
            )
            .all()
        )

        return violations

    def _generate_pvb_report_pdf(
        self, violations: List[PVBViolation], dtr: DTR
    ) -> Optional[bytes]:
        """Generate PDF report for PVB violations"""
        try:
            # Prepare data for export
            export_data = []
            for v in violations:
                export_data.append(
                    {
                        "Summons": v.summons or "",
                        "Plate": v.plate or "",
                        "State": v.state or "",
                        "Type": v.type or "",
                        "Issue Date": v.issue_date.strftime("%Y-%m-%d")
                        if v.issue_date
                        else "",
                        "Issue Time": v.issue_time.strftime("%H:%M:%S")
                        if v.issue_time
                        else "",
                        "Violation": v.violation_code or "",
                        "Location": v.street_name or "",
                        "Fine": float(v.fine or 0),
                        "Penalty": float(v.penalty or 0),
                        "Interest": float(v.interest or 0),
                        "Amount Due": float(v.amount_due or 0),
                        "Status": v.status.value if v.status else "",
                    }
                )

            # Use ExporterFactory to generate PDF
            exporter = ExporterFactory.get_exporter("pdf", export_data)
            buffer = exporter.export()
            return buffer.read()

        except Exception as e:
            logger.error(f"Error generating PVB PDF report: {str(e)}", exc_info=True)
            return None

    def _generate_tlc_report_pdf(
        self, violations: List[TLCViolation], dtr: DTR
    ) -> Optional[bytes]:
        """Generate PDF report for TLC violations"""
        try:
            # Prepare data for export
            export_data = []
            for v in violations:
                export_data.append(
                    {
                        "Summons No": v.summons_no or "",
                        "Violation Type": v.violation_type.value
                        if v.violation_type
                        else "",
                        "Description": v.description or "",
                        "Issue Date": v.issue_date.strftime("%Y-%m-%d")
                        if v.issue_date
                        else "",
                        "Issue Time": v.issue_time.strftime("%H:%M:%S")
                        if v.issue_time
                        else "",
                        "Due Date": v.due_date.strftime("%Y-%m-%d")
                        if v.due_date
                        else "",
                        "Amount": float(v.amount or 0),
                        "Penalty": float(v.penalty_amount or 0),
                        "Service Fee": float(v.service_fee or 0),
                        "Total Payable": float(v.total_payable or 0),
                        "Driver Payable": float(v.driver_payable or 0),
                        "Disposition": v.disposition or "",
                        "Status": v.status.value if v.status else "",
                    }
                )

            # Use ExporterFactory to generate PDF
            exporter = ExporterFactory.get_exporter("pdf", export_data)
            buffer = exporter.export()
            return buffer.read()

        except Exception as e:
            logger.error(f"Error generating TLC PDF report: {str(e)}", exc_info=True)
            return None


# Create singleton instance
def get_dtr_email_service(db: Session) -> DTREmailService:
    """Factory function to get DTR email service instance"""
    return DTREmailService(db)
