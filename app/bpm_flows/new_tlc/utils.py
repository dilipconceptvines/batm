from sqlalchemy.orm import Session
from app.tlc.models import TLCViolation
from app.uploads.services import upload_service


def format_tlc_violation(db: Session, tlc_violation: TLCViolation):
    """
    Format a TLCViolation SQLAlchemy model into a serializable dictionary.
    Handles relationship null-safety and optional fields gracefully.
    """

    if not tlc_violation:
        return {}

    # Safe helper: convert date/time/datetime to ISO format
    def iso(value):
        return value.isoformat() if value else None


    return {
        "id": tlc_violation.id,
        "case_no": tlc_violation.case_no,
        "summons_no": tlc_violation.summons_no,

        # Dates / times
        "issue_date": iso(tlc_violation.issue_date),
        "issue_time": iso(tlc_violation.issue_time),
        "due_date": iso(tlc_violation.due_date),

        # Details
        "violation_type": getattr(tlc_violation.violation_type, "value", None),
        "description": tlc_violation.description,

        # Amounts (safe float conversion)
        "amount": float(tlc_violation.amount) if tlc_violation.amount is not None else None,
        "service_fee": float(tlc_violation.service_fee) if tlc_violation.service_fee is not None else None,
        "total_payable": float(tlc_violation.total_payable) if tlc_violation.total_payable is not None else None,
        "driver_payable": float(tlc_violation.driver_payable) if tlc_violation.driver_payable is not None else None,

        "disposition": getattr(tlc_violation.disposition, "value", None),
        "note": tlc_violation.note,

        # Relationships (avoid attribute errors)
        "driver_id": getattr(tlc_violation.driver, "driver_id", None),
        "medallion_number": getattr(tlc_violation.medallion, "medallion_number", None),
        "lease_id": getattr(tlc_violation.lease, "lease_id", None),
        "vin": getattr(tlc_violation.vehicle, "vin", None),

        # Other fields
        "plate": tlc_violation.plate,
        "state": tlc_violation.state,
        "attachment_document_id": tlc_violation.attachment_document_id,
        "status": getattr(tlc_violation.status, "value", None)
    }
