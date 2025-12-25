from app.pvb.models import PVBViolation

def format_pvb_violation(violation: PVBViolation) -> dict:
    if not violation:
        return {}
    return {
        "source": violation.source.value,
        "id": violation.id,
        "plate": violation.plate,
        "state": violation.state,
        "type": violation.type,
        "summons": violation.summons,
        "issue_date": violation.issue_date,
        "issue_time": violation.issue_time,
        "violation_code": violation.violation_code,
        "amount_due": violation.amount_due,
        "fine": violation.fine,
        "processing_fee": violation.processing_fee,
        "payment": violation.payment,
        "disposition": violation.disposition,
        "disposition_change_date": violation.disposition_change_date,
        "note": violation.note,
        "reduce_by": violation.reduce_to,
    }