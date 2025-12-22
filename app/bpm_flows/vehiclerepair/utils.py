## app/bpm_flows/vehiclerepair/utils.py

# Standard library imports
from datetime import datetime

# Third-party imports
from sqlalchemy.orm import Session, joinedload

# Local application imports
from app.vehicles.models import Vehicle, VehicleRepair
from app.utils.logger import get_logger
from app.uploads.models import Document

logger = get_logger(__name__)

def fetch_vehicle_by_vin(db: Session, vehicle_vin: str) -> Vehicle:
    """
    Fetch the vehicle by vin
    """
    vehicle = db.query(Vehicle).options(joinedload(Vehicle.entity)).filter(
        Vehicle.vin == vehicle_vin).first()

    return vehicle

def get_repair_by_id(db: Session, repair_id: int):
    """Get repair record by ID"""
    return db.query(VehicleRepair).filter(
        VehicleRepair.id == repair_id
    ).first()

def create_vehicle_repair(db: Session, vehicle_id: int):
    """Create new vehicle repair record"""
    repair = VehicleRepair(
        vehicle_id=vehicle_id,
        status="In Progress"
    )
    db.add(repair)
    db.flush()
    return repair

def get_repair_details(db: Session, repair: VehicleRepair):
    """Get repair details"""
    if not repair:
        return {}
        
    return {
        "invoice_date": repair.invoice_date.strftime('%Y-%m-%d') if repair.invoice_date else None,
        "invoice_amount": repair.invoice_amount,
        "vehicle_in_date": repair.vehicle_in_date.strftime('%Y-%m-%d') if repair.vehicle_in_date else None,
        "vehicle_in_time": repair.vehicle_in_time,
        "vehicle_out_date": repair.vehicle_out_date.strftime('%Y-%m-%d') if repair.vehicle_out_date else None,
        "vehicle_out_time": repair.vehicle_out_time,
        "repair_paid_by": repair.repair_paid_by,
        "next_service_due_by": repair.next_service_due_by.strftime('%Y-%m-%d') if repair.next_service_due_by else None,
        "remarks": repair.remarks,
        "status": repair.status
    }

def update_repair_details(db: Session, repair: VehicleRepair, data: dict):
    """Update repair details"""
    date_fields = ['invoice_date', 'vehicle_in_date', 'vehicle_out_date', 'next_service_due_by']
    
    for field in date_fields:
        if field in data and data[field]:
            try:
                setattr(repair, field, datetime.strptime(data[field], '%Y-%m-%d').date())
            except ValueError:
                logger.error(f"Invalid date format for {field}: {data[field]}")
            
    time_fields = ['vehicle_in_time', 'vehicle_out_time']
    for field in time_fields:
        if field in data:
            setattr(repair, field, data[field])
            
    other_fields = ['invoice_amount', 'repair_paid_by', 'remarks']
    for field in other_fields:
        if field in data:
            setattr(repair, field, data[field])
            
    db.add(repair)
    db.flush()
    
def get_repair_documents(db: Session, repair: VehicleRepair):
    """Get repair related documents"""
    if not repair or not repair.invoice_document_id:
        return None
        
    document = db.query(Document).filter(
        Document.id == repair.invoice_document_id
    ).first()
    
    if document:
        return {
            "id": document.id,
            "name": document.document_name,
            "type": document.document_type,
            "upload_date": document.document_upload_date,
            "presigned_url": document.presigned_url if document.presigned_url else None
        }
    return None