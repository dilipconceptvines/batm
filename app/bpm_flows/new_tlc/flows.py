### app/bpm_flows/newtlc/flows.py

from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.audit_trail.schemas import AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.leases.services import lease_service
from app.drivers.services import driver_service
from app.vehicles.services import vehicle_service
from app.medallions.services import medallion_service
from app.tlc.services import TLCService
from app.tlc.models import TLCViolation , TLCViolationType
from app.uploads.services import upload_service
from app.utils.logger import get_logger
from app.medallions.utils import format_medallion_response
from app.core.config import settings


logger = get_logger(__name__)

ENTITY_MAPPER = {
    "TLC": "tlc_violation",
    "TLC_IDENTIFIER": "id",
}

@step(step_id="222", name="Fetch - Choose Driver", operation="fetch")
def choose_driver_fetch(db: Session, case_no: str, case_params: dict = None):
    """
    Fetches driver and associated active lease information for the TLC violation workflow.
    """
    try:
        if not case_params:
            return {"search_results": []}
        
        driver_name = case_params.get("driver_name")
        tlc_license_no = case_params.get("tlc_license_no")
        medallion_no = case_params.get("medallion_no")

        if not any([medallion_no, tlc_license_no, driver_name]):
            return {"search_results": []}

        driver = None
        active_leases = []
        
        # Search by TLC License Number
        if tlc_license_no:
            driver = driver_service.get_drivers(db, tlc_license_number=tlc_license_no)
            if driver:
                active_leases = lease_service.get_lease(
                    db, 
                    driver_id=driver.driver_id, 
                    status="Active", 
                    exclude_additional_drivers=True, 
                    multiple=True
                )

        
        # Search by Medallion Number
        elif medallion_no:
            active_leases = lease_service.get_lease(
                db, 
                medallion_number=medallion_no, 
                status="Active", 
                multiple=True
            )
            if active_leases and active_leases[0]:
                # Get primary driver from first lease
                first_lease = active_leases[0][0] if isinstance(active_leases[0], list) else active_leases[0]
                for lease_driver in first_lease.lease_driver:
                    if not lease_driver.is_additional_driver:
                        driver = lease_driver.driver
                        break
        
        # Search by Vehicle Plate Number
        elif driver_name:
            driver = driver_service.get_drivers(db, driver_name=driver_name)
            if driver:
                active_leases = lease_service.get_lease(
                    db, 
                    driver_id=driver.driver_id, 
                    status="Active", 
                    exclude_additional_drivers=True, 
                    multiple=True
                )

        if not driver:
            logger.info("No driver found for PVB case", case_no=case_no)
            return {
                "driver": None,
                "leases": []
            }
        
        if active_leases and active_leases[0]:
            if isinstance(active_leases[0], list):
                lease_list = active_leases[0]
            else:
                lease_list = [active_leases[0]]
        else:
            lease_list = []
        
        if not lease_list:
            logger.warning("No active leases found for driver", driver_id=driver.id)
            return {
                "driver": {
                    "id": driver.id,
                    "driver_id": driver.driver_id,
                    "full_name": driver.full_name,
                    "status": driver.driver_status.value if hasattr(driver.driver_status, 'value') else str(driver.driver_status),
                    "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
                    "phone": driver.phone_number_1 or "N/A",
                    "email": driver.email_address or "N/A",
                },
                "leases": []
            }
        
        # Format lease data for UI
        formatted_leases = []
        for lease in lease_list:
            driver_lease = lease_service.get_lease_drivers(db=db , lease_id=lease.id , driver_id=driver.driver_id , is_additional_driver=False)

            if not driver_lease:
                continue
            
            medallion_owner = format_medallion_response(lease.medallion).get("medallion_owner" , "N/A") if lease.medallion else "N/A"
            formatted_leases.append({
                "id": lease.id,
                "lease_id": lease.lease_id,
                "medallion_number": lease.medallion.medallion_number if lease.medallion else "N/A",
                "medallion_owner": medallion_owner,
                "plate_no": lease.vehicle.registrations[0].plate_number if lease.vehicle and lease.vehicle.registrations else "N/A",
                "vin": lease.vehicle.vin if lease.vehicle else "N/A",
                "vehicle_id": lease.vehicle_id if lease.vehicle_id else None,
                "medallion_id": lease.medallion_id if lease.medallion_id else None,
            })
        
        driver_data = {
            "id": driver.id,
            "driver_id": driver.driver_id,
            "full_name": driver.full_name,
            "status": driver.driver_status.value if hasattr(driver.driver_status, 'value') else str(driver.driver_status),
            "tlc_license": driver.tlc_license.tlc_license_number if driver.tlc_license else "N/A",
            "phone": driver.phone_number_1 or "N/A",
            "email": driver.email_address or "N/A",
        }

        tlc_ticket = upload_service.get_documents(
            db=db,
            object_type="tlc",
            object_id=case_no,
            document_type="tlc_ticket"
        )
        
        logger.info("Successfully fetched driver and lease details for PVB", case_no=case_no, driver_id=driver.id)

        violation_tyeps = {
            TLCViolationType.FI.value : "Failure to Inspect Vehicle",
            TLCViolationType.FN.value : "Failure to Comply with Notice",
            TLCViolationType.RF.value : "Reinspection Fee",
            TLCViolationType.EA.value : [
                "Air Bag Light",
                "Defective Light",
                "Dirty Cab",
                "Meter Mile Run",
                "Windshield"
            ]
        }
        
        return {
            "driver": driver_data,
            "leases": formatted_leases,
            "tlc_ticket": tlc_ticket,
            "violation_types": violation_tyeps,
            "service_fee": settings.tlc_service_fee
        }
        
    except Exception as e:
        logger.error("Error in TLC choose_driver_fetch: %s", e, exc_info=True)
        raise

@step(step_id="222", name="Process - Choose Driver", operation="process")
def choose_driver_process(db: Session, case_no: str, step_data: dict):
     """
    Creates a preliminary TLC Ticket violation record and associates it with the selected driver and lease.
    
    Expected step_data:
        - driver_id: Driver primary key
        - lease_id: Lease primary key
        - vehicle_id: Vehicle primary key
        - medallion_id: Medallion primary key
        - vehicle_plate_no: Vehicle plate number
     """
     try:
        logger.info("Processing driver selection for PVB case", case_no=case_no)
        
        # Validate required fields
        driver_id = step_data.get("driver_id" , None)
        lease_id = step_data.get("lease_id" , None)
        vehicle_id = step_data.get("vehicle_id" , None)
        medallion_id = step_data.get("medallion_id" , None)
        vehicle_plate_no = step_data.get("vehicle_plate_no" , None)
        
        required_fields = {
            "driver_id": driver_id,
            "lease_id": lease_id,
            "vehicle_id": vehicle_id,
            "medallion_id": medallion_id,
            "vehicle_plate_no": vehicle_plate_no
        }

        missing = [name for name, value in required_fields.items() if not value]

        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required fields: {', '.join(missing)}"
            )
        
        # Validate driver existence
        driver = driver_service.get_drivers(db, id=driver_id)
        if not driver:
            raise HTTPException(status_code=404, detail="Driver not found")
        
        # Validate lease existence
        lease = lease_service.get_lease(db, lookup_id=lease_id, status="Active")
        if not lease:
            raise HTTPException(status_code=404, detail="Active lease not found")
        
        # Verify driver is the primary driver on the lease
        is_primary_driver = False
        for lease_driver in lease.lease_driver:
            if lease_driver.driver_id == driver.driver_id and not lease_driver.is_additional_driver:
                is_primary_driver = True
                break
        
        if not is_primary_driver:
            raise HTTPException(
                status_code=400, 
                detail="Driver is not the primary driver on the selected lease"
            )
        
        vehicle = vehicle_service.get_vehicles(
            db=db , vehicle_id=vehicle_id, plate_number=vehicle_plate_no
        )
        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        
        if vehicle.id != lease.vehicle_id:
            raise HTTPException(status_code=400, detail="Vehicle does not belong to the selected lease")
        
        medallion = medallion_service.get_medallion(db, medallion_id=medallion_id)
        if not medallion:
            raise HTTPException(status_code=404, detail="Medallion not found")
        
        if medallion.id != lease.medallion_id:
            raise HTTPException(status_code=400, detail="Medallion does not belong to the selected lease")
        
        case_entity = bpm_service.get_case_entity(db, case_no=case_no, entity_name=ENTITY_MAPPER["TLC"])

        tlc_ticket = upload_service.get_documents(
            db=db,
            object_type="tlc",
            object_id=case_no,
            document_type="tlc_ticket"
        )

        tlc_service = TLCService(db)

        if case_entity:
            violation = db.query(TLCViolation).filter_by(id=int(case_entity.identifier_value)).first()
            if not violation:
                raise HTTPException(status_code=404, detail="TLC violation not found")

            violation.driver_id = driver.id
            violation.lease_id = lease.id
            violation.vehicle_id = vehicle.id
            violation.medallion_id = medallion.id
            violation.summons_no = step_data.get("summons_number")
            violation.issue_date = step_data.get("issue_date")
            violation.issue_time = datetime.now().time()
            violation.plate = vehicle_plate_no
            violation.violation_type = step_data.get("ticket_type")
            violation.description = step_data.get("description")
            violation.amount = Decimal(step_data.get("penalty_amount"))
            violation.service_fee = settings.tlc_inspection_fees
            violation.total_payable = Decimal(step_data.get("penalty_amount"))+Decimal(settings.tlc_inspection_fees)
            violation.driver_payable = Decimal(step_data.get("penalty_amount"))+Decimal(settings.tlc_inspection_fees)
            violation.disposition = step_data.get("disposition")
            violation.due_date = step_data.get("due_date")
            violation.note = step_data.get("note")
            db.add(violation)
            db.flush()
            logger.info("TLC violation updated successfully.")

        else:
            tlc_data = {
                "driver_id": driver.id,
                "lease_id": lease.id,
                "vehicle_id": vehicle.id,
                "medallion_id": medallion.id,
                "summons_no": step_data.get("summons_number"),
                "issue_date": step_data.get("issue_date"),
                "issue_time": datetime.now().time(),
                "plate": vehicle_plate_no,
                "state": "NY",
                "violation_type": step_data.get("ticket_type"),
                "description": step_data.get("description"),
                "amount": Decimal(step_data.get("penalty_amount")),
                "disposition": step_data.get("disposition"),
                "due_date": step_data.get("due_date"),
                "note": step_data.get("note"),
                "attachment_document_id": tlc_ticket.get("document_id") or None
            }
            violation = tlc_service.create_manual_violation(case_no, tlc_data , 1)
            logger.info("TLC violation created successfully.")

        if tlc_ticket and tlc_ticket.get("document_path"):
            upload_service.update_document(
                    db=db , 
                    document_dict={"document_id": tlc_ticket.get("document_id")},
                    document_path=tlc_ticket.get("document_path"),
                    object_id= violation.id,
                    object_type="tlc",
                    document_type="tlc_ticket",
                    new_filename=tlc_ticket.get("document_name"),
                    file_size_kb=tlc_ticket.get("document_size"),
                    original_extension=tlc_ticket.get("document_format"),
                    document_date=datetime.now().strftime('%Y-%m-%d'),
                    notes=tlc_ticket.get("document_notes")
                )
            logger.info("TLC ticket document updated successfully.")

        if not case_entity:
            bpm_service.create_case_entity(
                db, case_no, ENTITY_MAPPER["TLC"], ENTITY_MAPPER["TLC_IDENTIFIER"], str(violation.id)
            )

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"TLC violation created for case {case_no}",
                meta_data={"driver_id": driver_id, "lease_id": lease_id, "vehicle_id": vehicle_id, "medallion_id": medallion_id}
            )

        logger.info("TLC violation created successfully.")
        return "Ok"
     except Exception as e:
        logger.error("Error in TLC choose_driver_process: %s", e, exc_info=True)
        raise