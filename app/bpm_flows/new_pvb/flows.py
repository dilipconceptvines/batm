### app/bpm_flows/new_pvb/flows.py

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.audit_trail.schemas import AuditTrailType
from app.audit_trail.services import audit_trail_service
from app.bpm.services import bpm_service
from app.bpm.step_info import step
from app.drivers.services import driver_service
from app.leases.services import lease_service
from app.vehicles.services import vehicle_service
from app.medallions.services import medallion_service
from app.pvb.models import PVBViolation, PVBViolationStatus, PVBSource
from app.pvb.services import PVBService
from app.pvb.tasks import post_pvb_violations_to_ledger_task
from app.uploads.services import upload_service
from app.utils.logger import get_logger
from app.medallions.utils import format_medallion_response

logger = get_logger(__name__)

ENTITY_MAPPER = {
    "PVB": "pvb_violation",
    "PVB_IDENTIFIER": "id",
}


@step(step_id="161", name="Fetch - Choose Driver", operation="fetch")
def choose_driver_fetch(db: Session, case_no: str, case_params: Dict[str, Any] = None):
    """
    Fetches driver and lease information based on search criteria for the first step
    of the manual PVB creation workflow.
    
    Query Parameters:
        - medallion_no: Medallion number (e.g., "1P43")
        - tlc_license_no: TLC License number (e.g., "00504138")
        - vehicle_plate_no: Vehicle plate number (e.g., "Y203812C-NY")
    """
    try:
        logger.info("Fetching driver details for PVB case", case_no=case_no)
        
        if not case_params:
            return {
                "driver": None,
                "leases": []
            }
        
        # Extract search parameters
        medallion_no = case_params.get("medallion_no")
        tlc_license_no = case_params.get("tlc_license_no")
        vehicle_plate_no = case_params.get("vehicle_plate_no")
        
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
        elif vehicle_plate_no:
            # Extract plate and state if format is "PLATE-STATE"
            if "-" in vehicle_plate_no:
                plate, state = vehicle_plate_no.rsplit("-", 1)
            else:
                plate = vehicle_plate_no
                state = "NY"
            
            active_leases = lease_service.get_lease(
                db, 
                plate_number=plate, 
                status="Active", 
                multiple=True
            )
            if active_leases and active_leases[0]:
                first_lease = active_leases[0][0] if isinstance(active_leases[0], list) else active_leases[0]
                for lease_driver in first_lease.lease_driver:
                    if not lease_driver.is_additional_driver:
                        driver = lease_driver.driver
                        break
        
        if not driver:
            logger.info("No driver found for PVB case", case_no=case_no)
            return {
                "driver": None,
                "leases": []
            }
        
        # Handle the lease data structure
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

        pvb_invoice = upload_service.get_documents(
            db=db,
            object_type="pvb",
            object_id=case_no,
            document_type="pvb_invoice"
        )
        
        logger.info("Successfully fetched driver and lease details for PVB", case_no=case_no, driver_id=driver.id)
        
        return {
            "driver": driver_data,
            "leases": formatted_leases,
            "pvb_invoice": pvb_invoice
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching driver and lease details for PVB", case_no=case_no, error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while fetching driver details") from e


@step(step_id="161", name="Process - Choose Driver", operation="process")
def choose_driver_process(db: Session, case_no: str, step_data: Dict[str, Any]):
    """
    Creates a preliminary PVB violation record and associates it with the selected driver and lease.
    
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
        
        # Check if case entity already exists
        case_entity = bpm_service.get_case_entity(db, case_no=case_no, entity_name=ENTITY_MAPPER["PVB"])

        fine = Decimal(step_data.get("fine" , 0))
        penalty = Decimal(step_data.get("penalty" , 0))
        interest = Decimal(step_data.get("interest" , 0))
        reduction = Decimal(step_data.get("reduction" , 0))
        payment = Decimal(step_data.get("payment" , 0))
        processing_fee = 0

        if fine <= 50:
            processing_fee = Decimal(5)
        elif fine > 50:
            processing_fee = Decimal(7)

        amount_due = (fine + penalty + interest + processing_fee) - (reduction + payment)

        if amount_due != Decimal(step_data.get("amount_due" , 0)):
            raise HTTPException(
                status_code=400,
                detail="Amount due does not match the calculated total from fine, penalty, interest, reduction, processing fee, and payment"
            )
        
        pvb_service = PVBService(db)
        
        if case_entity:
            # Update existing violation record
            violation = db.query(PVBViolation).filter(PVBViolation.id == int(case_entity.identifier_value)).first()
            if violation:
                violation.driver_id = driver.id
                violation.lease_id = lease.id
                violation.vehicle_id = vehicle_id
                violation.medallion_id = medallion_id
                violation.plate = vehicle_plate_no.split("-")[0] if "-" in vehicle_plate_no else vehicle_plate_no
                violation.state = step_data.get("state" , None)
                violation.type = step_data.get("type" , None)
                violation.summons = step_data.get("summons" , None)
                violation.issue_date = step_data.get("issue_date" , None)
                violation.issue_time = step_data.get("issue_time" , None)
                violation.fine = fine
                violation.penalty = penalty
                violation.interest = interest
                violation.reduction = reduction
                violation.amount_due = amount_due
                violation.violation_code = step_data.get("violation_code" , None)
                violation.violation_country = step_data.get("violation_country" , None)
                violation.street_name = step_data.get("street_name" , None)
                violation.processing_fee = processing_fee
                violation.front_or_opp = step_data.get("front_or_opp" , None)
                violation.house_number = step_data.get("house_number" , None)
                violation.intersect_street = step_data.get("intersect_street" , None)
                violation.geo_location = step_data.get("geo_location" , None)
                violation.street_code_1 = step_data.get("street_code_1" , None)
                violation.street_code_2 = step_data.get("street_code_2" , None)
                violation.street_code_3 = step_data.get("street_code_3" , None)
                violation.non_program = step_data.get("non_program" , False)
                violation.payment = payment
                violation.ng_pmt = step_data.get("ng_pmt")
                violation.judgement = step_data.get("judgement")
                violation.system_entry_date = step_data.get("system_entry_date" , None)
                violation.new_issue = step_data.get("new_issue" , False)
                violation.hearing_ind = step_data.get("hearing" , None)
                violation.penalty_warning = step_data.get("penalty_warning" , None)


                db.flush()
                logger.info("Updated existing PVB violation record", violation_id=violation.id)
        else:
            # Create new preliminary violation record
            initial_data = {
                "driver_id": driver.id,
                "vehicle_id": vehicle_id,
                "medallion_id": medallion_id,
                "lease_id": lease.id,
                "status": PVBViolationStatus.ASSOCIATED.value,
                "plate": vehicle_plate_no.split("-")[0] if "-" in vehicle_plate_no else vehicle_plate_no,
                "state": step_data.get("state" , None),
                "type": step_data.get("type" , None),
                "summons": step_data.get("summons" , None),
                "issue_time": step_data.get("issue_time" , None),
                "issue_date": step_data.get("issue_date" , None),
                "fine": fine,
                "processing_fee": processing_fee,
                "penalty": penalty,
                "interest": interest,
                "reduction": reduction,
                "amount_due": amount_due,
                "violation_code": step_data.get("violation_code" , None),
                "violation_country": step_data.get("violation_country" , None),
                "street_name": step_data.get("street_name" , None),
                "front_or_opp": step_data.get("front_or_opp" , None),
                "house_number": step_data.get("house_number" , None),
                "intersect_street": step_data.get("intersect_street" , None),
                "geo_location": step_data.get("geo_location" , None),
                "street_code_1": step_data.get("street_code_1" , None),
                "street_code_2": step_data.get("street_code_2" , None),
                "street_code_3": step_data.get("street_code_3" , None),
                "non_program": step_data.get("non_program" , False),
                "payment": payment,
                "ng_pmt": step_data.get("ng_pmt" ),
                "judgement": step_data.get("judgement" , False),
                "system_entry_date": step_data.get("system_entry_date" , None),
                "new_issue": step_data.get("new_issue" , False),
                "hearing_ind": step_data.get("hearing" , None),
                "penalty_warning": step_data.get("penalty_warning" , None),

            }
            
            violation = pvb_service.create_manual_violation(case_no, initial_data, 1)

            pvb_invoice = upload_service.get_documents(
                db=db,
                object_type="pvb",
                object_id=case_no,
                document_type="pvb_invoice"
            )

            if pvb_invoice and pvb_invoice.get("document_path"):
                upload_service.update_document(
                    db=db , 
                    document_dict={"document_id": pvb_invoice.get("document_id")},
                    document_path=pvb_invoice.get("document_path"),
                    object_id= violation.id,
                    object_type="pvb",
                    document_type="pvb_invoice",
                    new_filename=pvb_invoice.get("document_name"),
                    file_size_kb=pvb_invoice.get("document_size"),
                    original_extension=pvb_invoice.get("document_format"),
                    document_date=datetime.now().strftime('%Y-%m-%d'),
                    notes=pvb_invoice.get("document_notes")
                )
            
            # Create case entity
            bpm_service.create_case_entity(
                db, 
                case_no, 
                ENTITY_MAPPER["PVB"], 
                ENTITY_MAPPER["PVB_IDENTIFIER"], 
                str(violation.id)
            )
            
            logger.info("Created preliminary PVB violation record", violation_id=violation.id)
        
        return "Ok"
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error in choose_driver_process for PVB", case_no=case_no, error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred while processing driver selection: {str(e)}") from e


# @step(step_id="160", name="Fetch - Enter PVB Details", operation="fetch")
# def enter_details_fetch(db: Session, case_no: str, case_params: Dict[str, Any] = None):
#     """
#     Fetches the existing (partially filled) PVB violation details for editing.
#     Also returns associated driver, medallion, and vehicle information.
#     """
#     try:
#         logger.info("Fetching PVB details for case", case_no=case_no)
        
#         case_entity = bpm_service.get_case_entity(db, case_no=case_no, entity_name=ENTITY_MAPPER["PVB"])
#         if not case_entity:
#             raise HTTPException(status_code=404, detail="No PVB violation record found for this case")
        
#         violation = db.query(PVBViolation).filter(PVBViolation.id == int(case_entity.identifier_value)).first()
#         if not violation:
#             raise HTTPException(status_code=404, detail=f"Violation with ID {case_entity.identifier_value} not found")
        
#         # Get associated entities
#         driver_data = None
#         if violation.driver:
#             driver_data = {
#                 "id": violation.driver.id,
#                 "driver_id": violation.driver.driver_id,
#                 "full_name": violation.driver.full_name,
#                 "tlc_license": violation.driver.tlc_license.tlc_license_number if violation.driver.tlc_license else "N/A",
#             }
        
#         medallion_data = None
#         if violation.medallion:
#             medallion_data = {
#                 "id": violation.medallion.id,
#                 "medallion_number": violation.medallion.medallion_number,
#             }
        
#         vehicle_data = None
#         if violation.vehicle:
#             vehicle_data = {
#                 "id": violation.vehicle.id,
#                 "vin": violation.vehicle.vin,
#                 "plate_number": violation.vehicle.registrations[0].plate_number if violation.vehicle.registrations else "N/A",
#             }
        
#         return {
#             "violation": {
#                 "id": violation.id,
#                 "plate": violation.plate,
#                 "state": violation.state,
#                 "type": violation.type if violation.type != "TEMP" else "",
#                 "summons": violation.summons if not violation.summons.startswith("TEMP-") else "",
#                 "issue_date": violation.issue_date.isoformat() if violation.issue_date else "",
#                 "issue_time": violation.issue_time.strftime("%H%M") if violation.issue_time else "",
#                 "fine": float(violation.fine) if violation.fine else 0.00,
#                 "penalty": float(violation.penalty) if violation.penalty else 0.00,
#                 "interest": float(violation.interest) if violation.interest else 0.00,
#                 "reduction": float(violation.reduction) if violation.reduction else 0.00,
#                 "amount_due": float(violation.amount_due) if violation.amount_due else 0.00,
#             },
#             "driver": driver_data,
#             "medallion": medallion_data,
#             "vehicle": vehicle_data,
#         }
    
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error("Error in enter_details_fetch for PVB", case_no=case_no, error=str(e), exc_info=True)
#         raise HTTPException(status_code=500, detail=f"An error occurred while fetching PVB details: {str(e)}") from e


# @step(step_id="160", name="Process - Enter PVB Details", operation="process")
# def enter_details_process(db: Session, case_no: str, step_data: Dict[str, Any]):
#     """
#     Updates the PVB violation record with the detailed ticket information.
    
#     Expected step_data:
#         - plate: License plate number
#         - state: State code (e.g., "NY")
#         - type: Violation type code
#         - summons: Summons/ticket number
#         - issue_date: Date violation was issued (ISO format)
#         - issue_time: Time violation was issued (HHMM format, optional)
#         - fine: Base fine amount
#         - penalty: Penalty amount (optional)
#         - interest: Interest amount (optional)
#         - reduction: Reduction amount (optional)
#     """
#     try:
#         logger.info("Processing PVB details for case", case_no=case_no)
        
#         pvb_service = PVBService(db)
#         case_entity = bpm_service.get_case_entity(db, case_no=case_no, entity_name=ENTITY_MAPPER["PVB"])
        
#         if not case_entity:
#             raise HTTPException(status_code=404, detail="No PVB violation record found for this case")
        
#         violation = db.query(PVBViolation).filter(PVBViolation.id == int(case_entity.identifier_value)).first()
#         if not violation:
#             raise HTTPException(status_code=404, detail=f"Violation with ID {case_entity.identifier_value} not found")
        
#         # Validate required fields
#         required_fields = ["plate", "state", "type", "summons", "issue_date", "fine"]
#         missing_fields = [field for field in required_fields if not step_data.get(field)]
#         if missing_fields:
#             raise HTTPException(
#                 status_code=400, 
#                 detail=f"Missing required fields: {', '.join(missing_fields)}"
#             )
        
#         # Parse issue_time if provided (format: HHMMX where X is A or P)
#         issue_time = None
#         if step_data.get("issue_time"):
#             try:
#                 time_str = step_data["issue_time"]
#                 if len(time_str) == 5:  # HHMMA or HHMMP
#                     meridiem = time_str[-1].upper()
#                     hour = int(time_str[:2])
#                     minute = int(time_str[2:4])
                    
#                     if meridiem == 'P' and hour != 12:
#                         hour += 12
#                     elif meridiem == 'A' and hour == 12:
#                         hour = 0
                    
#                     issue_time = f"{hour:02d}:{minute:02d}:00"
#             except (ValueError, IndexError) as e:
#                 logger.warning("Invalid time format for PVB", time_str=step_data.get("issue_time"), error=str(e))
        
#         # Calculate amount due
#         fine = Decimal(str(step_data.get("fine", 0)))
#         penalty = Decimal(str(step_data.get("penalty", 0)))
#         interest = Decimal(str(step_data.get("interest", 0)))
#         reduction = Decimal(str(step_data.get("reduction", 0)))
#         amount_due = fine + penalty + interest - reduction
        
#         # Update violation record
#         update_data = {
#             "plate": step_data["plate"],
#             "state": step_data["state"],
#             "type": step_data["type"],
#             "summons": step_data["summons"],
#             "issue_date": step_data["issue_date"],
#             "issue_time": issue_time,
#             "fine": fine,
#             "penalty": penalty,
#             "interest": interest,
#             "reduction": reduction,
#             "amount_due": amount_due,
#             "updated_on": datetime.utcnow(),
#         }
        
#         pvb_service.repo.update_violation(violation.id, update_data)
        
#         logger.info("PVB violation details updated successfully", violation_id=violation.id, summons=step_data["summons"])
        
#         return "Ok"
    
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error("Error in enter_details_process for PVB", case_no=case_no, error=str(e), exc_info=True)
#         raise HTTPException(status_code=500, detail=f"An error occurred while updating PVB details: {str(e)}") from e


# @step(step_id="161", name="Fetch - Attach Proof", operation="fetch")
# def attach_proof_fetch(db: Session, case_no: str, case_params: Dict[str, Any] = None):
#     """
#     Fetches any existing proof documents for the PVB violation and returns
#     violation summary for confirmation.
#     """
#     try:
#         logger.info("Fetching PVB proof documents for case", case_no=case_no)
        
#         case_entity = bpm_service.get_case_entity(db, case_no=case_no, entity_name=ENTITY_MAPPER["PVB"])
#         if not case_entity:
#             return {
#                 "documents": [],
#                 "violation_summary": None
#             }
        
#         violation = db.query(PVBViolation).filter(PVBViolation.id == int(case_entity.identifier_value)).first()
#         if not violation:
#             return {
#                 "documents": [],
#                 "violation_summary": None
#             }
        
#         # Get uploaded documents
#         documents = upload_service.get_documents(
#             db,
#             object_type="pvb_violation",
#             object_id=case_entity.identifier_value,
#             multiple=True,
#         )
        
#         # Prepare violation summary for confirmation
#         violation_summary = {
#             "summons": violation.summons,
#             "plate": violation.plate,
#             "state": violation.state,
#             "type": violation.type,
#             "issue_date": violation.issue_date.isoformat() if violation.issue_date else "",
#             "amount_due": float(violation.amount_due) if violation.amount_due else 0.00,
#             "driver": {
#                 "full_name": violation.driver.full_name if violation.driver else "N/A",
#                 "tlc_license": violation.driver.tlc_license.tlc_license_number if violation.driver and violation.driver.tlc_license else "N/A",
#             },
#             "medallion": {
#                 "medallion_number": violation.medallion.medallion_number if violation.medallion else "N/A",
#             },
#         }
        
#         return {
#             "documents": documents or [],
#             "violation_summary": violation_summary
#         }
    
#     except Exception as e:
#         logger.error("Error in attach_proof_fetch for PVB", case_no=case_no, error=str(e), exc_info=True)
#         raise HTTPException(status_code=500, detail=f"An error occurred while fetching proof documents: {str(e)}") from e


# @step(step_id="161", name="Process - Attach Proof", operation="process")
# def attach_proof_process(db: Session, case_no: str, step_data: Dict[str, Any]):
#     """
#     Finalizes the manual PVB violation entry. It associates the uploaded proof,
#     marks the violation as ASSOCIATED, and triggers the ledger posting task.
    
#     Expected step_data:
#         - document_id: ID of the uploaded proof document (optional but recommended)
#     """
#     try:
#         logger.info("Processing PVB proof attachment for case", case_no=case_no)
        
#         pvb_service = PVBService(db)
#         case_entity = bpm_service.get_case_entity(db, case_no=case_no, entity_name=ENTITY_MAPPER["PVB"])
        
#         if not case_entity:
#             raise HTTPException(status_code=404, detail="No PVB violation record found for this case")
        
#         violation = db.query(PVBViolation).filter(PVBViolation.id == int(case_entity.identifier_value)).first()
#         if not violation:
#             raise HTTPException(status_code=404, detail=f"Violation with ID {case_entity.identifier_value} not found")
        
#         # Link document to the violation if provided
#         document_id = step_data.get("document_id")
#         if document_id:
#             try:
#                 upload_service.upsert_document(db, {
#                     "id": document_id,
#                     "object_type": "pvb_violation",
#                     "object_lookup_id": case_entity.identifier_value
#                 })
#                 logger.info("Linked proof document to PVB violation", violation_id=violation.id, document_id=document_id)
#             except Exception as e:
#                 logger.warning("Failed to link document to PVB violation", error=str(e))
        
#         # Finalize the violation status
#         pvb_service.repo.update_violation(
#             int(case_entity.identifier_value),
#             {
#                 "status": PVBViolationStatus.ASSOCIATED,
#                 "updated_on": datetime.utcnow()
#             }
#         )
        
#         # Trigger asynchronous ledger posting
#         post_pvb_violations_to_ledger_task.delay()
        
#         # Create audit trail
#         case = bpm_service.get_cases(db, case_no=case_no)
#         audit_trail_service.create_audit_trail(
#             db,
#             description=f"Manual PVB violation created: Summons {violation.summons}",
#             case=case,
#             meta_data={
#                 "pvb_id": violation.id,
#                 "summons": violation.summons,
#                 "driver_id": violation.driver_id,
#                 "lease_id": violation.lease_id,
#                 "amount_due": float(violation.amount_due),
#             },
#             audit_type=AuditTrailType.AUTOMATED,
#         )
        
#         logger.info("PVB violation finalized and queued for ledger posting", violation_id=violation.id, summons=violation.summons)

#         return "Ok"

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error("Error in attach_proof_process for PVB", case_no=case_no, error=str(e), exc_info=True)
#         raise HTTPException(status_code=500, detail=f"An error occurred while finalizing PVB violation: {str(e)}") from e