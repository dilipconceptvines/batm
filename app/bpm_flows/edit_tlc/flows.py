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
from app.leases.schemas import LeaseStatus
from app.drivers.services import driver_service
from app.vehicles.services import vehicle_service
from app.medallions.services import medallion_service
from app.tlc.services import TLCService
from app.tlc.models import TLCViolation , TLCViolationType , TLCDisposition
from app.uploads.services import upload_service
from app.utils.logger import get_logger
from app.medallions.utils import format_medallion_response
from app.bpm_flows.new_tlc.utils import format_tlc_violation
from app.ledger.services import LedgerService
from app.ledger.repository import LedgerRepository
from app.ledger.models import PostingCategory , EntryType
from app.core.config import settings


logger = get_logger(__name__)

ENTITY_MAPPER = {
    "TLC": "tlc_violation",
    "TLC_IDENTIFIER": "id",
}

@step(step_id="223", name="Fetch - Choose Driver", operation="fetch")
def choose_driver_fetch(db: Session, case_no: str, case_params: dict = None):
    """
    Fetches driver and associated active lease information for the TLC violation workflow.
    """
    try:
        violation = None
        case_entity = bpm_service.get_case_entity(db, case_no=case_no, entity_name=ENTITY_MAPPER["TLC"])

        if case_params and case_params.get("object_name") == "tlc_violation":
            violation = db.query(TLCViolation).filter_by(id=int(case_params.get("object_lookup"))).first()
        if case_entity:
            violation = db.query(TLCViolation).filter_by(id=int(case_entity.identifier_value)).first()

        if not violation:
            return {}


        violation_data = format_tlc_violation(db , violation)

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

        if not case_entity:
            bpm_service.create_case_entity(
                db, case_no, ENTITY_MAPPER["TLC"], ENTITY_MAPPER["TLC_IDENTIFIER"], str(violation.id)
            )
          

        driver = None
        active_lease = None        
        if violation:
            driver = driver_service.get_drivers(db, id=violation.driver_id)
            active_lease = lease_service.get_lease(db,lookup_id=violation.lease_id, status= LeaseStatus.ACTIVE.value)

        if not driver:
            logger.info("No driver found for TLC case", case_no=case_no)
            return {
                "driver": None,
                "leases": [],
                "tlc_violation": violation_data,
                "violation_types": violation_tyeps
            }
        
        if not active_lease:
            logger.warning("No active lease found for driver", driver_id=driver.id)
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
                "lease": {},
                "tlc_violation": violation_data,
                "violation_types": violation_tyeps
            }
        
        # Format lease data for UI

        medallion_owner = format_medallion_response(active_lease.medallion).get("medallion_owner") if active_lease.medallion else None

        lease_confis = active_lease.lease_configuration
        lease_amount = 0

        if lease_confis:
            config = config = next(
                (c for c in lease_confis if c.lease_breakup_type == "lease_amount"),
                None
            )
            if config and config.lease_limit:
                lease_amount = float(config.lease_limit)

        format_data = {}
        format_data["lease"] = {
                "id": active_lease.id,
                "lease_id": active_lease.lease_id,
                "lease_type": active_lease.lease_type if active_lease.lease_type else "N/A",
                "status": active_lease.lease_status if active_lease.lease_status else "N/A",
                "start_date": active_lease.lease_start_date if active_lease.lease_start_date else "N/A",
                "end_date": active_lease.lease_end_date if active_lease.lease_end_date else "N/A",
                "amount": f"{lease_amount:,.2f}",
            }
        format_data["medallion"]= {
            "medallion_id": active_lease.medallion_id if active_lease.medallion_id else None,
            "medallion_number": active_lease.medallion.medallion_number if active_lease.medallion else "N/A",
            "medallion_owner": medallion_owner,
        }

        format_data["vehicle"] = {
            "vehicle_id": active_lease.vehicle_id if active_lease.vehicle_id else None,
            "plate_no": active_lease.vehicle.registrations[0].plate_number if active_lease.vehicle and active_lease.vehicle.registrations else "N/A",
            "vin": active_lease.vehicle.vin if active_lease.vehicle else "N/A",
            "vehicle": " ".join(filter(None , [active_lease.vehicle.make, active_lease.vehicle.model, active_lease.vehicle.year]))
        }
        
        format_data["driver"] = {
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
            object_id=violation.id,
            document_type="tlc_ticket"
        )

        other_tlc = upload_service.get_documents(
            db=db,
            object_type="tlc",
            object_id=violation.id,
            document_type="other_tlc"
        )   
        
        logger.info("Successfully fetched driver and lease details for TLC case", case_no=case_no, driver_id=driver.id)
        
        return {
            "data": format_data,
            "tlc_ticket": tlc_ticket,
            "other_tlc": other_tlc,
            "tlc_violation": violation_data,
            "violation_types": violation_tyeps,
            "service_fee": settings.tlc_service_fee
        }
        
    except Exception as e:
        logger.error("Error in TLC choose_driver_fetch: %s", e, exc_info=True)
        raise

@step(step_id="223", name="Process - Choose Driver", operation="process")
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

        case_entity = bpm_service.get_case_entity(db, case_no=case_no, entity_name=ENTITY_MAPPER["TLC"])

        tlc_service = TLCService(db)

        violation = None

        if case_entity:
            violation = db.query(TLCViolation).filter_by(id=int(case_entity.identifier_value)).first()

        
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
        
        if lease.id !=violation.lease_id:
            raise HTTPException(status_code=400, detail="Lease does not belong to the selected violation")
    
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
        

        if violation:
            ledger_repo = LedgerRepository(db)
            ledger_service = LedgerService(ledger_repo)

            new_disposition = step_data.get("disposition")
            old_disposition = violation.disposition

            if old_disposition == TLCDisposition.REDUCED.value and new_disposition == TLCDisposition.PAID.value:
                raise ValueError("TLC disposition cannot be changed from reduced to paid")

            if (
                old_disposition == TLCDisposition.DISMISSED.value
                and new_disposition in [TLCDisposition.PAID.value, TLCDisposition.REDUCED.value]
            ):
                raise ValueError("TLC disposition cannot be changed from dismissed to paid or reduced")
            
            total_payble = Decimal(step_data.get("driver_payable"))


            if step_data.get("disposition") == TLCDisposition.REDUCED.value:
                amount = violation.driver_payable - Decimal(step_data.get("driver_payable"))
                ledger_posting = ledger_service.create_obligation(
                    category=PostingCategory.TLC,
                    entry_type= EntryType.CREDIT if amount > 0 else EntryType.DEBIT,
                    amount= abs(amount) ,
                    reference_id= step_data.get("summons_number" , violation.summons_no),
                    driver_id=violation.driver_id,
                    lease_id=violation.lease_id,
                    medallion_id=violation.medallion_id,
                    vehicle_id=violation.vehicle_id
                )
            elif step_data.get("disposition") == TLCDisposition.DISMISSED.value:
                amount = abs(violation.driver_payable - settings.tlc_service_fee)
                total_payble = 0
                ledger_posting = ledger_service.create_obligation(
                    category=PostingCategory.TLC,
                    entry_type= EntryType.CREDIT if amount > 0 else EntryType.DEBIT,
                    amount= amount,
                    reference_id= step_data.get("summons_number" , violation.summons_no),
                    driver_id=violation.driver_id,
                    lease_id=violation.lease_id,
                    medallion_id=violation.medallion_id,
                    vehicle_id=violation.vehicle_id
                )
                

            violation.summons_no = step_data.get("summons_number")
            violation.issue_date = step_data.get("issue_date")
            violation.issue_time = datetime.now().time()
            violation.plate = vehicle_plate_no
            violation.violation_type = step_data.get("ticket_type")
            violation.description = step_data.get("description")
            violation.amount = Decimal(step_data.get("penalty_amount"))
            violation.service_fee = settings.tlc_service_fee
            violation.total_payable = Decimal(step_data.get("penalty_amount") + settings.tlc_service_fee)
            violation.driver_payable = total_payble
            violation.reduced_tlc_amount = step_data.get("reduced_by")
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
                "violation_type": step_data.get("ticket_type"),
                "description": step_data.get("description"),
                "amount": Decimal(step_data.get("penalty_amount")),
                "driver_payable": Decimal(step_data.get("driver_payable")),
                "disposition": step_data.get("disposition"),
                "due_date": step_data.get("due_date"),
                "note": step_data.get("note")
            }
            violation = tlc_service.create_manual_violation(case_no, tlc_data , 1)
            logger.info("TLC violation created successfully.")

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"TLC violation updated for case {case_no}",
                meta_data={"driver_id": driver_id, "lease_id": lease_id, "vehicle_id": vehicle_id, "medallion_id": medallion_id}
            )

        logger.info("TLC violation updated successfully.")
        return "Ok"
     except Exception as e:
        logger.error("Error in TLC choose_driver_process: %s", e, exc_info=True)
        raise