## app/bpm_flows/medallion_lease_terminate/flows.py

# Third party imports
from fastapi import HTTPException, status

# Local imports
from app.utils.logger import get_logger
from app.bpm.step_info import step
from app.audit_trail.services import audit_trail_service
from app.medallions.schemas import MedallionOwnerType, MedallionStatus
from app.vehicles.schemas import VehicleStatus
from app.bpm.services import bpm_service
from app.entities.services import entity_service
from app.medallions.services import medallion_service
from app.uploads.services import upload_service
from app.vehicles.services import vehicle_service
from app.bpm_flows.newmed.utils import format_medallion_basic_details

logger = get_logger(__name__)
entity_mapper = {
    "MEDALLION": "medallion",
    "MEDALLION_IDENTIFIER": "id",
}

@step(step_id="140", name="Fetch - Medallion Payee Details", operation='fetch')
def fetch_medallion_payee_detaills(db, case_no, case_params=None):
    """
    Fetch the medallion payee details for the update payee step
    """
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        medallion_owner_payee_info = {}
        medallion_info = ""

        if case_params:
            medallion_info = medallion_service.get_medallion(
                db, medallion_number=case_params['object_lookup']
            )

        if case_entity:
            medallion_info = medallion_service.get_medallion(
                db, medallion_id=int(case_entity.identifier_value)
            )

        if not medallion_info:
            return medallion_owner_payee_info

        medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)
        medallion_owner_payee_info.update(
            format_medallion_basic_details(medallion_info, medallion_owner)
        )
        medallion_owner_payee_info["medallion_payee_info"] = {
            "payee": medallion_info.pay_to if medallion_info.pay_to else "",
            "pay_to": "check"
        }

        bank_account = None
        if medallion_owner.medallion_owner_type == MedallionOwnerType.INDIVIDUAL:
            bank_account = medallion_owner.individual.bank_account
        elif medallion_owner.medallion_owner_type == MedallionOwnerType.CORPORATION:
            bank_account = medallion_owner.corporation.bank_account

        if not bank_account:
            return medallion_owner_payee_info

        medallion_owner_payee_info["medallion_payee_info"].update(
            {
                "bank_name": bank_account.bank_name if bank_account.bank_name else "",
                "bank_account_number": bank_account.bank_account_number if bank_account.bank_account_number else ""
            }
        )

        if bank_account.bank_address:
            b_a = bank_account.bank_address
            medallion_owner_payee_info['medallion_payee_info'][
                'address_line_1'] = b_a.address_line_1 if b_a.address_line_1 else ""
            medallion_owner_payee_info['medallion_payee_info'][
                'address_line_2'] = b_a.address_line_2 if b_a.address_line_2 else ""
            medallion_owner_payee_info['medallion_payee_info']['city'] = b_a.city if b_a.city else ""
            medallion_owner_payee_info['medallion_payee_info']['state'] = b_a.state if b_a.state else ""
            medallion_owner_payee_info['medallion_payee_info']['zip'] = b_a.zip if b_a.zip else ""
            medallion_owner_payee_info['medallion_payee_info'][
                'effective_from'] = bank_account.effective_from if bank_account.effective_from else ""
            medallion_owner_payee_info['medallion_payee_info']['pay_to'] = "ACH"

        # Create case entity if not present
        if not case_entity:
            case_entity = bpm_service.create_case_entity(
                db, case_no=case_no,
                entity_name=entity_mapper['MEDALLION'],
                identifier=entity_mapper['MEDALLION_IDENTIFIER'],
                identifier_value=str(medallion_info.id)
            )
        
        return medallion_owner_payee_info
    except Exception as e:
        logger.error("Error fetching medallion payee details: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e



@step(step_id="140", name="Process - Medallion Payee Details", operation='process')
def process_medallion_payee_details(db, case_no, step_data):
    """
    Process the medallion payee details for the update payee step
    """
    logger.info("Process - Medallion Payee Details")
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        logger.info("Creating/Updating Medallion Payee Details")

        medallion = medallion_service.get_medallion(
            db, medallion_number=step_data['medallion_number']
        )

        if medallion is None:
            return "Medallion not found"
            
        medallion_owner = medallion_service.get_medallion_owner(
            db, medallion_owner_id=medallion.owner_id
        )

        bank_account = None
        if medallion_owner.medallion_owner_type == MedallionOwnerType.INDIVIDUAL:
            bank_account = medallion_owner.individual.bank_account
        elif medallion_owner.medallion_owner_type == MedallionOwnerType.CORPORATION:
            bank_account = medallion_owner.corporation.bank_account

        if step_data['payto'] == "check":
            medallion_service.upsert_medallion(db, {
                "id": medallion.id,
                "pay_to": step_data['payee']
            })
            if bank_account:
                logger.info("Removing bank account %s", bank_account.id)
                entity_service.delete_bank_account(db, bank_account.id)
            else:
                logger.info("No bank account to dissociate")

        if step_data['payto'] == 'ACH':
            if bank_account:
                bank_account = entity_service.upsert_bank_account(
                    db, {
                        "id": bank_account.id,
                        **step_data
                    }
                )
            else:
                bank_account = entity_service.upsert_bank_account(db, step_data)

            # Associate new bank account with the owner
            if medallion_owner.individual:
                logger.info("Associating bank account with individual")
                entity_service.upsert_individual(db, {
                    "id": medallion_owner.individual.id,
                    "bank_account_id": bank_account.id
                })

            if medallion_owner.corporation:
                logger.info("Associating bank account with corporation")
                entity_service.upsert_corporation(db, {
                    "id": medallion_owner.corporation.id,
                    "bank_account_id": bank_account.id
                })

        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Updated medallion payee details for medallion {medallion.medallion_number}",
                meta_data={"medallion_id": medallion.id}
            )

        return "Ok"
    except Exception as e:
        logger.error("Error processing medallion payee details: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e


@step(step_id="141", name="Fetch - medallion payee documents", operation='fetch')
def fetch_medallion_payee_documents(db , case_no, case_params=None):
    """
    Fetch the medallion payee documents for the update payee step
    """
    logger.info("Fetch - Medallion Payee Documents")
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        medallion_owner_payee_info = {}
        medallion_info = ""

        if case_params:
            medallion_info = medallion_service.get_medallion(
                db, medallion_number=case_params['object_lookup']
            )
        if case_entity:
            medallion_info = medallion_service.get_medallion(
                db, medallion_id=int(case_entity.identifier_value)
            )

        if not medallion_info:
            return medallion_owner_payee_info

        medallion_owner = medallion_service.get_medallion_owner(
            db, medallion_owner_id=medallion_info.owner_id
        )
        medallion_owner_payee_info.update(
            format_medallion_basic_details(medallion_info, medallion_owner)
        )

        medallion_owner_payee_info["object_type"] = "medallion"
        medallion_owner_payee_info["document_type"] = "medallion_payee_proof"

        medallion_owner_payee_info["medallion_owner_payee_proofs"] = upload_service.get_documents(
            db, object_type="medallion",
            object_id=str(medallion_info.id),
            document_type="medallion_payee_proof"
        )

        return medallion_owner_payee_info
    except Exception as e:
        logger.error("Error fetching medallion payee documents: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e


@step(step_id="141", name="Process - terminate medallion lease", operation='process')
def process_terminate_medallion_lease(db , case_no , stepdata):
    """
    Process the terminate medallion lease 
    """
    logger.info("Process - Medallion lease termination")
    try:
        case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        medallion = None

        if case_entity:
            medallion = medallion_service.get_medallion(
                db, medallion_id=int(case_entity.identifier_value)
            )

        if not medallion:
            return "Medallion not found"
        if medallion.medallion_status == MedallionStatus.ARCHIVED:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Medallion is already terminated")
        
        if medallion.medallion_status not in [MedallionStatus.ASSIGNED_TO_VEHICLE, MedallionStatus.AVAILABLE]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Medallion is not available to terminate lease"
            )
        
        vehicle = vehicle_service.get_vehicles(db, medallion_id=medallion.id)
        vehicle_service.upsert_vehicle(db, {
            "id": vehicle.id,
            "vehicle_status": VehicleStatus.AVAILABLE,
            "medallion_id": None,
            "is_medallion_assigned": False
        })

        medallion_service.upsert_medallion(db, {
            "id": medallion.id,
            "medallion_status": MedallionStatus.ARCHIVED,
            "is_active": False
        })
        
        mo_lease = medallion_service.get_mo_lease(db, mo_lease_id=medallion.mo_lease_id)
        case = bpm_service.get_cases(db=db , case_no= case_no)
        if case:
            audit_trail_service.create_audit_trail(
                db=db,
                case=case,
                description=f"Terminated medallion lease for medallion {medallion.medallion_number}",
                meta_data={"medallion_id": medallion.id}
            )

        medallion_service.upsert_mo_lease(db, {
            "id": mo_lease.id,
            "is_active": False
        })
        
        return "Ok"
    except Exception as e:
        logger.error("Error processing terminate medallion lease: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e
