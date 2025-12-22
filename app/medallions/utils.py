### app/medallions/utils.py

from app.medallions.schemas import MedallionStatus , MedallionStatusCheck
import math
from app.uploads.services import upload_service


def extract_medallion_info(medallion):
    """
    Extract medallion information with null safety for document generation.

    Args:
        medallion: Medallion object

    Returns:
        Dictionary with medallion information
    """
    if not medallion:
        return {
            "number": "N/A",
        }

    return {
        "number": medallion.medallion_number if medallion.medallion_number else "N/A",
    }


def format_medallion_owner_response(db , medallion_owner):
    """Format the medallion owner response"""
    medallions = medallion_owner.medallions or []
    documents = upload_service.get_documents(db=db , object_id=medallion_owner.id , object_type="medallion_owner" , multiple=True) or []
    base_data = {
        "medallion_owner_id": medallion_owner.id,
        "contact_number": medallion_owner.primary_phone if medallion_owner.primary_phone else "",
        "email_address": medallion_owner.primary_email_address if medallion_owner.primary_email_address else "",
        "created_on": medallion_owner.created_on,
        "additional_info": {
            "medallions": [{
                "medallion_number": medallion.medallion_number,
            } for medallion in medallions if medallion.medallion_number and medallion.medallion_status != MedallionStatus.IN_PROGRESS]
        }
    }

    owner = {
    }

    if medallion_owner.individual:
        individual = medallion_owner.individual
        documents.extend(upload_service.get_documents(db=db, object_id=individual.id, object_type="individual_owner", multiple=True) or [])
        base_data["is_documents"] = bool(documents)
        owner = {
            "entity_type": "individual",
            "entity_name": individual.full_name,
            "ssn": f"XXX-XX-{individual.masked_ssn[-4:]}" if individual.masked_ssn else "",
            "ein": "",
            "owner_name": individual.full_name,
            "address": individual.primary_address.address_line_1 if individual and individual.primary_address else "",
            **base_data
        }
    elif medallion_owner.corporation:
        corporation = medallion_owner.corporation
        documents.extend(upload_service.get_documents(db=db, object_id=corporation.id, object_type="corporation", multiple=True) or [])
        base_data["is_documents"] = bool(documents)
        owner = {
            "entity_type": "corporation",
            "entity_name": corporation.name,
            "ssn": "",
            "ein": corporation.ein,
            "owner_name": corporation.name,
            "address": corporation.primary_address.address_line_1 if corporation and corporation.primary_address else "",
            "is_holding_entity": corporation.is_holding_entity,
            "is_llc": corporation.is_llc,
            "parent_corporation_id": corporation.linked_pad_owner.id if corporation.linked_pad_owner else "",
            "parent_corporation_name": corporation.linked_pad_owner.name if corporation.linked_pad_owner else "", 
            **base_data
        }

    return owner
    
def format_medallion_response(medallion, has_documents: bool = False, in_storage: bool = False, has_audit_trail: bool = False) -> dict:
    """Helper function to format medallion response"""
    medallion_lease = medallion.lease
    driver_lease= None
    
    if not medallion_lease and medallion.medallion_status == "Y":
        driver_lease = False
    elif medallion_lease and medallion.medallion_status == "Y" and medallion_lease[0].lease_status != "Active":
        driver_lease = False
    elif medallion_lease and medallion_lease[0].lease_status == "Active":
        driver_lease= True
    else:
        driver_lease = None

    owner_name = "Unknown"
    owner_address = {
        "address_line_1": "",
        "address_line_2": "",
        "city": "",
        "state": "",
        "zip": ""
    }
    secondary_address = {}
    entity_name = ""
    primary_email_address = ""
    primary_contact_number = ""
    secondary_contact_number = ""
    medallion_ssn = ""
    medallion_passport = ""
    if medallion.owner:
        if medallion.owner.medallion_owner_type == "I" and medallion.owner.individual:
            owner_name = f"{medallion.owner.individual.first_name} {medallion.owner.individual.last_name}"
            primary_email_address = medallion.owner.individual.primary_email_address if medallion.owner.individual.primary_email_address else ""
            primary_contact_number = medallion.owner.individual.primary_contact_number if medallion.owner.individual.primary_contact_number else ""
            secondary_contact_number = medallion.owner.individual.additional_phone_number_1 if medallion.owner.individual.additional_phone_number_1 else ""
            medallion_ssn = medallion.owner.individual.masked_ssn
            medallion_passport = medallion.owner.individual.passport if medallion.owner.individual.passport else ""
            owner_address = medallion.owner.individual.primary_address.to_dict() if medallion.owner.individual.primary_address else owner_address
            secondary_address = medallion.owner.individual.secondary_address.to_dict() if medallion.owner.individual.secondary_address else {}

        elif medallion.owner.medallion_owner_type == "C" and medallion.owner.corporation:
            owner_name = medallion.owner.corporation.name
            primary_email_address = medallion.owner.corporation.primary_email_address if medallion.owner.corporation.primary_email_address else ""
            primary_contact_number = medallion.owner.corporation.primary_contact_number if medallion.owner.corporation.primary_contact_number else ""
            secondary_contact_number = ""
            medallion_ssn = medallion.owner.corporation.ein
            medallion_passport = ""
            owner_address = medallion.owner.corporation.primary_address.to_dict() if medallion.owner.corporation.primary_address else owner_address
            secondary_address = medallion.owner.corporation.secondary_address.to_dict() if medallion.owner.corporation.secondary_address else {}

            corporation = medallion.owner.corporation

    return {
        "medallion_id": medallion.id,
        "medallion_number": medallion.medallion_number,
        "renewal_date": medallion.medallion_renewal_date,
        "ssn": medallion.owner.individual.masked_ssn if medallion.owner and medallion.owner.individual else None,
        "procurement_type": "lease",
        "agent_name": "Big Apple Taxi",
        "contract_start_data": medallion.mo_lease.contract_start_date if medallion.mo_lease else None,
        "contract_end_date": medallion.mo_lease.contract_end_date if medallion.mo_lease else None,
        "hack_indicator": True,
        "medallion_owner": owner_name,
        "medallion_status": medallion.medallion_status,
        "medallion_type": medallion.medallion_type,
        "validity_end_date": medallion.medallion_renewal_date,
        "lease_expiry_date": medallion.mo_lease.contract_end_date if medallion.mo_lease else None,
        "in_storage": in_storage,
        "does_medallion_have_documents": has_documents,
        "vehicle": bool(medallion.vehicle),
        "driver_lease": driver_lease,
        "audit_trial": has_audit_trail,
        "lease_due_on": None,
        "owner_address": owner_address,
        "secondary_address": secondary_address,
        "primary_contact_number":  primary_contact_number,
        "secondary_contact_number": secondary_contact_number,
        "primary_email_address": primary_email_address,
        "medallion_ssn": medallion_ssn,
        "medallion_passport": medallion_passport,
        "entity_name": "",
        "created_on": medallion.created_on,
        "updated_on": medallion.updated_on
    }
 
def format_medallon_owner(medalion_owner):
    """Format Medallion Owner"""
    if not medalion_owner:
        return {}
    owner_details = {
        "id": medalion_owner.id,
        "is_mailing_address_same": medalion_owner.is_mailing_address_same,
        "medallions": [{"medallion_number": medallion.medallion_number} for medallion in medalion_owner.medallions if medallion.medallion_number] if medalion_owner.medallions else []
    }

    if medalion_owner.medallion_owner_type == "I" and medalion_owner.individual:
        owner_details.update({
            "owner_type": "individual",
            "look_up_id": medalion_owner.individual.id or None,
            "owner_name": medalion_owner.individual.full_name if medalion_owner.individual else "",
            "ssn": medalion_owner.individual.masked_ssn if medalion_owner.individual else None,
            "primary_contact_number": medalion_owner.individual.primary_contact_number if medalion_owner.individual else None,
            "primary_email_address": medalion_owner.individual.primary_email_address if medalion_owner.individual else None,
            "additional_phone_number_1": medalion_owner.individual.additional_phone_number_1 if medalion_owner.individual else None,
            "additional_phone_number_2": medalion_owner.individual.additional_phone_number_2 if medalion_owner.individual else None,
            "dob": medalion_owner.individual.dob if medalion_owner.individual else None,
            "passport": medalion_owner.individual.passport if medalion_owner.individual else None,
            "passport_expiry_date": medalion_owner.individual.passport_expiry_date if medalion_owner.individual else None,
            "driving_license": medalion_owner.individual.driving_license if medalion_owner.individual else None,
            "driving_license_expiry_date": medalion_owner.individual.driving_license_expiry_date if medalion_owner.individual else None,
            "payee_details": {
                "pay_to_mode": "ACH" if (medalion_owner.individual.pay_to_mode) != "Check" else "Check",
                "data":{
                    "id": medalion_owner.individual.bank_account.id if medalion_owner.individual.bank_account else None,
                    "bank_name": medalion_owner.individual.bank_account.bank_name if medalion_owner.individual.bank_account and medalion_owner.individual.bank_account.bank_name else None,
                    "bank_account_number": medalion_owner.individual.bank_account.bank_account_number if medalion_owner.individual.bank_account and medalion_owner.individual.bank_account.bank_account_number else None,
                    "bank_account_name": medalion_owner.individual.bank_account.bank_account_name if medalion_owner.individual.bank_account and medalion_owner.individual.bank_account.bank_account_name else None,
                    "bank_routing_number" : medalion_owner.individual.bank_account.bank_routing_number if medalion_owner.individual.bank_account and medalion_owner.individual.bank_account.bank_routing_number else None,
                    "effective_from": medalion_owner.individual.bank_account.effective_from if medalion_owner.individual.bank_account and medalion_owner.individual.bank_account.effective_from else None
                } if medalion_owner.individual.pay_to_mode != "Check" else {"bank_account_name": medalion_owner.individual.payee or None}
            },
            "primary_address" : medalion_owner.individual.primary_address if medalion_owner.individual else None,
            "secondary_address": medalion_owner.individual.secondary_address if medalion_owner.individual else None
        })
    elif medalion_owner.medallion_owner_type == "C" and medalion_owner.corporation:
        owner_details.update({
            "owner_type": "corporation",
            "look_up_id": medalion_owner.corporation.id or None,
            "owner_name": medalion_owner.corporation.name if medalion_owner.corporation else "",
            "ein": medalion_owner.corporation.ein if medalion_owner.corporation else None,
            "is_llc": medalion_owner.corporation.is_llc if medalion_owner.corporation else None,
            "primary_contact_number": medalion_owner.corporation.primary_contact_number if medalion_owner.corporation else None,
            "primary_email_address": medalion_owner.corporation.primary_email_address if medalion_owner.corporation else None,
            "payee_details": [
                {
                    "pay_to_mode": "ACH" if payee.pay_to_mode != "Check" else "Check",
                    "data": {
                        "bank_name": payee.bank_account.bank_name if payee.bank_account else "",
                        "bank_routing_number": payee.bank_account.bank_routing_number if payee.bank_account else "",
                        "bank_account_number": payee.bank_account.bank_account_number if payee.bank_account else "",
                        "bank_account_name": payee.bank_account.bank_account_name if payee.bank_account else "",
                        "effective_from": payee.bank_account.effective_from if payee.bank_account else "",
                    } if payee.pay_to_mode != "Check" else {
                        "bank_account_name": payee.payee or ""
                    }
                } for payee in medalion_owner.corporation.corporation_payees
            ] if medalion_owner.corporation and medalion_owner.corporation.corporation_payees else [],
            "primary_address": medalion_owner.corporation.primary_address if medalion_owner.corporation else None,
            "secondary_address": medalion_owner.corporation.secondary_address if medalion_owner.corporation else None
        })
    
    return owner_details
    
def get_medallions_list_owner(medallion_owner , page= 1, per_page = 10):
    """
    List of Of Medallions Own By One Owner
    """

    page = int(page)     # default to page 1
    limit = int(per_page)  # default to 10 items per page

    # Calculate start and end index
    start = (page - 1) * limit
    end = start + limit

    # Slice the medallions
    paginated_medallions = [
        {
            "id": medallion.id,
            "medallion_number": medallion.medallion_number,
            "medallion_type": medallion.medallion_type,
            "medallion_status": MedallionStatusCheck[medallion.medallion_status].value,
            "end_date": medallion.validity_end_date,
            "expiry_date": medallion.mo_lease.contract_end_date if medallion.mo_lease and medallion.mo_lease.contract_end_date else None,
        }
        for medallion in medallion_owner.medallions[start:end]
    ]

    # Total count (optional)
    total_count = len(medallion_owner.medallions)

    # Optional response
    return {
        "items": paginated_medallions,
        "page": page,
        "per_page": limit,
        "total_count": total_count,
        "total_pages" : math.ceil(total_count / limit)
    }
