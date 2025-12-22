### app/entities/utils.py
from datetime import datetime

from app.entities.services import entity_service


def format_corporation_details(corporation , child_corporations = []):

    if not corporation:
        return {}
    
    primary_address_info = corporation.primary_address or None
    secondary_address_info = corporation.secondary_address or None
   
    return{
        "id":corporation.id,
        "name": corporation.name if corporation.name else None,
        "ein":corporation.ein if corporation.ein else None,
        "is_llc": corporation.is_llc,
        "is_holding_entity": corporation.is_holding_entity ,
        "no_of_child_entities": len(child_corporations) if child_corporations else 0,
        "child_entities": [
            {
                "id": child.id,
                "name": child.name,
                "ein": child.ein
            } for child in child_corporations
        ] if child_corporations else [],
        "holding_entity": corporation.linked_pad_owner_id or None,
        "contract_signed_mode": corporation.contract_signed_mode if corporation.contract_signed_mode else None,
        "registered_date": corporation.registered_date if corporation.registered_date else None,
        "primary_contact_number": corporation.primary_contact_number if corporation.primary_contact_number else None,
        "primary_email_address": corporation.primary_email_address if corporation.primary_email_address else None,
        "is_mailing_address_same": corporation.is_mailing_address_same,
        "primary_address": {
            "address_line_1": primary_address_info.address_line_1 if primary_address_info and primary_address_info.address_line_1 else None,
            "address_line_2": primary_address_info.address_line_2 if primary_address_info and primary_address_info.address_line_2 else None,
            "city": primary_address_info.city if primary_address_info and primary_address_info.city else None,
            "state": primary_address_info.state if primary_address_info and primary_address_info.state else None,
            "zip": primary_address_info.zip if primary_address_info and primary_address_info.zip else None
        },
        "secondary_address": {
            "address_line_1": secondary_address_info.address_line_1 if secondary_address_info and secondary_address_info.address_line_1 else None,
            "address_line_2": secondary_address_info.address_line_2 if secondary_address_info and secondary_address_info.address_line_2 else None,
            "city": secondary_address_info.city if secondary_address_info and secondary_address_info.city else None,
            "state": secondary_address_info.state if secondary_address_info and secondary_address_info.state else None,
            "zip": secondary_address_info.zip if secondary_address_info and secondary_address_info.zip else None
        },
        "benificial_owners": [
            {
                "owner_id": owner.id,
                "individual_owner_id": owner.owner_id,
                "owner_type": owner.owner_type,
                "owner_name": owner.name or None,
                "is_primary_contact": owner.is_primary_contact,
                "is_authorized_signatory": owner.is_authorized_signatory,
                "is_payee": owner.is_payee,
                "individual_data": owner.individual_owner.to_dict() if owner.individual_owner else {}
            } for owner in corporation.corporation_owners
        ] if corporation.corporation_owners else [],
        "payee_details": [
            {
                "pay_to_mode": payee.pay_to_mode if payee.pay_to_mode != "Check" else "Check",
                "sequence": payee.sequence,
                "payee_type": payee.payee_type,
                "allocation_percentage": payee.allocation_percentage,
                **(
                    {"individual_owner_id": payee.individual_id}
                    if payee.individual_id
                    else {"corporation_owner_id": payee.corporation_owner_id}
                ),
                "owner_details": {
                    "id": payee.individual_id if payee.individual_owner else payee.corporation_owner_id if payee.corporation_owner else None,
                    "name": payee.individual_owner.full_name if payee.individual_owner else payee.corporation_owner.name if payee.corporation_owner else "",
                    "type": "Individual" if payee.individual_owner else "Corporation" if payee.corporation_owner else None,
                    "ein or ssn" : payee.individual_owner.masked_ssn if payee.individual_owner else payee.corporation_owner.ein if payee.corporation_owner else ""
                },
                "data": {
                    "bank_name": payee.bank_account.bank_name if payee.bank_account else "",
                    "bank_routing_number": payee.bank_account.bank_routing_number if payee.bank_account else "",
                    "bank_account_number": payee.bank_account.bank_account_number if payee.bank_account else "",
                    "bank_account_name": payee.bank_account.bank_account_name if payee.bank_account else "",
                    "effective_from": payee.bank_account.effective_from if payee.bank_account else "",
                } if payee.pay_to_mode != "Check" else {
                    "bank_account_name": payee.payee or ""
                }
            } for payee in corporation.corporation_payees
        ] if corporation.corporation_payees else []
    }


def format_individual_details(individual):
    if not individual:
        return {}
    
    primary_address_info = individual.primary_address or None
    secondary_address_info = individual.secondary_address or None
    return {
        "individual_info" : {
                "first_name": individual.first_name or None,
                "middle_name": individual.middle_name or None,
                "last_name": individual.last_name or None,
                "full_name": individual.full_name or None,
                "ssn": individual.masked_ssn or None,
                "dob": datetime.strptime(individual.dob, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d") if individual.dob else None,
                "passport": individual.passport or None,
                "primary_email_address": individual.primary_email_address or None,
                "primary_contact_number": individual.primary_contact_number or None,
                "additional_phone_number_1": individual.additional_phone_number_1 or None,
                "additional_phone_number_2": individual.additional_phone_number_2 or None,
                "driving_license": individual.driving_license or None,
                "driving_license_expiry_date": individual.driving_license_expiry_date.strftime("%Y-%m-%d") if individual.driving_license_expiry_date else None,
                "correspondence_method": individual.correspondence_method or None,
            },
        "primary_address": {
            "address_line_1": primary_address_info.address_line_1 if primary_address_info and primary_address_info.address_line_1 else None,
            "address_line_2": primary_address_info.address_line_2 if primary_address_info and primary_address_info.address_line_2 else None,
            "city": primary_address_info.city if primary_address_info and primary_address_info.city else None,
            "state": primary_address_info.state if primary_address_info and primary_address_info.state else None,
            "zip": primary_address_info.zip if primary_address_info and primary_address_info.zip else None
        },
        "secondary_address": {
            "address_line_1": secondary_address_info.address_line_1 if secondary_address_info and secondary_address_info.address_line_1 else None,
            "address_line_2": secondary_address_info.address_line_2 if secondary_address_info and secondary_address_info.address_line_2 else None,
            "city": secondary_address_info.city if secondary_address_info and secondary_address_info.city else None,
            "state": secondary_address_info.state if secondary_address_info and secondary_address_info.state else None,
            "zip": secondary_address_info.zip if secondary_address_info and secondary_address_info.zip else None
        },
        "payee_info":{
            "pay_to_mode":"ACH" if individual.pay_to_mode != "Check" else "Check",
            "data": {
                "bank_name": individual.bank_account.bank_name if individual.bank_account and individual.bank_account.bank_name else None,
                "bank_account_number": individual.bank_account.bank_account_number if individual.bank_account and individual.bank_account.bank_account_number else None,
                "bank_account_name": individual.bank_account.bank_account_name if individual.bank_account and individual.bank_account.bank_account_name else None,
                "bank_routing_number" : individual.bank_account.bank_routing_number if individual.bank_account and individual.bank_account.bank_routing_number else None,
                "effective_from": individual.bank_account.effective_from if individual.bank_account and individual.bank_account.effective_from else None
            } if individual.pay_to_mode != "Check" else {"bank_account_name": individual.payee or None}
        }
    }

 
def formate_entity_details(entity):
    if not entity:
        return {}
    bank_details = {}
    return {
        "id": entity.id,
        "dos_id": entity.dos_id or "",
        "entity_name": entity.entity_name or "",
        "entity_address": entity.entity_address or {},
        "contact_person": entity.contact_person or {},
        "bank_details": bank_details,
        "ein": entity.ein_ssn or "",
        "is_corporation": True if entity.is_corporation else False,
        "registered_date": entity.registered_date or "",
        "contact_persion_primary_address": getattr(entity.contact_person, "primary_address", {}) if entity.contact_person else {},
        "contact_person_secondary_address": getattr(entity.contact_person, "secondary_address", {}) if entity.contact_person else {},
    }
