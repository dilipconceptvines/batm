## app/bpm_flows/newmed/utils.py

# Standard library imports
from datetime import datetime

from app.utils.logger import get_logger

logger = get_logger(__name__)


def format_individual_info(medallion_owner, medallion_info):
    """Format the individual owner information"""
    individual_medallion_info = {}
    if medallion_owner.individual:
        ind = medallion_owner.individual
        individual_medallion_info = {
            "firstName": ind.first_name,
            "middleName": ind.middle_name if ind.middle_name else "",
            "lastName": ind.last_name,
            "ssn": ind.masked_ssn,
            "dob": ind.dob,
            "selectDocument": "",
            "drivingLicenseNo": "",  # TODO: Confirm field
            "drivingLicenseExpiryDate": "",  # TODO: Confirm field
            "passportNo": ind.passport,
            "passport_expiry_date": ind.passport_expiry_date,
        }

        primary_address = ind.primary_address
        address_info = {}
        if primary_address:
            address_info["primaryAddress1"] = (
                primary_address.address_line_1 if primary_address.address_line_1 else ""
            )
            address_info["primaryAddress2"] = (
                primary_address.address_line_2 if primary_address.address_line_2 else ""
            )
            address_info["primaryCity"] = (
                primary_address.city if primary_address.city else ""
            )
            address_info["primaryState"] = (
                primary_address.state if primary_address.state else ""
            )
            address_info["primaryZip"] = (
                primary_address.zip if primary_address.zip else 0
            )
            address_info["primaryLatitude"] = (
                primary_address.latitude if primary_address.latitude else ""
            )
            address_info["primaryLongitude"] = (
                primary_address.longitude if primary_address.longitude else ""
            )

        individual_medallion_info.update(address_info)

        secondary_address = ind.secondary_address
        address_info = {}
        if secondary_address:
            address_info["secondaryAddress1"] = (
                secondary_address.address_line_1
                if secondary_address.address_line_1
                else ""
            )
            address_info["secondaryAddress2"] = (
                secondary_address.address_line_2
                if secondary_address.address_line_2
                else ""
            )
            address_info["secondaryCity"] = (
                secondary_address.city if secondary_address.city else ""
            )
            address_info["secondaryState"] = (
                secondary_address.state if secondary_address.state else ""
            )
            address_info["secondaryZip"] = (
                secondary_address.zip if secondary_address.zip else 0
            )
            address_info["secondaryLatitude"] = (
                secondary_address.latitude if secondary_address.latitude else ""
            )
            address_info["secondaryLongitude"] = (
                secondary_address.longitude if secondary_address.longitude else ""
            )

        individual_medallion_info.update(address_info)

        individual_medallion_info["payTo"] = (
            medallion_info.pay_to if medallion_info.pay_to else ""
        )
        # TODO: Confirm field information
        individual_medallion_info["payeeName"] = ""
        # TODO: Confirm field information
        individual_medallion_info["payee"] = ""

        bank_account = ind.bank_account
        bank_info = {}
        if bank_account:
            bank_info["bankName"] = (
                bank_account.bank_name if bank_account.bank_name else ""
            )
            bank_info["bankAccountNumber"] = (
                bank_account.bank_account_number
                if bank_account.bank_account_number
                else ""
            )

            # TODO: ConfirmField
            bank_info["effectiveFrom"] = ""

            if bank_account.bank_address:
                bank_address = bank_account.bank_address
                bank_info["bankAddress1"] = (
                    bank_address.address_line_1 if bank_address.address_line_1 else ""
                )
                bank_info["bankAddress2"] = (
                    bank_address.address_line_2 if bank_address.address_line_2 else ""
                )
                bank_info["bankCity"] = bank_address.city if bank_address.city else ""
                bank_info["bankState"] = (
                    bank_address.state if bank_address.state else ""
                )
                bank_info["bankZip"] = bank_address.zip if bank_address.zip else 0

        individual_medallion_info.update(bank_info)

        individual_medallion_info["primaryContactNumber"] = (
            ind.primary_contact_number if ind.primary_contact_number else ""
        )
        individual_medallion_info["additionalPhone1"] = (
            ind.additional_phone_number_1 if ind.additional_phone_number_1 else ""
        )
        individual_medallion_info["additionalPhone2"] = (
            ind.additional_phone_number_2 if ind.additional_phone_number_2 else ""
        )
        individual_medallion_info["primaryEmailAddress"] = (
            ind.primary_email_address if ind.primary_email_address else ""
        )

        return {"type": "individual", "details": individual_medallion_info}


def format_corporation_info(medallion_owner, medallion_info):
    """Format the corporation owner information"""
    corporate_medallion_info = {}
    if medallion_owner.corporation:
        corp = medallion_owner.corporation
        corporate_medallion_info = {
            "corporationName": corp.name,
            "ein": corp.ein if corp.ein else "",
            "primaryTelephoneNumber": corp.primary_contact_number
            if corp.primary_contact_number
            else "",
            "secondaryTelephoneNumber": "",  # TODO: Confirm this field
            "emailAddress": corp.primary_email_address
            if corp.primary_email_address
            else "",
        }

        primary_address = corp.primary_address
        address_info = {}
        if primary_address:
            address_info["addressLine1"] = (
                primary_address.address_line_1 if primary_address.address_line_1 else ""
            )
            address_info["addressLine2"] = (
                primary_address.address_line_2 if primary_address.address_line_2 else ""
            )
            address_info["city"] = primary_address.city if primary_address.city else ""
            address_info["state"] = (
                primary_address.state if primary_address.state else ""
            )
            address_info["zip"] = primary_address.zip if primary_address.zip else 0
            address_info["latitude"] = (
                primary_address.latitude if primary_address.latitude else ""
            )
            address_info["longitude"] = (
                primary_address.longitude if primary_address.longitude else ""
            )

        corporate_medallion_info.update(address_info)

        # TODO: Confirm field
        corporate_medallion_info["secondaryTelephoneNumber"] = ""
        corporate_medallion_info["primary_email_address"] = (
            corp.primary_email_address if corp.primary_email_address else ""
        )
        corporate_medallion_info["accountId"] = ""  # TODO: Confirm field
        corporate_medallion_info["lls"] = corp.is_llc if corp.is_llc else False
        corporate_medallion_info["member"] = corp.member if corp.member else 0
        corporate_medallion_info["manager"] = 0  # TODO: Confirm field
        # TODO: Confirm Field
        corporate_medallion_info["corporateOfficers"] = 0
        # TODO: Confirm Field
        corporate_medallion_info["ownerShip"] = "individual"

        corporate_medallion_info["entity"] = ""
        if corp.entity:
            corporate_medallion_info["entity"] = (
                corp.entity.entity_name if corp.entity.entity_name else ""
            )

        # TODO: Not sure about the following fields
        corporate_medallion_info["individual1"] = ""
        corporate_medallion_info["individual2"] = ""

        corporate_medallion_info["firstName"] = ""
        corporate_medallion_info["middleName"] = ""
        corporate_medallion_info["ssn"] = ""
        corporate_medallion_info["dob"] = ""
        corporate_medallion_info["selectDocument"] = ""
        corporate_medallion_info["drivingLicenseNo"] = ""
        corporate_medallion_info["drivingLicenseExpiryDate"] = ""
        corporate_medallion_info["passportNo"] = ""
        corporate_medallion_info["passportExpiryDate"] = ""
        corporate_medallion_info["payTo"] = ""
        corporate_medallion_info["payName"] = ""
        corporate_medallion_info["bankName"] = ""
        corporate_medallion_info["bankAccountNumber"] = ""
        corporate_medallion_info["payee"] = ""
        corporate_medallion_info["bankAddress1"] = ""
        corporate_medallion_info["bankAddress2"] = ""
        corporate_medallion_info["bankCity"] = ""
        corporate_medallion_info["bankState"] = ""
        corporate_medallion_info["bankZip"] = ""
        corporate_medallion_info["effectiveFrom"] = ""
        corporate_medallion_info["provider"] = ""
        corporate_medallion_info["broker"] = ""
        corporate_medallion_info["policy"] = ""
        corporate_medallion_info["amount"] = 0
        corporate_medallion_info["begins"] = ""
        corporate_medallion_info["ends"] = ""
        corporate_medallion_info["ledgerId"] = ""
        corporate_medallion_info["contractStartDate"] = ""
        corporate_medallion_info["contractEndDate"] = ""
        corporate_medallion_info["FirstSignedDate"] = ""

        return {"type": "corporation", "details": corporate_medallion_info}


def format_medallion_basic_details(medallion, medallion_owner):
    """Format the medallion basic details"""
    medallion_owner_name = ""
    primary_email_address = ""
    primary_contact_number = ""
    entity_name = ""
    medallion_ssn = ""
    medallion_passport = ""
    owner_type = ""
    owner_address = ""
    ssn = ""
    ein = ""

    medallion_lease = medallion.mo_lease.to_dict() if medallion.mo_lease else {}

    if not medallion_owner:
        return {}

    if medallion_owner.individual:
        medallion_owner_name = f"{medallion_owner.individual.first_name} {medallion_owner.individual.last_name}"
        primary_email_address = medallion_owner.individual.primary_email_address
        primary_contact_number = medallion_owner.individual.primary_contact_number
        medallion_ssn = f"XXX-XX-{medallion_owner.individual.masked_ssn[-4:]}" if medallion_owner.individual.masked_ssn else None
        medallion_passport = medallion_owner.individual.passport
        owner_type = "Individual"
        owner_address = medallion_owner.individual.primary_address.to_dict()
        ssn = f"XXX-XX-{medallion_owner.individual.masked_ssn[-4:]}" if medallion_owner.individual.masked_ssn else None


    if medallion_owner.corporation:
        medallion_owner_name = medallion_owner.corporation.name
        primary_email_address = medallion_owner.corporation.primary_email_address
        primary_contact_number = medallion_owner.corporation.primary_contact_number or None
        medallion_ssn = medallion_owner.corporation.ein or None
        owner_type = "Corporation"
        owner_address = medallion_owner.corporation.primary_address.to_dict() if medallion_owner.corporation.primary_address else {}
        ein = medallion_owner.corporation.ein or None

    medallion_details = {
        "medallion_id": medallion.id,
        "medallion_owner_name": medallion_owner_name,
        "medallion_number": medallion.medallion_number,
        "last_renewal_date": medallion.validity_end_date,
        "medallion_type": medallion.medallion_type,
        "primary_contact_nember": primary_contact_number,
        "primary_email_address": primary_email_address,
        "medallion_ssn": medallion_ssn,
        "medallion_passport": medallion_passport,
        "entity_name": "",
        "owner_type": owner_type,
        "owner_address": owner_address,
        "ssn": ssn,
        "ein": ein,
        "medallion_lease": {
            "id": medallion_lease.get("id" , None),
            "contract_start_date": medallion_lease.get("contract_start_date" , None),
            "contract_end_date": medallion_lease.get("contract_end_date" , None),
            "royalty_amount": medallion_lease.get("royalty_amount" , None),
            "contract_term": medallion_lease.get("contract_term" , None),
            "contract_signed_mode": medallion_lease.get("contract_signed_mode" , None),
            "mail_sent_date": medallion_lease.get("mail_sent_date" , None),
            "mail_received_date": medallion_lease.get("mail_received_date" , None),
            "lease_signed_flag": medallion_lease.get("lease_signed_flag" , None),
            "lease_signed_date": medallion_lease.get("lease_signed_date" , None),
            "in_house_lease": medallion_lease.get("in_house_lease" , None),
            "med_active_exemption": medallion_lease.get("med_active_exemption" , None),
        }
    }

    return medallion_details


def format_medallion_lease(medallion):
    """Format the medallion lease"""
    result = {}

    result["medallionId"] = medallion.id
    result["medallion_number"] = medallion.medallion_number
    result["medallion_type"] = medallion.medallion_type
    result["medallion_status"] = medallion.medallion_status

    # Fetch Lease details and expand them
    if medallion.mo_lease:
        lease = medallion.mo_lease
        result["leaseId"] = lease.id
        result["payee"] = lease.payee if lease.payee else ""
        result["back_id"] = lease.bank_account_id if lease.bank_account_id else ""
        result["contract_start_date"] = (
            lease.contract_start_date.strftime("%Y-%m-%d")
            if lease.contract_start_date
            else None
        )
        result["contract_end_date"] = (
            lease.contract_end_date.strftime("%Y-%m-%d")
            if lease.contract_end_date
            else None
        )
        result["royalty_amount"] = lease.royalty_amount if lease.royalty_amount else 0
        result["contract_signed_mode"] = lease.contract_signed_mode
        result["mail_sent_date"] = (
            lease.mail_sent_date.strftime("%Y-%m-%d") if lease.mail_sent_date else None
        )
        result["mail_received_date"] = (
            lease.mail_received_date.strftime("%Y-%m-%d")
            if lease.mail_received_date
            else None
        )
        result["lease_signed_flag"] = (
            lease.lease_signed_flag if lease.lease_signed_flag else False
        )
        result["lease_signed_date"] = (
            lease.lease_signed_date.strftime("%Y-%m-%d")
            if lease.lease_signed_date
            else None
        )
        result["in_house_lease"] = (
            lease.in_house_lease if lease.in_house_lease else False
        )
        result["med_active_exemption"] = (
            lease.med_active_exemption if lease.med_active_exemption else False
        )
        result["contract_term"] = lease.contract_term if lease.contract_term else ""

    return result


def prepare_medallion_designation_document(medallion, medallion_owner,lease):
    """Prepare the medallion designation document"""
    logger.info("Medallion owner info -*-*- %s", medallion_owner)
    med_agent_designation_template = {
        "medallion_owner_email": medallion_owner["primary_email_address"] or "",
        "medallion_owner_primary_address_line_1": medallion_owner["owner_address"]["address_line_1"] or "",
        "medallion_owner_primary_state": medallion_owner["owner_address"]["state"] or "",
        "medallion_owner_primary_zip": medallion_owner["owner_address"]["zip"] or "",
        "medallion_owner_secondary_address_line_1": medallion_owner.get("secondary_address", {}).get("address_line_1", ""),
        "medallion_owner_secondary_state": medallion_owner.get("secondary_address", {}).get("state", ""),
        "medallion_owner_secondary_zip": medallion_owner.get("secondary_address", {}).get("zip", ""),
        "medallion_owner_phone_number": medallion_owner["primary_contact_number"] or "",
        "medallion_owner_ein": medallion_owner["medallion_ssn"] or "",
        "medallion_number_1": medallion_owner["medallion_number"] or "",
        "medallion_number_2": "",
        "medallion_number_3": "",
        "medallion_number_4": "",
        "medallion_number_5": "",
        "medallion_number_6": "",
        "medallion_number_7": "",
        "medallion_number_8": "",
        "medallion_number_9": "",
        "medallion_number_10": "",
        "medallion_number_11": "",
        "medallion_number_12": "",
        "medallion_number_13": "",
        "medallion_number_14": "",
        "medallion_number_15": "",
        "medallion_number_16": "",
        "medallion_number_17": "",
        "medallion_number_18": "",
        "medallion_number_19": "",
        "medallion_number_20": "",
        "medallion_owner_name": medallion_owner["medallion_owner"] or "",
        "agent_designating": "Allen",
        "/owner_sign/": "",
        "/agent_sign/": ""
    }

    return med_agent_designation_template


def prepare_medallion_royalty_corp_llc_document(medallion, medallion_owner, lease):
    """Prepare the medallion royalty corp llc document"""
    return {
        "date_of_agreement": datetime.now().strftime("%m-%d-%Y"),
        "tax_id_no": "",
        "medallion_number": medallion_owner["medallion_number"],
        "medallion_owner_primary_address_1": medallion_owner["owner_address"]["address_line_1"] or "",
        "medallion_owner_secondary_address_1": (
            medallion_owner.get("secondary_address", {}).get("address_line_1") or ""
        ),
        "medallion_owner_primary_address_2": medallion_owner["owner_address"].get("address_line_2") or "",
        "medallion_owner_secondary_address_2": (
            medallion_owner.get("secondary_address", {}).get("address_line_2") or ""
        ),
        "medallion_owner_primary_phon": medallion_owner.get("primary_contact_number") or "",
        "medallion_owner_primary_phone_secondary": medallion_owner.get("secondary_contact_number") or "",
        "medallion_owner_secondary_phone": "",
        "medallion_owner_secondary_phone_secondary": "",
        "medallion_owner_email": medallion_owner.get("primary_email_address") or "",
        "contract_start_date": lease.contract_start_date.strftime("%m/%d/%Y") or "",
        "contract_end_date": lease.contract_end_date.strftime("%m/%d/%Y") or "",
        "medallion_owner_name": medallion_owner.get("medallion_owner") or "",
        "/agent_sign/": "",
        "/owner_sign/": "",
    }


def prepare_medallion_royalty_corp_document(medallion, medallion_owner, lease):
    """Prepare the medallion royalty corp document"""
    return {
        "date_of_agreement": datetime.now().strftime("%m/%d/%Y"),
        "tax_id_no": "",
        "licensor_name": medallion_owner["medallion_owner"] or "",
        "medallion_number": medallion_owner["medallion_number"] or "",
        "president_address_line_1": medallion_owner["owner_address"]["address_line_1"] or "",
        "secretary_address_line_1": "",
        "president_address_line_2": medallion_owner["owner_address"]["address_line_2"] or "",
        "secondary_address_line_2": "",
        "president_primary_phone": medallion_owner["primary_contact_number"] or "",
        "secretarty_primary_phone": "",
        "president_seconday_phone": "",
        "secretary_secondary_phone": "",
        "president_email": medallion_owner["primary_email_address"] or ""   ,
        "secretary_email": "",
        "contract_start_date": lease.contract_start_date.strftime("%m/%d/%Y") or "",
        "contract_end_date": lease.contract_end_date.strftime("%m/%d/%Y") or "",
        "medallion_owner_name": medallion_owner["medallion_owner"] or "",
        "/owner_sign/": "",
        "/agent_sign/": ""
    }


def prepare_medallion_royalty_individual_document(medallion, medallion_owner, lease):
    """Prepare the medallion royalty individual document"""
    print("-*-*- Medallion owner: %s", medallion_owner)
    return {
        "date_of_agreement": str(datetime.now().strftime("%m-%d-%Y")),
        "medallion_owner_ssn": medallion_owner["medallion_ssn"] or "",
        "medallion_number": medallion_owner["medallion_number"] or "",
        "medallion_owner_address_line_1": medallion_owner["owner_address"]["address_line_1"] or "",
        "medallion_owner_primary_phone": medallion_owner["primary_contact_number"] or "",
        "medallion_owner_address_line_2": medallion_owner["owner_address"]["address_line_2"] or "",
        "medallion_owner_secondary_phone": medallion_owner.get("secondary_contact_number", ""),
        "medallion_owner_email": medallion_owner["primary_email_address"] or "",
        "contract_start_date": lease.contract_start_date.strftime("%m/%d/%Y") or "",
        "contract_end_date": lease.contract_end_date.strftime("%m/%d/%Y") or "",
        "contract_amount": str(lease.royalty_amount) or "",
        "medallion_owner_name": medallion_owner["medallion_owner"] or "",
        "/owner_sign/": "",
        "/agent_sign/": ""
    }


def prepare_medallion_cover_letter_document(medallion, medallion_owner):
    """Prepare the medallion cover letter document"""
    return {
        "cover_letter_date": datetime.now().strftime("%B %d, %Y"),
        "medallion_owner_address": medallion_owner["owner_address"]["address_line_1"],
        "medallion_owner_city_state_zip": f"{medallion_owner['owner_address']['address_line_2']} {medallion_owner['owner_address']['city']}, {medallion_owner['owner_address']['state']} {medallion_owner['owner_address']['zip']}",
        "medallion_number": medallion.medallion_number,
        "medallion_owner_name": medallion_owner["medallion_owner"],
    }
