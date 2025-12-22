## app/bpm_flows/update_medallion_address/utils.py
from datetime import datetime

def format_address_info(address):
    """Format the address info"""
    if address:
        return {
                "address_line_1": address.address_line_1 or "",
                "address_line_2": address.address_line_2 or "",
                "city": address.city or "",
                "state": address.state or "",
                "zip": address.zip or "",
                "medallion_owner_id": address.medallion_owners[0].id if address.medallion_owners else None
            }
    return {
        "address_line_1": "",
        "address_line_2": "",
        "city": "",
        "state": "",
        "zip": "",
    }

def prepare_address_update_payload(address,secondary_address, owner, step_data=None):
    """
    Prepare the address update payload
    """

    old_primary_address = owner.get("primary_address" , None)
    old_secondary_address = owner.get("secondary_address" , None)

    if step_data is None:
        step_data = {}

    address_update_data = {
        "medallion_number_1": owner.get("medallions")[0].get("medallion_number") if owner.get("medallions") and len(owner.get("medallions")) > 0 and owner.get("medallions")[0].get("medallion_number") else "",
        "medallion_number_2": owner.get("medallions")[1].get("medallion_number") if owner.get("medallions") and len(owner.get("medallions")) > 1 and owner.get("medallions")[1].get("medallion_number") else "",
        "medallion_number_3": owner.get("medallions")[2].get("medallion_number") if owner.get("medallions") and len(owner.get("medallions")) > 2 and owner.get("medallions")[2].get("medallion_number") else "",
        "medallion_number_4": owner.get("medallions")[3].get("medallion_number") if owner.get("medallions") and len(owner.get("medallions")) > 3 and owner.get("medallions")[3].get("medallion_number") else "",
        "corporation_name_for_medallion": owner["owner_name"] if owner["owner_type"] == "corporation" and owner["owner_name"] else "",
        "name": owner["owner_name"] if owner["owner_type"] == "individual" and owner["owner_name"] else "",
        "old_mailing_address": old_primary_address.address_line_1 if old_primary_address and old_primary_address.address_line_1 else "",
        "old_mailing_address_city": old_primary_address.city if old_primary_address and old_primary_address.city else "",
        "old_mailIng_address_state": old_primary_address.state if old_primary_address and old_primary_address.state else "",
        "old_mailing_address_zipcode": str(old_primary_address.zip) if old_primary_address and old_primary_address.zip else "",
        "new_mailing_address": address.get("address_line_1", ""),
        "new_mailing_address_city": address.get("city", ""),
        "new_mailing_address_state": address.get("state", ""),
        "new_mailing_address_zipcode": str(address.get("zip", "")),
        "new_mailing_address_email": step_data.get("email" , ""),
        "new_mailing_address_telephone": step_data.get("phone_1" , ""),
        "old_residence_address": old_secondary_address.address_line_1 if old_secondary_address and old_secondary_address.address_line_1 else "",
        "old_residence_address_city": old_secondary_address.city if old_secondary_address and old_secondary_address.city else "",
        "old_residence_address_state": old_secondary_address.state if old_secondary_address and old_secondary_address.state else "",
        "old_residence_address_zipcode": old_secondary_address.zip if old_secondary_address and old_secondary_address.zip else "",
        "new_residence_address": secondary_address.get("address_line_1", ""),
        "new_residence_city": secondary_address.get("city", ""),
        "new_residence_address_state": secondary_address.get("state", ""),
        "new_residence_address_zipcode": secondary_address.get("zip", ""),
        "new_residence_address_email":step_data.get("email" , ""),
        "new_residence_address_telephone": step_data.get("phone_1" , ""),
        "medallion_owner_signature": "",
        "filling_date": datetime.today().date().isoformat(),
        "name_tlc_employee": "",
        "signature_tlc_employee": "",
        "tlc_sign_date": ""
    }

    return address_update_data