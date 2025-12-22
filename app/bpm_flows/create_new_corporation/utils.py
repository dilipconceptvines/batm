## app/bpm_flows/create_new_corporation/utils.py

from app.utils.logger import get_logger

logger = get_logger(__name__)

def prepare_rider_payload(corporation , authorized_signer=None):
    """
    Prepare the payload for the rider.
    """
    secondary_address = corporation.secondary_address if corporation and corporation.secondary_address else None
    payload = {
        "lesse_dba_name": "BIG APPLE TAXI MANAGEMENT LLC",
        "name": corporation.name or "",
        "leasor_dba_president": authorized_signer.name if authorized_signer else "",
        "leasor_dba_description": authorized_signer.owner_type if authorized_signer else "",
        "leasor_mailing_address": secondary_address.address_line_1 or "",
        "leasor_city": secondary_address.city if secondary_address else "",
        "leasor_state": secondary_address.state if secondary_address else "",
        "leasor_zipcode" : secondary_address.zip if secondary_address else "",
        "employer_identification_number" : corporation.ein or "",
        "leasor_dba_name": "",
    }
    

    logger.info("Prepared rider payload", payload=payload)

    return payload
