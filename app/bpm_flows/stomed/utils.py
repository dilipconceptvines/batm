## app/bpm_flows/stomed/utils.py

def prepare_storage_receipt_payload(storage, medallion):
    """
    Prepare the storage receipt payload
    """
    storage_payload = {
        "medallion_owner_name": medallion.get("medallion_owner_name" , ""),
        "medallion_number": medallion.get("medallion_number" , ""),
        "reason_for_placing": storage.storage_reason if storage.storage_reason else "",
        "signature": "",
        "print_name": storage.print_name if storage.print_name else "",
        "receipt_date": str(storage.storage_date) if storage.storage_date else "",
        "storage_entry_by": "",
        "storage_entry_date": str(storage.storage_initiated_date) if storage.storage_initiated_date else "",
        "medallion_removal_signature": "",
        "medallion_removal_name": "",
        "medallion_removal_date": "",
        "medallion_removal_tlc_date": "",
        "medallion_removed_by": "",
        "rate_card_removal_date": "",
        "rate_card_removed_by": ""
    }

    return storage_payload