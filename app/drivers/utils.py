## app/drivers/utils.py

from app.utils.general import format_address, format_us_phone_number


def extract_driver_info(driver):
    """
    Extract driver information with null safety for document generation.

    Args:
        driver: Driver object

    Returns:
        Dictionary with driver information
    """
    if not driver:
        return {
            "name": "N/A",
            "address": "N/A",
            "primary_phone": "N/A",
            "ssn": "N/A",
            "dmv_license": "N/A",
            "dmv_license_expiration": "N/A",
            "tlc_license": "N/A",
            "tlc_license_expiration": "N/A",
            "email": "N/A",
        }

    return {
        "name": driver.full_name if driver.full_name else "N/A",
        "address": format_address(driver.primary_driver_address)
        if driver.primary_driver_address
        else "N/A",
        "primary_phone": format_us_phone_number(driver.phone_number_1)
        if driver.phone_number_1
        else "N/A",
        "ssn": driver.ssn if driver.ssn else "N/A",
        "dmv_license": driver.dmv_license.dmv_license_number
        if driver.dmv_license and driver.dmv_license.dmv_license_number
        else "N/A",
        "dmv_license_expiration": driver.dmv_license.dmv_license_expiry_date.strftime(
            "%Y-%m-%d"
        )
        if driver.dmv_license and driver.dmv_license.dmv_license_expiry_date
        else "N/A",
        "tlc_license": driver.tlc_license.tlc_license_number
        if driver.tlc_license and driver.tlc_license.tlc_license_number
        else "N/A",
        "tlc_license_expiration": driver.tlc_license.tlc_license_expiry_date.strftime(
            "%Y-%m-%d"
        )
        if driver.tlc_license and driver.tlc_license.tlc_license_expiry_date
        else "N/A",
        "email": driver.email_address if driver.email_address else "N/A",
    }


def format_driver_response(
    driver,
    has_documents=False,
    has_active_lease: bool = False,
    has_vehicle: bool = False,
    has_audit_trail: bool = False,
):
    """Format driver response with additional fields"""
    marked_ssn = f"XXX-XX-{driver.ssn[-4:]}" if driver.ssn else ""
    bank_account = driver.driver_bank_account
    bank_address = (
        bank_account.bank_address
        if bank_account and bank_account.bank_address
        else None
    )
    return {
        "driver_details": {
            "driver_id": driver.id,
            "driver_lookup_id": driver.driver_id,
            "first_name": driver.first_name,
            "middle_name": driver.middle_name,
            "last_name": driver.last_name,
            "driver_type": driver.driver_type,
            "driver_status": driver.driver_status,
            "driver_ssn": marked_ssn,
            "dob": driver.dob,
            "phone_number_1": driver.phone_number_1,
            "phone_number_2": driver.phone_number_2,
            "email_address": driver.email_address,
            "primary_emergency_contact_person": driver.primary_emergency_contact_person,
            "primary_emergency_contact_relationship": driver.primary_emergency_contact_relationship,
            "primary_emergency_contact_number": driver.primary_emergency_contact_number,
            "additional_emergency_contact_person": driver.additional_emergency_contact_person,
            "additional_emergency_contact_relationship": driver.additional_emergency_contact_relationship,
            "additional_emergency_contact_number": driver.additional_emergency_contact_number,
            "violation_due_at_registration": driver.violation_due_at_registration,
            "is_drive_locked": driver.drive_locked,
            "has_audit_trail": has_audit_trail,
        },
        "dmv_license_details": {
            "is_dmv_license_active": bool(driver.dmv_license),
            "dmv_license_number": driver.dmv_license.dmv_license_number
            if driver.dmv_license
            else None,
            "dmv_license_issued_state": driver.dmv_license.dmv_license_issued_state
            if driver.dmv_license
            else None,
            "dmv_license_expiry_date": driver.dmv_license.dmv_license_expiry_date
            if driver.dmv_license
            else None,
            "dmv_class": driver.dmv_license.dmv_class if driver.dmv_license else None,
            "dmv_license_status": driver.dmv_license.dmv_license_status
            if driver.dmv_license
            else None,
            "dmv_class_change_date": driver.dmv_license.dmv_class_change_date
            if driver.dmv_license
            else None,
            "dmv_renewal_fee": driver.dmv_license.dmv_renewal_fee
            if driver.dmv_license
            else None,
        },
        "tlc_license_details": {
            "is_tlc_license_active": bool(driver.tlc_license),
            "tlc_license_number": driver.tlc_license.tlc_license_number
            if driver.tlc_license
            else None,
            "tlc_license_expiry_date": driver.tlc_license.tlc_license_expiry_date
            if driver.tlc_license
            else None,
            "tlc_issued_state": driver.tlc_license.tlc_issued_state
            if driver.tlc_license
            else None,
            "tlc_ddc_date": driver.tlc_license.tlc_ddc_date
            if driver.tlc_license
            else None,
            "tlc_drug_test_date": driver.tlc_license.tlc_drug_test_date
            if driver.tlc_license
            else None,
            "tlc_hack_date": driver.tlc_license.tlc_hack_date
            if driver.tlc_license
            else None,
            "tlc_lease_card_date": driver.tlc_license.tlc_lease_card_date
            if driver.tlc_license
            else None,
            "tlc_renewal_fee": driver.tlc_license.tlc_renewal_fee
            if driver.tlc_license
            else None,
        },
        "primary_address_details": {
            "address_line_1": driver.primary_driver_address.address_line_1
            if driver.primary_driver_address
            else None,
            "address_line_2": driver.primary_driver_address.address_line_2
            if driver.primary_driver_address
            else None,
            "city": driver.primary_driver_address.city
            if driver.primary_driver_address
            else None,
            "state": driver.primary_driver_address.state
            if driver.primary_driver_address
            else None,
            "zip": driver.primary_driver_address.zip
            if driver.primary_driver_address
            else None,
            "latitude": driver.primary_driver_address.latitude
            if driver.primary_driver_address
            else None,
            "longitude": driver.primary_driver_address.longitude
            if driver.primary_driver_address
            else None,
        },
        "secondary_address_details": {
            "address_line_1": driver.secondary_driver_address.address_line_1
            if driver.secondary_driver_address
            else None,
            "address_line_2": driver.secondary_driver_address.address_line_2
            if driver.secondary_driver_address
            else None,
            "city": driver.secondary_driver_address.city
            if driver.secondary_driver_address
            else None,
            "state": driver.secondary_driver_address.state
            if driver.secondary_driver_address
            else None,
            "zip": driver.secondary_driver_address.zip
            if driver.secondary_driver_address
            else None,
            "latitude": driver.secondary_driver_address.latitude
            if driver.secondary_driver_address
            else None,
            "longitude": driver.secondary_driver_address.longitude
            if driver.secondary_driver_address
            else None,
        },
        "payee_details": {
            "pay_to_mode": "ACH"
            if driver.pay_to_mode and driver.pay_to_mode != "Check"
            else "Check",
            "data": {
                "bank_name": bank_account.bank_name if bank_account else "",
                "bank_routing_number": bank_account.bank_routing_number
                if bank_account
                else "",
                "bank_account_number": bank_account.bank_account_number
                if bank_account
                else "",
                "bank_account_name": bank_account.bank_account_name
                if bank_account
                else "",
                "effective_from": bank_account.effective_from if bank_account else "",
            }
            if driver.pay_to_mode and driver.pay_to_mode != "Check"
            else {"bank_account_name": driver.pay_to or ""},
        },
        "lease_info": {
            "has_active_lease": has_active_lease,
            "lease_type": driver.lease_drivers[0].lease.lease_type
            if driver.lease_drivers
            else None,
        },
        "has_documents": has_documents,
        "has_vehicle": has_vehicle,
        "is_archived": driver.is_archived,
    }
