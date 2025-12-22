import xml.etree.ElementTree as ET

import test_settings as settings
from app.core.db import get_db
from app.driver_payments.models import DriverTransactionReceipt
from app.drivers.services import driver_service
from app.dtr.models import DTR
from app.leases.services import lease_service
from app.medallions.services import medallion_service
from curb_api_service import CurbApiService


def extract_trip_records(xml_string, fields=("CABNUMBER", "DRIVER")):
    """
    Extract specific attributes from each RECORD element in the XML.

    Args:
        xml_string (str): Full XML response string.
        fields (tuple): Attribute names to extract from each RECORD element.

    Returns:
        List[dict]: List of dictionaries with extracted fields per record.
    """
    try:
        root = ET.fromstring(xml_string)
        records = []
        plain_records = []
        # Loop through each "RECORD" element
        for record in root.findall(".//RECORD"):
            extracted = {field: record.get(field, None) for field in fields}
            records.append(extracted)
            plain_records.append(record)
        return records, plain_records

    except ET.ParseError as e:
        print("Error parsing XML:", e)
        return []


def main():
    curb_service = CurbApiService(
        base_url=settings.CURB_URL,
        merchant=settings.CURB_MERCHANT,
        username=settings.CURB_USERNAME,
        password=settings.CURB_PASSWORD,
    )

    present_combo = []
    dbgen = get_db()
    db = next(dbgen)

    driver_id = None
    cab_number = None

    # Example API call
    try:
        print("Transactions by date api call starting")
        # rp = curb_service.get_trans_by_date_cab12(
        #     from_date="2025-11-05",
        #     to_date="2025-11-06",
        #     # driver_id="4725959",
        # )
        print("Transactions by date api call is done")
        print("Calling trips data")
        response = curb_service.get_trips_log10(
            from_date="2025-11-01",
            to_date="2025-11-07",
            # driver_id="4725959",
        )
        print("Calling trips data is done")
        (trip_records, raw_records) = extract_trip_records(response)
        for idx, record in enumerate(trip_records):
            print(record)
            if record["DRIVER"]:
                driver_id = record["DRIVER"]

            if record["CABNUMBER"]:
                cab_number = record["CABNUMBER"]

            if not cab_number or not driver_id:
                print("Record does not have driver or cab number %s " % record)
                continue

            driver = driver_service.get_drivers(db, tlc_license_number=driver_id)
            if not driver:
                print("Driver does not exist")
                continue

            # Find medallion by cab number
            medallion = medallion_service.get_medallion(db, medallion_number=cab_number)
            if not medallion:
                print("Medallion does not exist")
                continue

            # Find the active lease for this driver and medallion on the trip date
            active_lease = lease_service.get_lease(
                db,
                driver_id=driver.driver_id,
                medallion_number=medallion.medallion_number,
                status="Active",
            )
            if not active_lease:
                print("Active Lease does not exist")
                continue

            present_combo.append(
                {
                    "medallion_no": cab_number,
                    "driver_no": driver_id,
                    "lease_id": active_lease.lease_id
                    if active_lease.lease_id is not None
                    else "",
                    "og_record": raw_records[idx],
                }
            )
    except Exception as e:
        print("Error:", e)
    print("Combo's present")
    for combo in present_combo:
        print(combo)


if __name__ == "__main__":
    main()
    # Example usage
    # xml_data = """
    # <Trips>
    #     <RECORD ID="8288059" PERIOD="202504" CABNUMBER="1T54" DRIVER="6080560" NUM_SERVICE="4915"
    #         START_DATE="11/05/2025 14:49:57" END_DATE="11/05/2025 15:35:55" TRIP="53.4000" />
    #     <RECORD ID="8288060" PERIOD="202504" CABNUMBER="7E23" DRIVER="6023456" NUM_SERVICE="4916"
    #         START_DATE="11/05/2025 16:00:00" END_DATE="11/05/2025 16:45:00" TRIP="35.7500" />
    # </Trips>
    # """

    # trip_records = extract_trip_records(xml_data)
    # print(trip_records)
