# app/curb/services.py

import xml.etree.ElementTree as ET
import datetime as dt
from datetime import timedelta, date, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any
from io import BytesIO

import requests
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.curb.exceptions import (
    CurbApiError, DataMappingError, TripProcessingError
)
from app.curb.models import (
    CurbTrip, CurbTripStatus, PaymentType
)
from app.curb.repository import CurbRepository
from app.medallions.models import Medallion
from app.medallions.schemas import MedallionStatus
from app.drivers.services import driver_service
from app.leases.services import lease_service
from app.medallions.services import medallion_service
from app.ledger.services import LedgerService
from app.ledger.repository import LedgerRepository
from app.worker.app import app
from app.utils.s3_utils import s3_utils
from app.utils.logger import get_logger

logger = get_logger(__name__)


class CurbApiService:
    """
    Handles low level communication with the CURB SOAP API.
    """

    def __init__(self):
        self.base_url = settings.curb_url
        self.username = settings.curb_username
        self.password = settings.curb_password
        self.merchant = settings.curb_merchant
        self.headers = {"Content-Type": "text/xml; charset=utf-8"}

    def _make_soap_request(self, soap_action: str, payload: str) -> str:
        """
        Makes a SOAP request to the CURB API and returns the response XML.
        """
        try:
            full_action = f"https://www.taxitronic.org/VTS_SERVICE/{soap_action}"
            self.headers["SOAPAction"] = full_action
            response = requests.post(
                self.base_url, data=payload.encode("utf-8"), headers=self.headers, timeout=120
            )
            response.raise_for_status()

            root = ET.fromstring(response.content)
            namespace = "https://www.taxitronic.org/VTS_SERVICE/"
            result_tag = f"{{{namespace}}}{soap_action}Result"
            result_element = root.find(f".//{result_tag}")

            if result_element is None:
                raise CurbApiError(f"'{soap_action}Result' tag not found in SOAP response.")
            
            return result_element.text
        
        except requests.exceptions.RequestException as e:
            logger.error("CURB API request failed: %s", e, exc_info=True)
            raise CurbApiError(f"Network error communicating with CURB API: {e}") from e
        except ET.ParseError as e:
            logger.error("Failed to parse CURB API SOAP response: %s", e, exc_info=True)
            raise CurbApiError("Invalid XML response from CURB API.") from e
        
    def get_trips_log10(self, from_date: str, to_date: str, cab_number: str = None) -> str:
        """Fetches trip data from the GET_TRIPS_LOG10 endpoint."""
        if cab_number:
            payload = f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <GET_TRIPS_LOG10 xmlns="https://www.taxitronic.org/VTS_SERVICE/">
                <UserId>{self.username}</UserId>
                <Password>{self.password}</Password>
                <Merchant>{self.merchant}</Merchant>
                <DRIVERID></DRIVERID>
                <CABNUMBER>{cab_number}</CABNUMBER>
                <DATE_FROM>{from_date}</DATE_FROM>
                <DATE_TO>{to_date}</DATE_TO>
                <RECON_STAT>-1</RECON_STAT>
                </GET_TRIPS_LOG10>
            </soap:Body>
            </soap:Envelope>"""
        else:
            payload = f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <GET_TRIPS_LOG10 xmlns="https://www.taxitronic.org/VTS_SERVICE/">
                <UserId>{self.username}</UserId>
                <Password>{self.password}</Password>
                <Merchant>{self.merchant}</Merchant>
                <DRIVERID></DRIVERID>
                <CABNUMBER></CABNUMBER>
                <DATE_FROM>{from_date}</DATE_FROM>
                <DATE_TO>{to_date}</DATE_TO>
                <RECON_STAT>-1</RECON_STAT>
                </GET_TRIPS_LOG10>
            </soap:Body>
            </soap:Envelope>"""
        return self._make_soap_request("GET_TRIPS_LOG10", payload)
    
    def get_trans_by_date_cab12(self, from_date: str, to_date: str, cab_number: str = "") -> str:
        """Fetches card transaction data from the Get_Trans_By_Date_Cab12 endpoint."""
        payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <Get_Trans_By_Date_Cab12 xmlns="https://www.taxitronic.org/VTS_SERVICE/">
              <UserId>{self.username}</UserId>
              <Password>{self.password}</Password>
              <Merchant>{self.merchant}</Merchant>
              <fromDateTime>{from_date}</fromDateTime>
              <ToDateTime>{to_date}</ToDateTime>
              <CabNumber>{cab_number}</CabNumber>
              <TranType>ALL</TranType>
            </Get_Trans_By_Date_Cab12>
          </soap:Body>
        </soap:Envelope>"""
        return self._make_soap_request("Get_Trans_By_Date_Cab12", payload)

    def reconcile_trips(self, trip_ids: List[str], reconciliation_id: str):
        """Marks a list of trip IDs as reconciled in the CURB system."""
        if not trip_ids:
            return

        list_ids = ",".join(trip_ids)
        
        payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <Reconciliation_TRIP_LOG xmlns="https://www.taxitronic.org/VTS_SERVICE/">
              <UserId>{self.username}</UserId>
              <Password>{self.password}</Password>
              <Merchant>{self.merchant}</Merchant>
              <RECON_STAT>{reconciliation_id}</RECON_STAT>
              <ListIDs>{list_ids}</ListIDs>
            </Reconciliation_TRIP_LOG>
          </soap:Body>
        </soap:Envelope>"""
        self._make_soap_request("Reconciliation_TRIP_LOG", payload)


class CurbService:
    """
    Main service for orchestrating CURB data import, processing, and reconciliation.
    """

    def __init__(self, db: Session):
        self.db = db
        self.api_service = CurbApiService()
        self.repo = CurbRepository(db)
        self.ledger_repo = LedgerRepository(db)
        self.ledger_service = LedgerService(self.ledger_repo)

    def _parse_and_normalize_trips(self, xml_data: str, filter_cash_only: bool = False) -> List[Dict]:
        """
        Parses the XML response from CURB and normalizes it into a standard dictionary format.
        Handles multiple XML structures: GET_TRIPS_LOG10, Get_Trans_By_Date_Cab12, and TRIPS/RECORD format.
        """
        trips = []
        if not xml_data:
            return trips
        
        try:
            root = ET.fromstring(xml_data)
            # Support multiple XML structures: trip, tran, and RECORD elements
            trip_nodes = root.findall(".//RECORD") + root.findall(".//tran")
            
            for trip_node in trip_nodes:
                if not isinstance(trip_node, ET.Element):
                    continue
                    
                try:
                    def get_value(element, field_name, alt_names=None):
                        """Get value from either attribute or nested element, with alternative field names"""
                        # List of possible field names to try
                        field_names = [field_name]
                        if alt_names:
                            field_names.extend(alt_names)
                        
                        for fname in field_names:
                            # Try attribute first
                            attr_value = element.attrib.get(fname)
                            if attr_value is not None:
                                return attr_value
                                
                            # Try nested element
                            child_elem = element.find(fname)
                            if child_elem is not None:
                                return child_elem.text
                                
                        return None

                    # Determine payment type with flexible parsing
                    payment_type_str = get_value(trip_node, "PAYMENT_TYPE", ["CC_TYPE", "T"]) or ""
                    payment_type_char = payment_type_str[0] if payment_type_str else None
                    
                    if payment_type_char == '$' or 'cash' in (trip_node.findtext('PAYMENT_TYPE') or '').lower():
                        payment_type = PaymentType.CASH
                    elif payment_type_char in ['C', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']:
                        payment_type = PaymentType.CREDIT_CARD
                    elif payment_type_char == 'P':
                        payment_type = PaymentType.PRIVATE
                    else:
                        payment_type = PaymentType.UNKNOWN

                    if filter_cash_only and payment_type == PaymentType.CREDIT_CARD:
                        continue

                    # Use ROWID for transactions, ID for RECORD elements, or a composite key for trips
                    trip_id = trip_node.attrib.get("ROWID") or trip_node.attrib.get("RECORD ID") or trip_node.attrib.get("ID")
                    period = trip_node.attrib.get("PERIOD")
                    
                    # If period is not available, generate it as MMYYYY from current date
                    if not period:
                        from datetime import datetime
                        now = datetime.now()
                        period = f"{now.month:02d}{now.year}"

                    if not trip_id:
                        continue
                    
                    unique_id = f"{period}-{trip_id}" if period else trip_id

                    # Parse date and time with flexible approach
                    trip_date = get_value(trip_node, "TRIPDATE") or ""
                    trip_time_start = get_value(trip_node, "TRIPTIMESTART") or ""
                    trip_time_end = get_value(trip_node, "TRIPTIMEEND") or ""
                    
                    # Try to get combined datetime first, then fall back to date+time combination
                    start_datetime_str = get_value(trip_node, "START_DATE")
                    end_datetime_str = get_value(trip_node, "END_DATE")

                    # Try to get the transaction date if available
                    transaction_date_str = get_value(trip_node, "DATETIME") or None
                    
                    # If no combined datetime, construct from separate date and time fields
                    if not start_datetime_str and trip_date and trip_time_start:
                        start_datetime_str = f"{trip_date} {trip_time_start}"
                    if not end_datetime_str and trip_date and trip_time_end:
                        end_datetime_str = f"{trip_date} {trip_time_end}"
                        
                    # Handle alternative datetime format (DATETIME field)
                    if not start_datetime_str:
                        datetime_field = get_value(trip_node, "DATETIME")
                        if datetime_field:
                            start_datetime_str = datetime_field
                            # For single datetime, assume trip duration or use same for both
                            end_datetime_str = end_datetime_str or datetime_field
                    
                    # Clean up the strings
                    start_datetime_str = (start_datetime_str or "").strip()
                    end_datetime_str = (end_datetime_str or "").strip()

                    
                    # Try parsing with seconds first, then without
                    def parse_flexible_datetime(datetime_str: str) -> dt.datetime:
                        """Parse datetime with flexible format handling."""
                        if not datetime_str or datetime_str == " ":
                            raise ValueError("Empty datetime string")
                        
                        # Try with seconds first
                        try:
                            return dt.datetime.strptime(datetime_str, "%m/%d/%Y %H:%M:%S")
                        except ValueError:
                            # If that fails, try without seconds
                            try:
                                return dt.datetime.strptime(datetime_str, "%m/%d/%Y %H:%M")
                            except ValueError:
                                # If both fail, log and re-raise
                                logger.warning(f"Failed to parse datetime: '{datetime_str}'")
                                raise

                    start_time = parse_flexible_datetime(start_datetime_str)
                    end_time = parse_flexible_datetime(end_datetime_str)
                    if transaction_date_str:
                        logger.info("Parsing transaction date **** ", transaction_date_str=transaction_date_str)
                        transaction_date = parse_flexible_datetime(transaction_date_str)
                    else:
                        transaction_date = end_time

                    # Extract CABNUMBER with flexible parsing (attributes or nested elements)
                    cab_number = get_value(trip_node, "CABNUMBER", ["CAB_NUMBER", "CabNumber"])
                        
                    if not cab_number and trip_node.tag == "tran":
                        # Debug: Log when CABNUMBER is missing from transaction records
                        logger.warning(f"Missing CABNUMBER in transaction {unique_id}. Available attributes: {list(trip_node.attrib.keys())}")
                        # Log child elements for debugging
                        child_elements = [child.tag for child in trip_node]
                        logger.warning(f"Available child elements: {child_elements}")
                    elif cab_number:
                        logger.debug(f"Found CABNUMBER: {cab_number} for transaction {unique_id}")

                    # Generate curb_period from transaction_date if period is null/none
                    if not period and transaction_date:
                        # Format: MMYYYY (e.g., 032025 for March 2025)
                        period = transaction_date.strftime("%m%Y")
                        logger.debug(f"Generated curb_period from transaction_date: {period}")

                    trip_data = {
                        "curb_trip_id": unique_id,
                        "curb_period": period,
                        "status": CurbTripStatus.UNRECONCILED,
                        "curb_driver_id": get_value(trip_node, "DRIVER", ["TRIPDRIVERID"]),
                        "curb_cab_number": cab_number,
                        "start_time": start_time,
                        "end_time": end_time,
                        "fare": Decimal(get_value(trip_node, "TRIPFARE", ["TRIP"]) or "0.00"),
                        "tips": Decimal(get_value(trip_node, "TRIPTIPS", ["TIPS"]) or "0.00"),
                        "tolls": Decimal(get_value(trip_node, "TRIPTOLL", ["TOLLS"]) or "0.00"),
                        "extras": Decimal(get_value(trip_node, "TRIPEXTRAS", ["EXTRAS"]) or "0.00"),
                        "total_amount": Decimal(get_value(trip_node, "TOTAL_AMOUNT", ["AMOUNT"]) or "0.00"),
                        "surcharge": Decimal(get_value(trip_node, "TAX") or "0.00"),
                        "improvement_surcharge": Decimal(get_value(trip_node, "IMPTAX") or "0.00"),
                        "congestion_fee": Decimal(get_value(trip_node, "CongFee", ["CONGFEE"]) or "0.00"),
                        "airport_fee": Decimal(get_value(trip_node, "airportFee") or "0.00"),
                        "cbdt_fee": Decimal(get_value(trip_node, "cbdt") or "0.00"),
                        "start_long": Decimal(get_value(trip_node, "GPS_START_LO", ["FromLo"]) or None),
                        "start_lat": Decimal(get_value(trip_node, "GPS_START_LA", ["FromLa"]) or None),
                        "end_long": Decimal(get_value(trip_node, "GPS_END_LO", ["ToLo"]) or None),
                        "end_lat": Decimal(get_value(trip_node, "GPS_END_LA", ["ToLa"]) or None),
                        "num_service": int(get_value(trip_node, "NUM_SERVICE") or None),
                        "payment_type": payment_type,
                        "transaction_date": transaction_date,
                    }
                    logger.info("Parsed trip data: %s", trip_data)
                    trips.append(trip_data)
                except (ValueError, KeyError) as e:
                    logger.warning("Skipping malformed trip record: %s. Error: %s", 
                                ET.tostring(trip_node, 'utf-8'), e)
                    continue

        except ET.ParseError as e:
            logger.error("Failed to parse CURB XML data: %s", e, exc_info=True)
            raise CurbApiError("Invalid XML data received from CURB.") from e
        
        # Deduplicate trips by curb_trip_id - keep the last occurrence
        seen_trip_ids = {}
        deduplicated_trips = []
        
        for trip in trips:
            trip_id = trip["curb_trip_id"]
            if trip_id in seen_trip_ids:
                logger.warning(f"Duplicate trip ID found in XML batch: {trip_id}. Keeping latest occurrence.")
                # Replace the previous entry
                deduplicated_trips = [t for t in deduplicated_trips if t["curb_trip_id"] != trip_id]
            
            deduplicated_trips.append(trip)
            seen_trip_ids[trip_id] = True
        
        if len(deduplicated_trips) != len(trips):
            logger.info(f"Removed {len(trips) - len(deduplicated_trips)} duplicate trips from XML batch")
        
        return deduplicated_trips
    
    def _reconcile_locally(self, trips: List[CurbTrip]) -> int:
        """
        Marks a list of trips as reconciled directly in the local database.
        """
        if not trips:
            return 0
        
        for trip in trips:
            self.repo.update_trip_status(trip.id, CurbTripStatus.RECONCILED)

        self.db.commit()
        logger.info(f"Locally marked {len(trips)} trips as RECONCILED for non-production environment.")

        return len(trips)
    
    def import_and_reconcile_data(
        self, from_date: Optional[date] = None, to_date: Optional[date] = None,
        medallion_no: Optional[str] = None
    ) -> Dict:
        """
        Fetches CURB data based on active medallions, stores it raw, and reconciles.
        """
        try:
            # Step 1: Determine medallions to query
            if medallion_no:
                medallion_numbers = [m.strip() for m in medallion_no.split(',') if m.strip()]
                logger.info(f"Starting CURB import for specified medallions: {medallion_numbers}")
            else:
                logger.info("Starting CURB import for all active medallions in the system.")
                active_medallions = self.db.query(Medallion.medallion_number).filter(
                    Medallion.medallion_status == MedallionStatus.ACTIVE
                ).all()
                medallion_numbers = [m[0] for m in active_medallions]
                if not medallion_numbers:
                    return {"message": "No active medallions found in the system to import data for."}
                
            # Step 2: Set the date range
            to_date = to_date or date.today()
            from_date = from_date or (to_date - timedelta(days=1))
            date_format = "%m/%d/%Y"
            from_date_str = from_date.strftime(date_format)
            to_date_str = to_date.strftime(date_format)

            # Step 3: Fetch data for each medallion
            all_trips_data = {}
            api_errors = []
            for cab_number in medallion_numbers:
                try:
                    logger.debug(f"Fetching data for medallion {cab_number}...")
                    trips_log_xml = self.api_service.get_trips_log10(from_date_str + " 00:00:00", to_date_str + " 23:59:59", cab_number=cab_number)
                    trans_xml = self.api_service.get_trans_by_date_cab12(from_date_str + " 00:00:00", to_date_str + " 23:59:59", cab_number=cab_number)
                    
                    normalized_trips = self._parse_and_normalize_trips(trips_log_xml)
                    normalized_trans = self._parse_and_normalize_trips(trans_xml)
                    
                    # Deduplicate using a dictionary
                    for trip in normalized_trips + normalized_trans:
                        all_trips_data[trip['curb_trip_id']] = trip

                except CurbApiError as e:
                    logger.error(f"Failed to fetch data for medallion {cab_number}: {e}")
                    api_errors.append({"medallion_number": cab_number, "error": str(e)})

            final_trip_list = list(all_trips_data.values())
            logger.info(f"Fetched a total of {len(final_trip_list)} unique records from CURB.")

            # Step 4: Store Raw Data
            inserted, updated = self.repo.bulk_insert_or_update(final_trip_list)
            self.db.commit()
            logger.info(f"Database operation complete: {inserted} new trips inserted, {updated} trips updated.")

            # Step 5: Reconcile Trips
            unreconciled_trips = self.repo.get_unreconciled_trips()
            reconciled_count = 0
            reconciliation_id = None
            
            if unreconciled_trips:
                if settings.environment == "production":
                    logger.info("Reconciling with CURB server (production environment).")
                    trip_ids_to_reconcile = [trip.curb_trip_id.split('-')[-1] for trip in unreconciled_trips]
                    reconciliation_id = f"BAT-RECO-{dt.datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
                    try:
                        self.api_service.reconcile_trips(trip_ids_to_reconcile, reconciliation_id)
                        # Update status in local DB after successful API call
                        for trip in unreconciled_trips:
                            self.repo.update_trip_status(trip.id, CurbTripStatus.RECONCILED, reconciliation_id)
                        self.db.commit()
                        reconciled_count = len(unreconciled_trips)
                        logger.info(f"Successfully reconciled {reconciled_count} trips with CURB API.")
                    except CurbApiError as e:
                        self.db.rollback()
                        logger.error(f"Failed to reconcile trips with CURB API: {e}")
                        api_errors.append({"reconciliation": "failed", "error": str(e)})
                else:
                    reconciled_count = self._reconcile_locally(unreconciled_trips)

            return {
                "medallions_queried": len(medallion_numbers),
                "date_range": {"from": from_date_str, "to": to_date_str},
                "records_fetched": len(final_trip_list),
                "newly_inserted": inserted,
                "records_updated": updated,
                "records_reconciled": reconciled_count,
                "reconciliation_id": reconciliation_id,
                "api_errors": api_errors
            }

        except Exception as e:
            self.db.rollback()
            logger.error("An unexpected error occurred during CURB import: %s", e, exc_info=True)
            raise

    def map_reconciled_trips(self) -> Dict:
        """
        Finds all RECONCILED (but not yet mapped) trips and attempts to associate them
        with internal Driver, Medallion, Vehicle, and Lease records. On success, the
        trip status is updated to MAPPED.
        """
        logger.info("Starting task to map reconciled CURB trip records.")
        
        trips_to_map = self.repo.find_trips_by_status(CurbTripStatus.RECONCILED)

        if not trips_to_map:
            logger.info("No reconciled CURB trips found to map.")
            return {
                "total_trips_found": 0,
                "successfully_mapped": 0,
                "mapping_failures": 0,
                "errors": []
            }
            
        logger.info(f"Found {len(trips_to_map)} reconciled trips to process for mapping.")
        
        # Use the shared mapping logic
        return self._process_trip_mappings(trips_to_map)

        # return {
        #     "total_trips_to_map": len(trips_to_map),
        #     "successfully_mapped": successful_count,
        #     "mapping_failures": failed_count,
        #     "errors": errors,
        # }
    
    def map_reconciled_trips_by_ids(self, trip_ids: List[int]) -> Dict:
        """
        Map specific reconciled trips by their internal database IDs.
        More efficient than mapping all reconciled trips when only targeting a subset.
        
        Args:
            trip_ids: List of internal CurbTrip.id values to map
            
        Returns:
            Dictionary with mapping results
        """
        logger.info(f"Starting targeted mapping for {len(trip_ids)} specific trip IDs.")
        
        trips_to_map = (
            self.db.query(CurbTrip)
            .filter(
                CurbTrip.id.in_(trip_ids),
                CurbTrip.status == CurbTripStatus.RECONCILED
            )
            .all()
        )
        
        if not trips_to_map:
            logger.info("No reconciled trips found for the provided IDs.")
            return {
                "total_trips_found": 0,
                "successfully_mapped": 0,
                "mapping_failures": 0,
                "errors": []
            }
            
        logger.info(f"Found {len(trips_to_map)} reconciled trips to process for targeted mapping.")
        
        # Use the same mapping logic as the main method
        return self._process_trip_mappings(trips_to_map)
    
    def _process_trip_mappings(self, trips_to_map: List[CurbTrip]) -> Dict:
        """
        Internal method to process trip mappings for a given list of trips.
        Extracted from map_reconciled_trips to avoid code duplication.
        """
        successful_count = 0
        failed_count = 0
        errors = []

        for trip in trips_to_map:
            try:
                # Find the driver by tlc license number
                driver = driver_service.get_drivers(db=self.db, tlc_license_number=trip.curb_driver_id)
                if not driver:
                    raise DataMappingError("TLC License", trip.curb_driver_id)
                
                # Find the medallion by cab number
                medallion = medallion_service.get_medallion(db=self.db, medallion_number=trip.curb_cab_number)
                if not medallion:
                    raise DataMappingError("Medallion Number", trip.curb_cab_number)
                
                # Find the active lease mapping this driver and medallion
                lease = lease_service.get_lease(
                    db=self.db, driver_id=driver.driver_id, medallion_number=medallion.medallion_number,
                    status="Active"
                )

                if not lease:
                    raise DataMappingError(
                        "Lease", f"Driver ID {driver.driver_id} & Medallion {medallion.medallion_number}"
                    )
                
                # Update the CurbTrip record with foreign keys and set status to MAPPED
                trip.driver_id = driver.id
                trip.medallion_id = medallion.id
                trip.lease_id = lease.id
                trip.vehicle_id = lease.vehicle_id
                trip.status = CurbTripStatus.MAPPED
                
                if lease.vehicle and lease.vehicle.registrations:
                    trip.plate = lease.vehicle.registrations[0].plate_number

                successful_count += 1

            except DataMappingError as e:
                failed_count += 1
                errors.append({"curb_trip_id": trip.curb_trip_id, "error": str(e)})
                logger.warning(f"Mapping failed for trip {trip.curb_trip_id}: {e}")
                continue
            except Exception as e:
                failed_count += 1
                errors.append({"curb_trip_id": trip.curb_trip_id, "error": f"An unexpected error occurred: {str(e)}"})
                logger.error(f"Unexpected error mapping trip {trip.curb_trip_id}: {e}", exc_info=True)
                continue

        try:
            self.db.commit()
            logger.info(f"Committed {successful_count} successful trip mappings to the database.")
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error("Database commit failed during trip mapping: %s", e, exc_info=True)
            errors.append({"general_error": "Database commit failed", "detail": str(e)})
            failed_count = len(trips_to_map)  # Mark all as failed if commit fails
            successful_count = 0

        return {
            "total_trips_to_map": len(trips_to_map),
            "successfully_mapped": successful_count,
            "mapping_failures": failed_count,
            "errors": errors,
        }
    
    def post_mapped_earnings_to_ledger(
        self, start_date: date, end_date: date,
        lease_id: Optional[int] = None, driver_id: Optional[int] = None
    ) -> Dict:
        """
        Finds all MAPPED credit card trips for a given period and optional filters,
        and posts them as earnings to the centralized ledger.
        """
        logger.info(
            f"Starting manual earnings posting for period: {start_date} to {end_date} "
            f"(Lease ID: {lease_id}, Driver ID: {driver_id})"
        )

        try:
            trips_to_post = self.repo.get_mapped_credit_card_trips_for_posting(
                start_date=start_date,
                end_date=end_date,
                lease_id=lease_id,
                driver_id=driver_id
            )

            if not trips_to_post:
                logger.info("No new mapped credit card earnings found to post to the ledger.")
                return {
                    "drivers_processed": 0,
                    "trips_posted": 0,
                    "total_amount_posted": 0.0,
                    "errors": []
                }

            # Group earnings by (driver_id, lease_id) to make one ledger call per combination
            earnings_by_driver_lease: Dict[tuple, Decimal] = {}
            trips_by_driver_lease: Dict[tuple, List[int]] = {}

            for trip in trips_to_post:
                if trip.driver_id is None or trip.lease_id is None:
                    logger.warning(f"Skipping trip {trip.curb_trip_id} as it is missing driver_id or lease_id.")
                    continue
                
                # Earnings are net of taxes/surcharges, so we sum the core components
                net_earning = (trip.total_amount or Decimal("0.0"))                
                key = (trip.driver_id, trip.lease_id)
                earnings_by_driver_lease.setdefault(key, Decimal("0.0"))
                trips_by_driver_lease.setdefault(key, [])

                earnings_by_driver_lease[key] += net_earning
                trips_by_driver_lease[key].append(trip.id)

            posted_driver_count = 0
            total_posted_amount = Decimal("0.0")
            errors = []

            for (driver_id, lease_id), total_earnings in earnings_by_driver_lease.items():
                try:
                    logger.info(f"Posting earnings of ${total_earnings} for driver_id {driver_id} on lease {lease_id}.")
                    
                    # This service call will create the CREDIT posting and apply it against open balances
                    self.ledger_service.apply_weekly_earnings(
                        driver_id=driver_id,
                        earnings_amount=total_earnings,
                        lease_id=lease_id,
                    )
                    
                    # On success, update the status of the trips that were included in this batch
                    trip_ids_to_update = trips_by_driver_lease[(driver_id, lease_id)]
                    for trip_id in trip_ids_to_update:
                        self.repo.update_trip_status(trip_id, CurbTripStatus.POSTED_TO_LEDGER)
                    
                    posted_driver_count += 1
                    total_posted_amount += total_earnings
                
                except Exception as e:
                    error_msg = f"Failed to post earnings for driver {driver_id} on lease {lease_id}: {str(e)}"
                    errors.append({"driver_id": driver_id, "lease_id": lease_id, "error": error_msg})
                    logger.error(error_msg, exc_info=True)
            
            self.db.commit()
            logger.info(
                f"Manual earnings posting complete. Processed {posted_driver_count} driver/lease pairs. "
                f"Total amount posted: ${total_posted_amount}"
            )

            return {
                "drivers_processed": posted_driver_count,
                "trips_posted": len(trips_to_post) - sum(len(e) for e in errors),
                "total_amount_posted": float(total_posted_amount),
                "errors": errors
            }

        except Exception as e:
            self.db.rollback()
            logger.error("A critical error occurred during the earnings posting process: %s", e, exc_info=True)
            raise TripProcessingError(
                trip_id="batch", reason=f"A critical error occurred: {e}"
            ) from e

    # --- S3 Backup CURB trips -----------------------------        
    def import_and_reconcile_from_s3(
        self,
        start_datetime: dt.datetime,
        end_datetime: dt.datetime,
    ) -> Dict:
        """
        Import and reconcile CURB data from S3 XML files based on datetime range.

        This method:
        1. Lists XML files in S3 for the datetime range (matches exact folder structure)
        2. Downloads and parses XML files
        3. Normalizes trip data
        4. Stores in database
        5. Reconciles trips

        Args:
            start_datetime: Start datetime for import
            end_datetime: End datetime for import

        Returns:
            Dictionary with import summary
        """
        logger.info(
            f"Starting CURB import from S3 for datetime range: {start_datetime} to {end_datetime}"
        )

        try:
            # Step 1: List S3 files
            s3_files = self._list_s3_files_by_datetime_range(
                start_datetime, end_datetime
            )

            if not s3_files["transactions"] and not s3_files["trips"]:
                logger.warning("No XML files found in S3 for the specified date range")
                return {
                    "message": "No XML files found in S3 for the specified date range",
                    "records_fetched": 0,
                    "newly_inserted": 0,
                    "records_updated": 0,
                    "records_reconciled": 0,
                }

            # Step 2: Download and parse XML files
            all_trips_data = {}
            parse_errors = []

            # Process transaction files
            for s3_key in s3_files["transactions"]:
                try:
                    xml_content = self._download_and_parse_xml_from_s3(s3_key)
                    normalized_trans = self._parse_and_normalize_trips(xml_content)

                    # Deduplicate using dictionary
                    for trip in normalized_trans:
                        all_trips_data[trip["curb_trip_id"]] = trip

                    logger.info(
                        f"Parsed {len(normalized_trans)} transactions from {s3_key}"
                    )

                except Exception as e:
                    logger.error(f"Failed to process {s3_key}: {e}")
                    parse_errors.append({"file": s3_key, "error": str(e)})

            # Process trip files
            for s3_key in s3_files["trips"]:
                try:
                    xml_content = self._download_and_parse_xml_from_s3(s3_key)
                    normalized_trips = self._parse_and_normalize_trips(xml_content, filter_cash_only=True)

                    # Deduplicate using dictionary
                    for trip in normalized_trips:
                        all_trips_data[trip["curb_trip_id"]] = trip

                    logger.info(f"Parsed {len(normalized_trips)} trips from {s3_key}")

                except Exception as e:
                    logger.error(f"Failed to process {s3_key}: {e}")
                    parse_errors.append({"file": s3_key, "error": str(e)})

            final_trip_list = list(all_trips_data.values())
            logger.info(
                f"Fetched a total of {len(final_trip_list)} unique records from S3."
            )

            # Step 3: Store data in database
            inserted, updated = self.repo.bulk_insert_or_update(final_trip_list)
            self.db.commit()
            logger.info(
                f"Database operation complete: {inserted} new trips inserted, {updated} trips updated."
            )

            # Step 4: Reconcile trips
            unreconciled_trips = self.repo.get_unreconciled_trips()
            reconciled_count = 0
            reconciliation_id = None

            if unreconciled_trips:
                if settings.environment == "production":
                    logger.info(
                        "Reconciling with CURB server (production environment)."
                    )
                    trip_ids_to_reconcile = [
                        trip.curb_trip_id.split("-")[-1] for trip in unreconciled_trips
                    ]
                    reconciliation_id = f"BAT-S3-RECO-{dt.datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
                    try:
                        self.api_service.reconcile_trips(
                            trip_ids_to_reconcile, reconciliation_id
                        )
                        # Update status in local DB after successful API call
                        for trip in unreconciled_trips:
                            self.repo.update_trip_status(
                                trip.id, CurbTripStatus.RECONCILED, reconciliation_id
                            )
                        self.db.commit()
                        reconciled_count = len(unreconciled_trips)
                        logger.info(
                            f"Successfully reconciled {reconciled_count} trips with CURB API."
                        )
                    except CurbApiError as e:
                        self.db.rollback()
                        logger.error(f"Failed to reconcile trips with CURB API: {e}")
                        parse_errors.append(
                            {"reconciliation": "failed", "error": str(e)}
                        )
                else:
                    reconciled_count = self._reconcile_locally(unreconciled_trips)

            datetime_format = (
                f"{settings.common_date_format} {settings.common_time_format}"
            )
            return {
                "source": "s3",
                "datetime_range": {
                    "from": start_datetime.strftime(datetime_format),
                    "to": end_datetime.strftime(datetime_format),
                },
                "files_processed": {
                    "transactions": len(s3_files["transactions"]),
                    "trips": len(s3_files["trips"]),
                },
                "records_fetched": len(final_trip_list),
                "newly_inserted": inserted,
                "records_updated": updated,
                "records_reconciled": reconciled_count,
                "reconciliation_id": reconciliation_id,
                "parse_errors": parse_errors,
            }

        except Exception as e:
            self.db.rollback()
            logger.error(
                "An unexpected error occurred during S3 import: %s", e, exc_info=True
            )
            raise

    def _download_and_parse_xml_from_s3(self, s3_key: str) -> str:
        """
        Download XML file from S3 and return its contents.

        Args:
            s3_key: S3 object key

        Returns:
            XML content as string
        """
        logger.debug(f"Downloading XML from S3: {s3_key}")

        try:
            # Download file from S3
            file_content = s3_utils.download_file(key=s3_key)

            if file_content is None:
                raise Exception(f"Failed to download file from S3: {s3_key}")

            # If file_content is bytes, decode it
            if isinstance(file_content, bytes):
                xml_content = file_content.decode("utf-8")
            elif isinstance(file_content, BytesIO):
                xml_content = file_content.getvalue().decode("utf-8")
            else:
                xml_content = str(file_content)

            logger.debug(
                f"Successfully downloaded {len(xml_content)} bytes from {s3_key}"
            )
            return xml_content

        except Exception as e:
            logger.error(f"Error downloading/parsing XML from S3: {e}", exc_info=True)
            raise

    def _list_s3_files_by_datetime_range(
        self, start_datetime: dt.datetime, end_datetime: dt.datetime
    ) -> Dict[str, List[str]]:
        """
        List XML files from S3 within a datetime range.
        Lists all buckets and filters based on timestamp in bucket names.
        Folder structure: curb-data/MM-DD-YYYY/HH-MM-SS/

        Args:
            start_datetime: Start datetime
            end_datetime: End datetime

        Returns:
            Dictionary with 'transactions' and 'trips' lists of S3 keys
        """
        logger.info(f"Listing S3 files from {start_datetime} to {end_datetime}")

        transactions_files = []
        trips_files = []

        try:
            # List all files under the curb-data folder
            prefix = f"{settings.curb_s3_folder}/"
            all_files = s3_utils.list_files(prefix=prefix)

            logger.debug(f"Found {len(all_files)} total files in {prefix}")

            # Filter files based on datetime range from bucket names
            for file_key in all_files:
                # Skip metadata files
                if "/metadata/" in file_key or not file_key.endswith(".xml"):
                    continue

                # Extract date and time from path: curb-data/09-21-2025/12-00-00/file.xml
                path_parts = file_key.split("/")
                if len(path_parts) < 3:
                    continue

                folder_date = path_parts[-3]  # e.g., "09-21-2025"
                folder_time = path_parts[-2]  # e.g., "12-00-00"

                # Convert folder names back to datetime for comparison
                try:
                    # Replace dashes with slashes for date, and colons for time
                    date_str = folder_date.replace("-", "/")
                    time_str = folder_time.replace("-", ":")

                    # Parse using settings formats
                    datetime_str = f"{date_str} {time_str}"
                    datetime_format = (
                        f"{settings.common_date_format} {settings.common_time_format}"
                    )
                    file_datetime = dt.datetime.strptime(datetime_str, datetime_format)

                    # Check if file datetime is within range
                    if start_datetime <= file_datetime <= end_datetime:
                        if "transactions_" in file_key:
                            transactions_files.append(file_key)
                        elif "trips_" in file_key:
                            trips_files.append(file_key)

                except ValueError as e:
                    logger.warning(
                        f"Failed to parse datetime from path {file_key}: {e}"
                    )
                    continue

        except Exception as e:
            logger.error(f"Failed to list files from S3: {e}", exc_info=True)
            raise

        logger.info(
            f"Found {len(transactions_files)} transaction files and {len(trips_files)} trip files in datetime range"
        )

        return {"transactions": transactions_files, "trips": trips_files}


# --- Celery Tasks ---

@app.task(
    bind=True,
    name="curb.post_earnings_to_ledger",
    max_retries=3,
    default_retry_delay=60,
)
def post_earnings_to_ledger_task(
    self,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lease_id: Optional[int] = None,
    driver_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Celery task to post MAPPED credit card earnings to the centralized ledger.

    This task is designed to run as part of the Sunday financial processing chain.
    It finds all MAPPED credit card trips for the specified period and posts them
    as earnings (CREDIT entries) to the ledger, applying them against open balances.

    Args:
        start_date: Start date in YYYY-MM-DD format (defaults to previous Sunday)
        end_date: End date in YYYY-MM-DD format (defaults to previous Saturday)
        lease_id: Optional filter for specific lease
        driver_id: Optional filter for specific driver

    Returns:
        Dictionary with posting results including:
        - drivers_processed: Number of unique drivers processed
        - trips_posted: Number of trips successfully posted
        - total_amount_posted: Total dollar amount posted to ledger
        - errors: List of any posting errors

    Example:
        {
            "drivers_processed": 25,
            "trips_posted": 150,
            "total_amount_posted": 12500.75,
            "errors": []
        }
    """
    from app.core.db import SessionLocal
    import datetime as dt
    from datetime import timedelta

    logger.info("*" * 80)
    logger.info("Starting CURB earnings posting task")

    db = None
    try:
        # Initialize database session and service
        db = SessionLocal()
        curb_service = CurbService(db)

        # Calculate date range if not provided (previous Sunday to Saturday)
        if not start_date or not end_date:
            now = dt.datetime.now()
            # Find last Sunday (week starts on Sunday)
            days_since_sunday = (now.weekday() + 1) % 7  # Monday=0 becomes 1, Sunday=6 becomes 0
            last_sunday = now - timedelta(days=days_since_sunday + 7)  # Go back to previous Sunday
            last_saturday = last_sunday + timedelta(days=6)  # End of that week

            if not start_date:
                start_date = last_sunday.strftime("%Y-%m-%d")
            if not end_date:
                end_date = last_saturday.strftime("%Y-%m-%d")

        # Convert string dates to date objects
        start_date_obj = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = dt.datetime.strptime(end_date, "%Y-%m-%d").date()

        logger.info(f"Posting earnings for period: {start_date_obj} to {end_date_obj}")
        if lease_id:
            logger.info(f"Filtering by lease ID: {lease_id}")
        if driver_id:
            logger.info(f"Filtering by driver ID: {driver_id}")

        # Post earnings to ledger
        result = curb_service.post_mapped_earnings_to_ledger(
            start_date=start_date_obj,
            end_date=end_date_obj,
            lease_id=lease_id,
            driver_id=driver_id
        )

        logger.info(
            f"Earnings posting completed: {result.get('drivers_processed', 0)} drivers processed, "
            f"${result.get('total_amount_posted', 0.0)} posted to ledger"
        )

        logger.info("*" * 80)
        return result

    except Exception as e:
        logger.error(f"Earnings posting task failed: {str(e)}", exc_info=True)
        if db:
            db.rollback()
        raise self.retry(exc=e, countdown=60)

    finally:
        if db:
            db.close()

