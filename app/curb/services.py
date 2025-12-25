# app/curb/services.py

"""
CURB Service Layer - Business Logic

Handles all CURB business operations including:
- Multi-account data import from CURB API
- S3 data lake integration
- Driver/Lease/Vehicle mapping
- Individual trip ledger posting
- Reconciliation (server or local per account)
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import BytesIO
from typing import Dict, List, Optional

import requests
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.curb.exceptions import (
    CurbApiError, CurbDataParsingError, CurbReconciliationError,
)
from app.curb.models import CurbAccount, CurbTrip, CurbTripStatus, PaymentType, ReconciliationMode
from app.curb.repository import CurbRepository
from app.drivers.models import Driver
from app.leases.services import lease_service
from app.ledger.models import PostingCategory, EntryType
from app.ledger.repository import LedgerRepository
from app.ledger.services import LedgerService
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)


class CurbApiService:
    """
    Low-level CURB API client
    
    Handles SOAP requests to CURB API endpoints.
    """
    
    def __init__(self, account: CurbAccount):
        self.account = account
        self.base_url = account.api_url
        self.merchant = account.merchant_id
        self.username = account.username
        self.password = account.password

    def _build_soap_envelope(self, method: str, params: dict) -> str:
        """Build SOAP envelope for CURB API calls"""
        params_xml = "".join([f"<{k}>{v}</{k}>" for k, v in params.items()])
        
        envelope = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
                    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <{method} xmlns="https://www.taxitronic.org/VTS_SERVICE/">
                <UserId>{self.username}</UserId>
                <Password>{self.password}</Password>
                <Merchant>{self.merchant}</Merchant>
                {params_xml}
                </{method}>
            </soap:Body>
        </soap:Envelope>"""
        
        return envelope
    
    def _make_request(self, method: str, params: dict) -> str:
        """Make SOAP request to CURB API"""
        envelope = self._build_soap_envelope(method, params)
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f"https://www.taxitronic.org/VTS_SERVICE/{method}"
        }
        
        try:
            response = requests.post(
                self.base_url,
                data=envelope,
                headers=headers,
                timeout=60
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"CURB API request failed for account {self.account.account_name}: {e}")
            raise CurbApiError(f"CURB API request failed: {e}")
        
    def get_trips_log10(
        self, from_datetime: str, to_datetime: str, recon_stat: int = 0
    ) -> str:
        """
        Call GET_TRIPS_LOG10 endpoint
        
        Args:
            from_datetime: Start datetime (MM/DD/YYYY HH:MM:SS)
            to_datetime: End datetime (MM/DD/YYYY HH:MM:SS)
            recon_stat: Reconciliation status filter
                       0 = only unreconciled (default)
                       <0 = all records
                       >0 = specific reconciliation ID
        
        Returns:
            Raw XML response
        """
        params = {
            "DRIVERID": "",  # Blank = all drivers
            "CABNUMBER": "",  # Blank = all cabs
            "DATE_FROM": from_datetime,
            "DATE_TO": to_datetime,
            "RECON_STAT": recon_stat
        }
        
        return self._make_request("GET_TRIPS_LOG10", params)
    
    def get_trans_by_date_cab12(
        self, from_datetime: str, to_datetime: str,
        cab_number: str = "", tran_type: str = "ALL"
    ) -> str:
        """
        Call Get_Trans_By_Date_Cab12 endpoint
        
        Returns all card transactions (credit card, mobile payments, etc.)
        
        Args:
            from_datetime: Start datetime (MM/DD/YYYY HH:MM:SS)
            to_datetime: End datetime (MM/DD/YYYY HH:MM:SS)
            cab_number: Specific cab/medallion (blank = all)
            tran_type: Transaction type filter:
                      - "AP" = approved transactions only
                      - "DC" = failed transactions only
                      - "DUP" = duplicates only
                      - "ALL" = all transactions (default)
        
        Returns:
            Raw XML response
        """
        params = {
            "fromDateTime": from_datetime,
            "ToDateTime": to_datetime,
            "CabNumber": cab_number,
            "TranType": tran_type
        }
        
        return self._make_request("Get_Trans_By_Date_Cab12", params)
    
    def reconcile_trips(self, trip_ids: List[str], reconciliation_id: str) -> str:
        """
        Call Reconciliation_TRIP_LOG endpoint
        
        Marks trips as reconciled on CURB server.
        """
        params = {
            "DATE_FROM": datetime.now(timezone.utc).strftime("%m/%d/%Y"),
            "RECON_STAT": reconciliation_id,
            "ListIDs": ",".join(trip_ids)
        }
        
        return self._make_request("Reconciliation_TRIP_LOG", params)
    

class CurbService:
    """
    Main CURB service orchestrator
    
    Coordinates data import, mapping, reconciliation, and ledger posting.
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.repo = CurbRepository(db)
        self.ledger_repo = LedgerRepository(db)
        self.ledger_service = LedgerService(self.ledger_repo)

    # --- DATA IMPORT FROM CURB API ---

    def import_trips_from_accounts(
        self,
        account_ids: Optional[List[int]] = None,
        from_datetime: Optional[datetime] = None,
        to_datetime: Optional[datetime] = None,
    ) -> Dict:
        """
        Import CASH trips from one or more CURB accounts
        
        Process:
        1. Get active accounts (or specific accounts if provided)
        2. For each account, call GET_TRIPS_LOG10 API
        3. Store raw XML to S3 data lake
        4. Parse XML and extract CASH trips only
        5. Map to driver/lease/vehicle
        6. Store in database with IMPORTED status
        7. Reconcile based on account config (server or local)
        
        Args:
            account_ids: Specific accounts to import from (None = all active)
            from_datetime: Start datetime (default: 3 hours ago)
            to_datetime: End datetime (default: now)
        """
        start_time = datetime.now()
        
        # Set datetime range (3-hour window by default)
        if not to_datetime:
            to_datetime = datetime.now(timezone.utc)
        if not from_datetime:
            from_datetime = to_datetime - timedelta(hours=3)
        
        # Get accounts to process
        if account_ids:
            accounts = [self.repo.get_account_by_id(aid) for aid in account_ids]
        else:
            accounts = self.repo.get_active_accounts()
        
        if not accounts:
            return {
                "status": "no_accounts",
                "message": "No active CURB accounts found",
                "accounts_processed": [],
            }
        
        logger.info(f"Starting CURB import for {len(accounts)} account(s) from {from_datetime} to {to_datetime}")
        
        all_trips = []
        account_results = []
        errors = []
        
        for account in accounts:
            try:
                account_trips = self._import_from_account(
                    account,
                    from_datetime,
                    to_datetime
                )
                
                all_trips.extend(account_trips)
                account_results.append({
                    "account_id": account.id,
                    "account_name": account.account_name,
                    "trips_fetched": len(account_trips),
                    "status": "success"
                })
                
                logger.info(f"Account '{account.account_name}': fetched {len(account_trips)} CASH trips")
                
            except Exception as e:
                logger.error(f"Failed to import from account '{account.account_name}': {e}", exc_info=True)
                errors.append({
                    "account_id": account.id,
                    "account_name": account.account_name,
                    "error": str(e)
                })
        
        # Bulk insert all trips from all accounts
        trips_imported, trips_updated = 0, 0
        if all_trips:
            trips_imported, trips_updated = self.repo.bulk_insert_or_update_trips(all_trips)
            self.db.commit()
        
        # Reconciliation
        reconciliation_details = self._handle_reconciliation(accounts)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        return {
            "status": "success",
            "message": f"Imported from {len(accounts)} account(s)",
            "accounts_processed": account_results,
            "datetime_range": {
                "from": from_datetime.isoformat(),
                "to": to_datetime.isoformat()
            },
            "total_trips_fetched": len(all_trips),
            "trips_imported": trips_imported,
            "trips_updated": trips_updated,
            "trips_skipped": len(all_trips) - trips_imported - trips_updated,
            "reconciled_count": reconciliation_details.get("total_reconciled", 0),
            "reconciliation_details": reconciliation_details,
            "processing_time_seconds": round(processing_time, 2),
            "errors": errors,
        }
    
    def _import_from_account(
        self,
        account: CurbAccount,
        from_datetime: datetime,
        to_datetime: datetime,
    ) -> List[Dict]:
        """
        Import trips from a single CURB account
        
        Fetches from BOTH endpoints:
        1. GET_TRIPS_LOG10 - All trip data (CASH, credit card, etc.)
        2. Get_Trans_By_Date_Cab12 - Card transaction data
        
        Returns:
            List of parsed trip dictionaries ready for database insertion
        """
        api_service = CurbApiService(account)
        
        # Format datetime for CURB API
        from_str = from_datetime.strftime("%m/%d/%Y %H:%M:%S")
        to_str = to_datetime.strftime("%m/%d/%Y %H:%M:%S")
        
        all_trips = []
        
        # --- CALL 1: GET_TRIPS_LOG10 - Get all trips (CASH and credit cards) ---

        try:
            logger.info(f"Fetching trips from GET_TRIPS_LOG10 for {account.account_name}")
            trips_xml = api_service.get_trips_log10(from_str, to_str, recon_stat=-1)
            
            # Store to S3 data lake
            self._store_to_s3(account, trips_xml, from_datetime, "trips")
            
            # Parse trips (all payment types)
            trips = self._parse_trips_xml(trips_xml, account.id)
            all_trips.extend(trips)
            logger.info(f"GET_TRIPS_LOG10: Parsed {len(trips)} trips")
            
        except Exception as e:
            logger.error(f"Failed to fetch GET_TRIPS_LOG10 for {account.account_name}: {e}")
        
        # --- CALL 2: Get_Trans_By_Date_Cab12 - Get card transactions ---
        
        try:
            logger.info(f"Fetching transactions from Get_Trans_By_Date_Cab12 for {account.account_name}")
            trans_xml = api_service.get_trans_by_date_cab12(from_str, to_str)
            
            # Store to S3 data lake
            self._store_to_s3(account, trans_xml, from_datetime, "transactions")
            
            # Parse transactions
            transactions = self._parse_transactions_xml(trans_xml, account.id)
            all_trips.extend(transactions)
            logger.info(f"Get_Trans_By_Date_Cab12: Parsed {len(transactions)} transactions")
            
        except Exception as e:
            logger.error(f"Failed to fetch Get_Trans_By_Date_Cab12 for {account.account_name}: {e}")
        
        # Deduplicate by curb_trip_id (in case same trip appears in both endpoints)
        unique_trips = {}
        for trip in all_trips:
            trip_id = trip.get("curb_trip_id")
            if trip_id:
                # Keep the one with more complete data (prefer transactions data)
                if trip_id not in unique_trips or trip.get("payment_type") != PaymentType.CASH:
                    unique_trips[trip_id] = trip
        
        deduplicated_trips = list(unique_trips.values())
        logger.info(f"Total unique trips after deduplication: {len(deduplicated_trips)}")
        
        # Map to internal entities
        mapped_trips = []
        for trip in deduplicated_trips:
            try:
                mapped_trip = self._map_trip_to_entities(trip)
                if mapped_trip:
                    mapped_trips.append(mapped_trip)
            except Exception as e:
                logger.warning(f"Failed to map trip {trip.get('curb_trip_id')}: {e}")
                # Still include trip but without mapping
                mapped_trips.append(trip)
        
        logger.info(f"Mapped {len(mapped_trips)} trips to internal entities")
        return mapped_trips
    
    def _parse_trips_xml(self, xml_response: str, account_id: int) -> List[Dict]:
        """
        Parse CURB GET_TRIPS_LOG10 XML response
        
        Extracts ALL payment types (CASH, credit card, private card, etc.)
        
        Returns:
            List of trip dictionaries
        """
        try:
            root = ET.fromstring(xml_response)
            trips = []
            
            # Extract the inner XML from GET_TRIPS_LOG10 element
            result_element = root.find(".//{https://www.taxitronic.org/VTS_SERVICE/}GET_TRIPS_LOG10Result")
            
            if result_element is None or not result_element.text:
                logger.warning("No GET_TRIPS_LOG10Result found in response")
                return []
            
            # Parse the inner XML (trans/tran elements are in the text content)
            inner_xml = result_element.text
            inner_root = ET.fromstring(f"<root>{inner_xml}</root>")  # Wrap in root to parse properly
            
            # Navigate to TRIPS/RECORD elements
            for record in inner_root.findall(".//RECORD"):
                # Map payment type from T attribute
                payment_type_code = record.get("T", "")
                payment_type = self._map_payment_type(payment_type_code)
                
                trip = {
                    "account_id": account_id,
                    "curb_trip_id": f"{record.get('PERIOD')}-{record.get('ID')}",
                    "curb_driver_id": record.get("DRIVER", ""),
                    "curb_cab_number": record.get("CABNUMBER", ""),
                    "status": CurbTripStatus.IMPORTED,
                    "payment_type": payment_type,
                    
                    # Parse timestamps
                    "start_time": self._parse_datetime(record.get("START_DATE")),
                    "end_time": self._parse_datetime(record.get("END_DATE")),
                    
                    # Financial amounts
                    "fare": Decimal(record.get("TRIP", "0.00")),
                    "tips": Decimal(record.get("TIPS", "0.00")),
                    "tolls": Decimal(record.get("TOLLS", "0.00")),
                    "extras": Decimal(record.get("EXTRAS", "0.00")),
                    "total_amount": Decimal(record.get("TOTAL_AMOUNT", "0.00")),
                    
                    # Tax breakdown
                    "surcharge": Decimal(record.get("TAX", "0.00")),
                    "improvement_surcharge": Decimal(record.get("IMPTAX", "0.00")),
                    "congestion_fee": Decimal(record.get("CONGFEE", "0.00")),
                    "airport_fee": Decimal(record.get("airportFee", "0.00")),
                    "cbdt_fee": Decimal(record.get("cbdt", "0.00")),
                    
                    # Additional data
                    "distance_miles": Decimal(record.get("DIST_SERVCE", "0.00")),
                    "num_passengers": int(record.get("PASSENGER_NUM", 1)),

                    # Coordinates
                    "start_long": Decimal(record.get("GPS_START_LO", "0.00")),
                    "start_lat": Decimal(record.get("GPS_START_LA", "0.00")),
                    "end_long": Decimal(record.get("GPS_END_LO", "0.00")),
                    "end_lat": Decimal(record.get("GPS_END_LA", "0.00")),

                    # Number of services
                    "num_service": int(record.get("NUM_SERVICE", 1)),

                    # Transaction Date
                    "transaction_date": self._parse_datetime(record.get("END_DATE")),
                }
                
                trips.append(trip)
            
            logger.info(f"Parsed {len(trips)} trips from GET_TRIPS_LOG10 XML")
            return trips
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse GET_TRIPS_LOG10 XML: {e}")
            raise CurbDataParsingError(f"XML parsing failed: {e}") from e
        
    def _parse_transactions_xml(self, xml_response: str, account_id: int) -> List[Dict]:
        """
        Parse CURB Get_Trans_By_Date_Cab12 XML response
        
        Extracts credit card and other non-cash transactions.
        
        Returns:
            List of transaction dictionaries
        """
        try:
            # Parse outer SOAP envelope
            root = ET.fromstring(xml_response)
            transactions = []
            
            # Extract the inner XML from Get_Trans_By_Date_Cab12Result element
            result_element = root.find(".//{https://www.taxitronic.org/VTS_SERVICE/}Get_Trans_By_Date_Cab12Result")
            
            if result_element is None or not result_element.text:
                logger.warning("No Get_Trans_By_Date_Cab12Result found in response")
                return []
            
            # Parse the inner XML (trans/tran elements are in the text content)
            inner_xml = result_element.text
            inner_root = ET.fromstring(f"<root>{inner_xml}</root>")  # Wrap in root to parse properly
            
            # Now find tran elements (no namespace in inner XML)
            for tran in inner_root.findall(".//tran"):
                # Get ROWID attribute (unique transaction ID)
                rowid = tran.get("ROWID", "")
                
                # Extract nested elements
                trip_date = self._get_element_text(tran, "TRIPDATE")
                trip_time_start = self._get_element_text(tran, "TRIPTIMESTART")
                trip_time_end = self._get_element_text(tran, "TRIPTIMEEND")
                
                # Combine date and time
                start_time = self._parse_transaction_datetime(trip_date, trip_time_start)
                end_time = self._parse_transaction_datetime(trip_date, trip_time_end)
                
                # Map CC_TYPE to payment type
                cc_type = self._get_element_text(tran, "CC_TYPE")
                payment_type = self._map_cc_type_to_payment(cc_type)
                
                transaction = {
                    "account_id": account_id,
                    "curb_trip_id": f"TRANS-{rowid}",  # Use ROWID as unique ID
                    "curb_driver_id": self._get_element_text(tran, "TRIPDRIVERID"),
                    "curb_cab_number": self._get_element_text(tran, "CABNUMBER"),
                    "status": CurbTripStatus.IMPORTED,
                    "payment_type": payment_type,
                    
                    # Timestamps
                    "start_time": start_time,
                    "end_time": end_time,
                    
                    # Financial amounts
                    "fare": Decimal(self._get_element_text(tran, "TRIPTRIPS", "0.00")),
                    "tips": Decimal(self._get_element_text(tran, "TRIPTIPS", "0.00")),
                    "tolls": Decimal(self._get_element_text(tran, "TRIPTOLL", "0.00")),
                    "extras": Decimal(self._get_element_text(tran, "TRIPEXTRAS", "0.00")),
                    "total_amount": Decimal(self._get_element_text(tran, "AMOUNT", "0.00")),
                    
                    # Tax breakdown
                    "surcharge": Decimal(self._get_element_text(tran, "TAX", "0.00")),
                    "improvement_surcharge": Decimal(self._get_element_text(tran, "IMPTAX", "0.00")),
                    "congestion_fee": Decimal(self._get_element_text(tran, "CongFee", "0.00")),
                    "airport_fee": Decimal(self._get_element_text(tran, "airportFee", "0.00")),
                    "cbdt_fee": Decimal(self._get_element_text(tran, "cbdt", "0.00")),
                    
                    # Additional data
                    "distance_miles": Decimal(self._get_element_text(tran, "TRIPDIST", "0.00")),
                    "num_passengers": 1,  # Not provided in transaction data

                    # Coordinates (not provided in transaction data)
                    "start_long": Decimal(self._get_element_text(tran, "From_Lo", "0.00")),
                    "start_lat": Decimal(self._get_element_text(tran, "From_La", "0.00")),
                    "end_long": Decimal(self._get_element_text(tran, "To_Lo", "0.00")),
                    "end_lat": Decimal(self._get_element_text(tran, "To_La", "0.00")),

                    # Number of services (not provided in transaction data)
                    "num_service": self._get_element_text(tran, "NUM_SERVICE", 1),

                    # Transaction Date
                    "transaction_date": self._parse_datetime(self._get_element_text(tran, "DATETIME")),
                }
                
                transactions.append(transaction)
            
            logger.info(f"Parsed {len(transactions)} transactions from Get_Trans_By_Date_Cab12 XML")
            return transactions
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse Get_Trans_By_Date_Cab12 XML: {e}")
            raise CurbDataParsingError(f"Transaction XML parsing failed: {e}") from e
        
    def _get_element_text(self, parent: ET.Element, tag_name: str, default: str = "") -> str:
        """Helper to safely extract text from XML element (no namespace for inner transaction XML)"""
        element = parent.find(f".//{tag_name}")
        if element is not None and element.text:
            return element.text.strip()
        return default
    
    def _map_payment_type(self, type_code: str) -> PaymentType:
        """
        Map GET_TRIPS_LOG10 T attribute to PaymentType enum
        
        T values:
        - "$" = CASH
        - "C" = CCARD (credit card/mobile)
        - "P" = PRIVATE CARD
        """
        mapping = {
            "$": PaymentType.CASH,
            "C": PaymentType.CREDIT_CARD,
            "P": PaymentType.CREDIT_CARD,  # Private cards treated as credit
        }
        return mapping.get(type_code, PaymentType.UNKNOWN)
    
    def _map_cc_type_to_payment(self, cc_type: str) -> PaymentType:
        """
        Map Get_Trans_By_Date_Cab12 CC_TYPE to PaymentType enum
        
        CC_TYPE values from documentation:
        1 = Visa, 2 = AMEX, 3 = Mastercard, 4 = Discover, 5 = Diners Club,
        6 = JCB, 7 = VeriPay, 8 = Debit, 9 = PayPal, 10 = Other, 
        11 = Split, 12 = Virtual Card
        
        All are considered credit card transactions
        """
        return PaymentType.CREDIT_CARD
    
    def _parse_transaction_datetime(self, date_str: str, time_str: str) -> datetime:
        """
        Parse transaction date and time into datetime
        
        Args:
            date_str: Date in MM/DD/YYYY format
            time_str: Time in HH:MM format
        """
        try:
            # Combine date and time
            datetime_str = f"{date_str} {time_str}:00"
            return datetime.strptime(datetime_str, "%m/%d/%Y %H:%M:%S").replace(tzinfo=timezone.utc)
        except:
            return datetime.now(timezone.utc)
        
    def _parse_datetime(self, date_str: str) -> datetime:
        """Parse CURB datetime string: MM/DD/YYYY HH:MM:SS"""
        try:
            return datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S").replace(tzinfo=timezone.utc)
        except:
            return datetime.now(timezone.utc)
        
    def _map_trip_to_entities(self, trip: Dict) -> Dict:
        """
        Map CURB identifiers to internal driver/lease/vehicle/medallion
        
        Logic:
        1. Find driver by curb_driver_id (matches TLC license number)
        2. Find active lease for that driver at trip start time
        3. Get vehicle and medallion from the lease
        """
        driver_id = None
        lease_id = None
        vehicle_id = None
        medallion_id = None
        
        # Find driver by TLC license matching curb_driver_id
        driver = self.db.query(Driver).join(Driver.tlc_license).filter(
            Driver.tlc_license.has(tlc_license_number=trip["curb_driver_id"])
        ).first()
        
        if driver:
            driver_id = driver.driver_id
            
            # Find active lease at trip start time
            active_lease = lease_service.get_lease(
                self.db, driver_id=driver_id, medallion_number=trip["curb_cab_number"]
            )
            
            if active_lease:
                lease_id = active_lease.id
                vehicle_id = active_lease.vehicle_id
                medallion_id = active_lease.medallion_id
        
            # Update trip with mapped IDs
            trip["driver_id"] = driver.id
            trip["lease_id"] = lease_id
            trip["vehicle_id"] = vehicle_id
            trip["medallion_id"] = medallion_id
            
            return trip
        return None
    
    def _store_to_s3(self, account: CurbAccount, xml_content: str, timestamp: datetime, data_type: str = "trips"):
        """
        Store raw XML to S3 data lake
        
        Args:
            account: CURB account
            xml_content: Raw XML response
            timestamp: Import timestamp
            data_type: "trips" or "transactions"
        """
        try:
            # S3 path structure: curb/{data_type}/{account_name}/YYYY-MM-DD-HH/{timestamp}.xml
            s3_path = (
                f"curb/{data_type}/{account.account_name}/"
                f"{timestamp.strftime('%Y-%m-%d-%H')}/"
                f"{timestamp.strftime('%Y%m%d%H%M%S')}.xml"
            )
            
            xml_bytes = xml_content.encode('utf-8')
            file_obj = BytesIO(xml_bytes)
            
            s3_utils.upload_file(file_obj, s3_path, content_type="application/xml")
            logger.info(f"Stored {data_type} XML to S3: {s3_path}")
            
        except Exception as e:
            logger.error(f"Failed to store {data_type} to S3: {e}")
            # Don't fail the import if S3 fails

    # --- RECONCILIATION ---

    def _handle_reconciliation(self, accounts: List[CurbAccount]) -> Dict:
        """Handle reconciliation for all accounts based on their config"""
        results = {
            "server_reconciled": 0,
            "local_reconciled": 0,
            "total_reconciled": 0,
            "errors": []
        }
        
        for account in accounts:
            try:
                if account.reconciliation_mode == ReconciliationMode.SERVER:
                    count = self._reconcile_with_server(account)
                    results["server_reconciled"] += count
                else:
                    count = self._reconcile_locally(account)
                    results["local_reconciled"] += count
                
                results["total_reconciled"] += count
                
            except Exception as e:
                logger.error(f"Reconciliation failed for {account.account_name}: {e}")
                results["errors"].append({
                    "account": account.account_name,
                    "error": str(e)
                })
        
        return results
    
    def _reconcile_with_server(self, account: CurbAccount) -> int:
        """Reconcile trips with CURB API (server-side)"""
        # Get unreconciled trips for this account
        trips = self.db.query(CurbTrip).filter(
            and_(
                CurbTrip.account_id == account.id,
                CurbTrip.reconciliation_id.is_(None)
            )
        ).limit(1000).all()  # Batch size
        
        if not trips:
            return 0
        
        # Extract CURB trip IDs (format: PERIOD-ID, we need just ID)
        trip_ids = [trip.curb_trip_id.split('-')[1] for trip in trips]
        reconciliation_id = f"BAT-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        
        try:
            api_service = CurbApiService(account)
            api_service.reconcile_trips(trip_ids, reconciliation_id)
            
            # Mark as reconciled in database
            internal_trip_ids = [trip.id for trip in trips]
            count = self.repo.mark_trips_as_reconciled(internal_trip_ids, reconciliation_id)
            self.db.commit()
            
            logger.info(f"Server reconciled {count} trips for {account.account_name}")
            return count
            
        except Exception as e:
            self.db.rollback()
            raise CurbReconciliationError(f"Server reconciliation failed: {e}")
        
    def _reconcile_locally(self, account: CurbAccount) -> int:
        """Mark trips as reconciled locally (no API call)"""
        trips = self.db.query(CurbTrip).filter(
            and_(
                CurbTrip.account_id == account.id,
                CurbTrip.reconciliation_id.is_(None)
            )
        ).limit(1000).all()
        
        if not trips:
            return 0
        
        reconciliation_id = f"LOCAL-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        trip_ids = [trip.id for trip in trips]
        
        count = self.repo.mark_trips_as_reconciled(trip_ids, reconciliation_id)
        self.db.commit()
        
        logger.info(f"Locally reconciled {count} trips for {account.account_name}")
        return count

    # --- LEDGER POSTING ---

    def post_trips_to_ledger(
        self,
        start_date: datetime,
        end_date: datetime,
        driver_ids: Optional[List[int]] = None,
        lease_ids: Optional[List[int]] = None,
    ) -> Dict:
        """
        Post CURB trips to ledger with proper handling of refunds and zero amounts
        
        Scenarios:
        1. Positive total_amount → Post as CREDIT (earnings)
        2. Negative total_amount → Post as DEBIT (refund/chargeback)
        3. Zero total_amount → Skip (no financial impact)
        
        Note: Taxes are included in total_amount by CURB API
        """
        
        logger.info(f"Posting CURB trips to ledger from {start_date} to {end_date}")
        
        trips = self.repo.get_trips_ready_for_ledger(
            start_date, end_date, driver_ids, lease_ids
        )
        
        if not trips:
            return {
                "status": "no_trips",
                "message": "No trips ready for ledger posting",
                "trips_processed": 0,
            }
        
        posted_count = 0
        failed_count = 0
        skipped_zero_count = 0
        refund_count = 0
        postings_created = []
        errors = []
        total_amount = Decimal("0.00")
        
        for trip in trips:
            try:
                # ====== SCENARIO 1: Zero Amount - Skip ======
                if trip.total_amount == 0:
                    skipped_zero_count += 1
                    logger.info(
                        f"Skipping trip {trip.curb_trip_id} - zero net amount "
                        f"(Taxes: MTA=${trip.surcharge}, Cong=${trip.congestion_fee})"
                    )
                    trip.status = CurbTripStatus.POSTED_TO_LEDGER
                    trip.posted_to_ledger_at = datetime.now()
                    trip.ledger_posting_ref = "SKIPPED-ZERO-AMOUNT"
                    continue
                
                # ====== SCENARIO 2: Negative Amount - Refund/Chargeback ======
                elif trip.total_amount < 0:
                    refund_count += 1
                    logger.warning(
                        f"Processing REFUND for trip {trip.curb_trip_id}: "
                        f"${trip.total_amount} "
                        f"(Fare: ${trip.fare}, Taxes: ${self._calculate_taxes(trip)})"
                    )
                    
                    # For refunds, we need to DEBIT (reduce earnings)
                    # But create_obligation expects positive amounts for DEBIT
                    # So we use absolute value and DEBIT entry type
                    reference_id = f"CURB-REFUND-{trip.curb_trip_id}"
                    
                    posting, balance = self.ledger_service.create_obligation(
                        category=PostingCategory.EARNINGS,
                        amount=abs(trip.total_amount),  # Use absolute value
                        entry_type=EntryType.DEBIT,     # DEBIT reduces earnings
                        reference_id=reference_id,
                        driver_id=trip.driver_id,
                        lease_id=trip.lease_id,
                        vehicle_id=trip.vehicle_id,
                        medallion_id=trip.medallion_id,
                    )
                    
                    trip.status = CurbTripStatus.POSTED_TO_LEDGER
                    trip.posted_to_ledger_at = datetime.now()
                    trip.ledger_posting_ref = posting.id
                    
                    posted_count += 1
                    total_amount += trip.total_amount  # Keep negative for reporting
                    
                    postings_created.append({
                        "trip_id": trip.curb_trip_id,
                        "driver_id": trip.driver_id,
                        "amount": float(trip.total_amount),
                        "type": "REFUND",
                        "posting_id": posting.id,
                    })
                
                # ====== SCENARIO 3: Positive Amount - Normal Earnings ======
                else:  # trip.total_amount > 0
                    reference_id = f"CURB-TRIP-{trip.curb_trip_id}"
                    
                    posting, balance = self.ledger_service.create_obligation(
                        category=PostingCategory.EARNINGS,
                        amount=trip.total_amount,      # Positive amount
                        entry_type=EntryType.CREDIT,   # CREDIT for earnings
                        reference_id=reference_id,
                        driver_id=trip.driver_id,
                        lease_id=trip.lease_id,
                        vehicle_id=trip.vehicle_id,
                        medallion_id=trip.medallion_id,
                    )
                    
                    trip.status = CurbTripStatus.POSTED_TO_LEDGER
                    trip.posted_to_ledger_at = datetime.now()
                    trip.ledger_posting_ref = posting.id
                    
                    posted_count += 1
                    total_amount += trip.total_amount
                    
                    postings_created.append({
                        "trip_id": trip.curb_trip_id,
                        "driver_id": trip.driver_id,
                        "amount": float(trip.total_amount),
                        "type": "EARNING",
                        "posting_id": posting.id,
                    })
                
                # Commit every 100 trips
                if (posted_count + refund_count) % 100 == 0:
                    self.db.commit()
                    logger.info(
                        f"Progress: {posted_count} earnings, "
                        f"{refund_count} refunds, "
                        f"{skipped_zero_count} skipped"
                    )
                
            except Exception as e:
                failed_count += 1
                logger.error(
                    f"Failed to post trip {trip.curb_trip_id}: {e}",
                    exc_info=True
                )
                errors.append({
                    "trip_id": trip.curb_trip_id,
                    "amount": float(trip.total_amount),
                    "error": str(e)
                })
        
        # Final commit
        self.db.commit()
        
        logger.info(
            f"CURB ledger posting completed: "
            f"{posted_count} earnings posted, "
            f"{refund_count} refunds posted, "
            f"{skipped_zero_count} zero-amount skipped, "
            f"{failed_count} failed"
        )
        
        return {
            "status": "success",
            "message": f"Posted {posted_count + refund_count} trips to ledger",
            "trips_processed": posted_count,
            "refunds_processed": refund_count,
            "trips_skipped": skipped_zero_count,
            "trips_failed": failed_count,
            "total_amount": total_amount,
            "postings_created": postings_created[:10],
            "errors": errors[:10] if errors else [],
        }


    def _calculate_taxes(self, trip: CurbTrip) -> Decimal:
        """Calculate total taxes/fees for a trip"""
        return (
            trip.surcharge + 
            trip.improvement_surcharge + 
            trip.congestion_fee + 
            trip.airport_fee + 
            trip.cbdt_fee
        )