import logging
import xml.dom.minidom as minidom
import xml.etree.ElementTree as ET
from typing import List

import requests

logger = logging.getLogger(__name__)


class CurbApiError(Exception):
    """Custom exception for CURB API errors."""

    pass


class CurbApiService:
    """
    Handles low-level communication with the CURB SOAP API.
    """

    def __init__(self, base_url: str, merchant: str, username: str, password: str):
        self.base_url = base_url
        self.merchant = merchant
        self.username = username
        self.password = password
        self.headers = {"Content-Type": "text/xml; charset=utf-8"}

    def _make_soap_request(self, soap_action: str, payload: str) -> str:
        """Makes a SOAP request to the CURB API and returns a formatted XML response."""
        try:
            full_action = f"https://www.taxitronic.org/VTS_SERVICE/{soap_action}"
            self.headers["SOAPAction"] = full_action
            response = requests.post(
                self.base_url,
                data=payload.encode("utf-8"),
                headers=self.headers,
                timeout=120,
            )
            response.raise_for_status()

            # Extract the string content from within the Result tag
            root = ET.fromstring(response.content)
            namespace = "https://www.taxitronic.org/VTS_SERVICE/"
            result_tag = f"{{{namespace}}}{soap_action}Result"
            result_element = root.find(f".//{result_tag}")

            if result_element is None:
                raise CurbApiError(
                    f"'{soap_action}Result' tag not found in SOAP response."
                )

            # Pretty print the XML content
            raw_xml = result_element.text
            parsed_xml = minidom.parseString(raw_xml)
            pretty_xml = parsed_xml.toprettyxml(indent="  ")

            return pretty_xml

        except requests.exceptions.RequestException as e:
            logger.error("CURB API request failed: %s", e, exc_info=True)
            raise CurbApiError(f"Network error communicating with CURB API: {e}") from e
        except ET.ParseError as e:
            logger.error("Failed to parse CURB API SOAP response: %s", e, exc_info=True)
            raise CurbApiError("Invalid XML response from CURB API.") from e
        except Exception as e:
            logger.error(
                "Unexpected error while processing SOAP response: %s", e, exc_info=True
            )
            raise

    def get_trips_log10(
        self, from_date: str, to_date: str, driver_id: str = "", cab_number: str = ""
    ) -> str:
        """
        Fetches trip data from the GET_TRIPS_LOG10 endpoint.
        """
        payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <GET_TRIPS_LOG10 xmlns="https://www.taxitronic.org/VTS_SERVICE/">
              <UserId>{self.username}</UserId>
              <Password>{self.password}</Password>
              <Merchant>{self.merchant}</Merchant>
              <DRIVERID>{driver_id}</DRIVERID>
              <CABNUMBER>{cab_number}</CABNUMBER>
              <DATE_FROM>{from_date}</DATE_FROM>
              <DATE_TO>{to_date}</DATE_TO>
              <RECON_STAT>-1</RECON_STAT>
            </GET_TRIPS_LOG10>
          </soap:Body>
        </soap:Envelope>"""
        return self._make_soap_request("GET_TRIPS_LOG10", payload)

    def get_trans_by_date_cab12(
        self, from_date: str, to_date: str, cab_number: str = ""
    ) -> str:
        """
        Fetches card transaction data from the Get_Trans_By_Date_Cab12 endpoint.

        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            cab_number: Optional medallion number filter
        """
        payload = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <Get_Trans_By_Date_Cab12 xmlns="https://www.taxitronic.org/VTS_SERVICE/">
              <UserId>{self.username}</UserId>
              <Password>{self.password}</Password>
              <Merchant>{self.merchant}</Merchant>
              <fromDateTime>{from_date} 00:00:00</fromDateTime>
              <ToDateTime>{to_date} 23:59:59</ToDateTime>
              <CabNumber>{cab_number}</CabNumber>
              <TranType>ALL</TranType>
            </Get_Trans_By_Date_Cab12>
          </soap:Body>
        </soap:Envelope>"""
        return self._make_soap_request("Get_Trans_By_Date_Cab12", payload)
