## app/drivers/services.py

# Standard library imports
from typing import Optional, Union, List

# Third party imports
from sqlalchemy.orm import Session
from sqlalchemy import func

# Local imports
from app.drivers.models import Driver, TLCLicense, DMVLicense
from app.leases.models import Lease , LeaseDriver
from app.vehicles.models import Vehicle
from app.medallions.models import Medallion
from app.drivers.schemas import DriverStatus
from app.utils.logger import get_logger

logger = get_logger(__name__)


class DriverService:
    """Service for driver operations"""
    def get_drivers_by_status(self, db: Session) -> dict:
        """Get drivers by status"""
        try:
            status_count = db.query(Driver.driver_status, func.count(Driver.id)).group_by(
                Driver.driver_status
            ).filter(
                (Driver.driver_status.in_([
                    DriverStatus.ACTIVE,
                    DriverStatus.IN_PROGRESS,
                    DriverStatus.REGISTERED
                ])) & (Driver.is_active == True) & (Driver.drive_locked == False)
            ).all()

            return {status: count for status, count in status_count}
        except Exception as e:
            logger.error("Error getting drivers by status: %s", str(e))
            raise e
        
    def get_drivers(
        self, db: Session,
        id: Optional[int] = None,
        driver_id: Optional[str] = None,
        ssn: Optional[str] = None,
        driver_name: Optional[str] = None,
        medallion_number: Optional[str] = None,
        vin: Optional[str] = None,
        tlc_license_number: Optional[str] = None,
        dmv_license_number: Optional[str] = None,
        is_active: Optional[bool] = True,
        is_additional_driver: Optional[bool] = False,
        driver_status: Optional[DriverStatus] = None,
        driver_type: Optional[str] = None,
        email_address: Optional[str] = None,
        phone_number: Optional[str] = None,
        multiple: Optional[bool] = False,
    ) -> Union[List[Driver], List[Driver], None]:
        """
        Get drivers based on the provided filters
        """
        try:
            query = db.query(Driver)

            if driver_id is not None:
                logger.info(f"Filtering drivers with IDs: {driver_id}")
            if id:
                query = query.filter(Driver.id == id)
            if driver_id:
                driver_id = str(driver_id).split(',')
                query = query.filter(Driver.driver_id.in_(driver_id))
            if driver_name:
                query = query.filter(Driver.full_name.ilike(f"%{driver_name}%"))
            if ssn:
                ssn = ssn.strip()
                if len(ssn) == 4:
                    query = query.filter(func.substr(Driver.ssn, -4) == ssn)
                else:
                    query = query.filter(Driver.ssn == ssn)

            if medallion_number:
                query = query.join(
                    LeaseDriver,
                    Driver.driver_id == LeaseDriver.driver_id
                ).join(
                    Lease,
                    LeaseDriver.lease_id == Lease.id
                ).join(
                    Medallion,
                    Lease.medallion_id == Medallion.id
                ).filter(Medallion.medallion_number == medallion_number)
            
            if vin:
                 query = query.join(
                    LeaseDriver,
                    Driver.driver_id == LeaseDriver.driver_id
                ).join(
                    Lease,
                    LeaseDriver.lease_id == Lease.id
                ).join(
                    Vehicle,
                    Lease.vehicle_id == Vehicle.id
                ).filter(Vehicle.vin == vin)
            

            if tlc_license_number:
                query = query.join(
                    TLCLicense,
                    Driver.tlc_license_number_id == TLCLicense.id
                ).filter(TLCLicense.tlc_license_number == tlc_license_number)
            if dmv_license_number:
                query = query.join(
                    DMVLicense,
                    Driver.dmv_license_number_id == DMVLicense.id
                ).filter(DMVLicense.dmv_license_number == dmv_license_number)
            if is_active:
                query = query.filter(Driver.is_active == is_active)
            if is_additional_driver:
                query = query.filter(Driver.is_additional_driver == is_additional_driver)
            if driver_status:
                query = query.filter(Driver.driver_status == driver_status)
            if driver_type:
                query = query.filter(Driver.driver_type == driver_type)
            if email_address:
                query = query.filter(Driver.email_address == email_address)
            if phone_number:
                query = query.filter(Driver.phone_number_1 == phone_number)
                
            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting drivers: %s", e)
            raise e
        
    def upsert_driver(self, db: Session, driver_data: dict) -> Driver:
        """
        Upsert a driver
        """
        try:
            if driver_data.get('id'):
                driver = self.get_drivers(db, id=driver_data['id'])
                for key, value in driver_data.items():
                    setattr(driver, key, value)
                db.commit()
                db.refresh(driver)
                return driver
            else:
                driver = Driver(**driver_data)
                db.add(driver)
                db.commit()
                db.refresh(driver)
                return driver
        except Exception as e:
            logger.error("Error upserting driver: %s", e)
            raise e
        
    def get_dmv_license(
        self, db: Session,
        dmv_license_number: Optional[str] = None,
        driver_id: Optional[int] = None,
        license_id: Optional[int] = None, multiple: Optional[bool] = False
    ) -> Union[DMVLicense, List[DMVLicense], None]:
        """
        Get a dmv license by the provided filters
        """
        try:
            query = db.query(DMVLicense)

            if dmv_license_number:
                query = query.filter(DMVLicense.dmv_license_number == dmv_license_number)
            if driver_id:
                query = query.filter(DMVLicense.driver.driver_id == driver_id)
            if license_id:
                query = query.filter(DMVLicense.id == license_id)

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting dmv license: %s", e)
            raise e
        
    def get_tlc_license(
        self, db: Session, tlc_license_number: Optional[str] = None,
        license_id: Optional[int] = None, multiple: Optional[bool] = False
    ) -> Union[TLCLicense, List[TLCLicense], None]:
        """
        Get a tlc license by the provided filters
        """
        try:
            query = db.query(TLCLicense)

            if tlc_license_number:
                query = query.filter(TLCLicense.tlc_license_number == tlc_license_number)
            if license_id:
                query = query.filter(TLCLicense.id == license_id)

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting tlc license: %s", e)
            raise e
        
    def upsert_dmv_license(
        self, db: Session, dmv_data: dict
    ) -> DMVLicense:
        """
        Upsert a dmv license
        """
        try:
            if dmv_data.get('id'):
                dmv_license = self.get_dmv_license(db, license_id=dmv_data['id'])
                for key, value in dmv_data.items():
                    setattr(dmv_license, key, value)
                db.commit()
                db.refresh(dmv_license)
                return dmv_license
            else:
                dmv_license = DMVLicense(**dmv_data)
                db.add(dmv_license)
                db.commit()
                db.refresh(dmv_license)
                return dmv_license
        except Exception as e:
            logger.error("Error upserting dmv license: %s", e)
            raise e
        
    def upsert_tlc_license(
        self, db: Session, tlc_data: dict
    ) -> TLCLicense:
        """
        Upsert a tlc license
        """
        try:
            if tlc_data.get('id'):
                tlc_license = self.get_tlc_license(db, license_id=tlc_data['id'])
                for key, value in tlc_data.items():
                    setattr(tlc_license, key, value)
                db.commit()
                db.refresh(tlc_license)
                return tlc_license
            else:
                tlc_license = TLCLicense(**tlc_data)
                db.add(tlc_license)
                db.commit()
                db.refresh(tlc_license)
                return tlc_license
        except Exception as e:
            logger.error("Error upserting tlc license: %s", e)
            raise e

driver_service = DriverService()
