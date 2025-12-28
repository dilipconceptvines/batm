# app/tests/test_deposits.py

import pytest
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch, MagicMock

from sqlalchemy.orm import Session

from app.core.db import get_db
from app.deposits.models import Deposit, DepositStatus, CollectionMethod
from app.deposits.services import DepositService
from app.deposits.exceptions import DepositValidationError, DepositNotFoundError, InvalidDepositOperationError
from app.deposits.repository import DepositRepository
from app.leases.models import Lease
from app.leases.services import LeaseService
from app.drivers.models import Driver
from app.vehicles.models import Vehicle
from app.medallions.models import Medallion
from app.ledger.models import LedgerBalance, PostingCategory, BalanceStatus
from app.ledger.services import LedgerService
from app.utils.logger import get_logger

logger = get_logger(__name__)


@pytest.fixture
def db_session():
    """Database session fixture for testing"""
    # Use the test database session from the existing test setup
    from test.test_db import db_session as test_db_session
    return test_db_session


@pytest.fixture
def deposit_service(db_session):
    """Deposit service fixture"""
    return DepositService(db_session)


@pytest.fixture
def deposit_repo(db_session):
    """Deposit repository fixture"""
    return DepositRepository(db_session)


@pytest.fixture
def sample_driver(db_session):
    """Create a sample driver for testing"""
    driver = Driver(
        id=99999,
        full_name="Test Driver",
        email="test@driver.com",
        phone="555-0123",
        ssn="123456789",
        driver_status="Active",
        pay_to_mode="ACH"
    )
    db_session.add(driver)
    db_session.commit()
    return driver


@pytest.fixture
def sample_vehicle(db_session):
    """Create a sample vehicle for testing"""
    vehicle = Vehicle(
        id=99999,
        vin="1HGCM82633A123456",
        make="Toyota",
        model="Camry",
        year=2020
    )
    db_session.add(vehicle)
    db_session.commit()
    return vehicle


@pytest.fixture
def sample_medallion(db_session):
    """Create a sample medallion for testing"""
    medallion = Medallion(
        id=99999,
        medallion_number="T12345",
        medallion_status="Active"
    )
    db_session.add(medallion)
    db_session.commit()
    return medallion


@pytest.fixture
def sample_lease(db_session, sample_driver, sample_vehicle, sample_medallion):
    """Create a sample lease for testing"""
    lease = Lease(
        id=99999,
        lease_id="TEST-LEASE-001",
        driver_id=sample_driver.id,
        vehicle_id=sample_vehicle.id,
        medallion_id=sample_medallion.id,
        lease_status="Active",
        lease_start_date=date.today(),
        lease_end_date=date.today() + timedelta(days=365),
        weekly_lease_fee=Decimal("500.00"),
        deposit_amount_paid=Decimal("1000.00")  # Legacy field
    )
    db_session.add(lease)
    db_session.commit()
    return lease


class TestDepositCreation:
    """Test deposit creation scenarios"""

    def test_create_deposit_full_payment(self, deposit_service, sample_lease, db_session):
        """Test creating a deposit with full payment"""
        deposit_data = {
            'lease_id': sample_lease.id,
            'required_amount': Decimal('1000.00'),
            'collected_amount': Decimal('1000.00'),
            'driver_tlc_license': 'T123456C'
        }

        deposit = deposit_service.create_deposit(db_session, deposit_data)

        assert deposit.deposit_id == "DEP-99999-01"
        assert deposit.required_amount == Decimal('1000.00')
        assert deposit.collected_amount == Decimal('1000.00')
        assert deposit.outstanding_amount == Decimal('0.00')
        assert deposit.deposit_status == DepositStatus.PAID

        # Verify ledger posting was created
        ledger_balance = db_session.query(LedgerBalance).filter(
            LedgerBalance.reference_id == deposit.deposit_id,
            LedgerBalance.category == PostingCategory.DEPOSIT
        ).first()

        assert ledger_balance is not None
        assert ledger_balance.balance == Decimal('1000.00')
        assert ledger_balance.status == BalanceStatus.OPEN

    def test_create_deposit_partial_payment(self, deposit_service, sample_lease, db_session):
        """Test creating a deposit with partial payment"""
        deposit_data = {
            'lease_id': sample_lease.id,
            'required_amount': Decimal('1000.00'),
            'collected_amount': Decimal('500.00'),
            'driver_tlc_license': 'T123456C'
        }

        deposit = deposit_service.create_deposit(db_session, deposit_data)

        assert deposit.required_amount == Decimal('1000.00')
        assert deposit.collected_amount == Decimal('500.00')
        assert deposit.outstanding_amount == Decimal('500.00')
        assert deposit.deposit_status == DepositStatus.PARTIALLY_PAID

    def test_create_deposit_zero_payment(self, deposit_service, sample_lease, db_session):
        """Test creating a deposit with zero payment"""
        deposit_data = {
            'lease_id': sample_lease.id,
            'required_amount': Decimal('1000.00'),
            'collected_amount': Decimal('0.00'),
            'driver_tlc_license': 'T123456C'
        }

        deposit = deposit_service.create_deposit(db_session, deposit_data)

        assert deposit.required_amount == Decimal('1000.00')
        assert deposit.collected_amount == Decimal('0.00')
        assert deposit.outstanding_amount == Decimal('1000.00')
        assert deposit.deposit_status == DepositStatus.PENDING

        # Verify NO ledger posting was created
        ledger_balance = db_session.query(LedgerBalance).filter(
            LedgerBalance.reference_id == deposit.deposit_id,
            LedgerBalance.category == PostingCategory.DEPOSIT
        ).first()

        assert ledger_balance is None


class TestDepositCollection:
    """Test deposit collection scenarios"""

    def test_deposit_installment_payment(self, deposit_service, sample_lease, db_session):
        """Test multiple payments on a deposit"""
        # Create initial deposit
        deposit_data = {
            'lease_id': sample_lease.id,
            'required_amount': Decimal('1000.00'),
            'collected_amount': Decimal('300.00'),
            'driver_tlc_license': 'T123456C'
        }
        deposit = deposit_service.create_deposit(db_session, deposit_data)
        assert deposit.deposit_status == DepositStatus.PARTIALLY_PAID

        # Add second payment
        updated_deposit = deposit_service.update_deposit_collection(
            db=db_session,
            deposit_id=deposit.deposit_id,
            additional_amount=Decimal('700.00'),
            collection_method=CollectionMethod.CASH,
            notes="Second payment"
        )

        assert updated_deposit.collected_amount == Decimal('1000.00')
        assert updated_deposit.outstanding_amount == Decimal('0.00')
        assert updated_deposit.deposit_status == DepositStatus.PAID

        # Verify both ledger postings exist
        ledger_postings = db_session.query(LedgerBalance).filter(
            LedgerBalance.reference_id == deposit.deposit_id,
            LedgerBalance.category == PostingCategory.DEPOSIT
        ).all()

        assert len(ledger_postings) == 2
        total_balance = sum(posting.balance for posting in ledger_postings)
        assert total_balance == Decimal('1000.00')


class TestDepositHoldPeriod:
    """Test deposit hold period scenarios"""

    def test_initiate_hold_period(self, deposit_service, sample_lease, db_session):
        """Test initiating hold period after lease termination"""
        # Create and pay deposit
        deposit_data = {
            'lease_id': sample_lease.id,
            'required_amount': Decimal('1000.00'),
            'collected_amount': Decimal('1000.00'),
            'driver_tlc_license': 'T123456C'
        }
        deposit = deposit_service.create_deposit(db_session, deposit_data)
        assert deposit.deposit_status == DepositStatus.PAID

        # Terminate lease
        termination_date = date.today()

        # Initiate hold period
        held_deposit = deposit_service.initiate_hold_period(
            db=db_session,
            lease_id=sample_lease.id,
            termination_date=termination_date
        )

        assert held_deposit.deposit_status == DepositStatus.HELD
        assert held_deposit.lease_termination_date == termination_date
        assert held_deposit.hold_expiry_date == termination_date + timedelta(days=30)


class TestDepositAutoApply:
    """Test automatic deposit application scenarios"""

    def test_auto_apply_deposit_with_obligations(self, deposit_service, sample_lease, db_session):
        """Test auto-applying deposit to outstanding obligations"""
        # Create deposit
        deposit_data = {
            'lease_id': sample_lease.id,
            'required_amount': Decimal('1000.00'),
            'collected_amount': Decimal('1000.00'),
            'driver_tlc_license': 'T123456C'
        }
        deposit = deposit_service.create_deposit(db_session, deposit_data)

        # Create EZPass obligation
        ezpass_balance = LedgerBalance(
            category=PostingCategory.EZPASS,
            balance=Decimal('200.00'),
            status=BalanceStatus.OPEN,
            reference_id="EZPASS-001",
            driver_id=sample_lease.driver_id,
            lease_id=sample_lease.id
        )
        db_session.add(ezpass_balance)

        # Create PVB obligation
        pvb_balance = LedgerBalance(
            category=PostingCategory.PVB,
            balance=Decimal('150.00'),
            status=BalanceStatus.OPEN,
            reference_id="PVB-001",
            driver_id=sample_lease.driver_id,
            lease_id=sample_lease.id
        )
        db_session.add(pvb_balance)
        db_session.commit()

        # Auto-apply deposit
        result = deposit_service.auto_apply_deposit(
            db=db_session,
            lease_id=sample_lease.id
        )

        # Verify obligations were reduced
        updated_ezpass = db_session.query(LedgerBalance).filter(
            LedgerBalance.id == ezpass_balance.id
        ).first()
        updated_pvb = db_session.query(LedgerBalance).filter(
            LedgerBalance.id == pvb_balance.id
        ).first()

        assert updated_ezpass.balance == Decimal('0.00')  # Fully paid
        assert updated_ezpass.status == BalanceStatus.CLOSED
        assert updated_pvb.balance == Decimal('50.00')    # Partially paid (150 - 100 = 50)


class TestDepositRefund:
    """Test deposit refund scenarios"""

    def test_manual_refund(self, deposit_service, sample_lease, db_session):
        """Test processing a manual refund"""
        # Create and pay deposit
        deposit_data = {
            'lease_id': sample_lease.id,
            'required_amount': Decimal('1000.00'),
            'collected_amount': Decimal('1000.00'),
            'driver_tlc_license': 'T123456C'
        }
        deposit = deposit_service.create_deposit(db_session, deposit_data)

        # Put deposit on hold
        termination_date = date.today()
        held_deposit = deposit_service.initiate_hold_period(
            db=db_session,
            lease_id=sample_lease.id,
            termination_date=termination_date
        )

        # Process refund
        refund_result = deposit_service.process_refund(
            db=db_session,
            deposit_id=held_deposit.deposit_id,
            refund_amount=Decimal('800.00'),
            refund_method="CHECK",
            notes="Partial refund after hold period"
        )

        assert refund_result.deposit_status == DepositStatus.REFUNDED
        assert refund_result.refund_amount == Decimal('800.00')
        assert refund_result.refund_method == "CHECK"

        # Verify refund ledger posting was created
        refund_posting = db_session.query(LedgerBalance).filter(
            LedgerBalance.reference_id == f"REFUND-{held_deposit.deposit_id}",
            LedgerBalance.category == PostingCategory.DEPOSIT
        ).first()

        assert refund_posting is not None
        assert refund_posting.balance == Decimal('-800.00')  # Credit for refund


class TestDepositValidation:
    """Test deposit validation scenarios"""

    def test_deposit_validation_errors(self, deposit_service, sample_lease, db_session):
        """Test various validation error scenarios"""

        # Test collected > required
        with pytest.raises(DepositValidationError):
            deposit_data = {
                'lease_id': sample_lease.id,
                'required_amount': Decimal('1000.00'),
                'collected_amount': Decimal('1200.00'),  # Over collected
                'driver_tlc_license': 'T123456C'
            }
            deposit_service.create_deposit(db_session, deposit_data)

        # Test negative collected amount
        with pytest.raises(DepositValidationError):
            deposit_data = {
                'lease_id': sample_lease.id,
                'required_amount': Decimal('1000.00'),
                'collected_amount': Decimal('-100.00'),  # Negative
                'driver_tlc_license': 'T123456C'
            }
            deposit_service.create_deposit(db_session, deposit_data)

        # Test duplicate deposit for lease
        deposit_data = {
            'lease_id': sample_lease.id,
            'required_amount': Decimal('1000.00'),
            'collected_amount': Decimal('500.00'),
            'driver_tlc_license': 'T123456C'
        }
        deposit_service.create_deposit(db_session, deposit_data)

        # Try to create another deposit for same lease
        with pytest.raises(DepositValidationError):
            deposit_service.create_deposit(db_session, deposit_data)


class TestDepositBPMIntegration:
    """Test BPM flow integration"""

    @patch('app.bpm_flows.driverlease.flows.bpm_service')
    @patch('app.bpm_flows.driverlease.flows.lease_service')
    def test_bpm_flow_integration(self, mock_lease_service, mock_bpm_service, deposit_service, db_session):
        """Test BPM flow integration with deposit creation"""
        # Mock the lease service
        mock_lease = MagicMock()
        mock_lease.id = 12345
        mock_lease_service.get_lease.return_value = mock_lease

        # Mock BPM service
        mock_bpm_service.create_case_entity.return_value = None

        # Test deposit creation through BPM flow (this would be called from the actual BPM step)
        deposit_data = {
            'lease_id': 12345,
            'required_amount': Decimal('1500.00'),
            'collected_amount': Decimal('1500.00'),
            'driver_tlc_license': 'T123456C'
        }

        deposit = deposit_service.create_deposit(db_session, deposit_data)

        assert deposit.lease_id == 12345
        assert deposit.required_amount == Decimal('1500.00')
        assert deposit.deposit_status == DepositStatus.PAID


class TestDepositBackwardCompatibility:
    """Test backward compatibility scenarios"""

    def test_backward_compatibility(self, deposit_service, sample_lease, db_session):
        """Test that old lease.deposit_amount_paid field doesn't break anything"""
        # The old field should still exist but not be used for deposit calculations
        assert hasattr(sample_lease, 'deposit_amount_paid')

        # Create deposit using new system
        deposit_data = {
            'lease_id': sample_lease.id,
            'required_amount': Decimal('1000.00'),
            'collected_amount': Decimal('600.00'),
            'driver_tlc_license': 'T123456C'
        }
        deposit = deposit_service.create_deposit(db_session, deposit_data)

        # Verify deposit uses new collected_amount, not old lease field
        assert deposit.collected_amount == Decimal('600.00')
        assert deposit.collected_amount != sample_lease.deposit_amount_paid  # Should be different

        # Verify outstanding calculation is correct
        assert deposit.outstanding_amount == Decimal('400.00')  # 1000 - 600