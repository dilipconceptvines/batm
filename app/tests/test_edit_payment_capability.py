"""
Tests for Deviation #4: Edit capability in two-step flow

Tests the ability to edit payment details after proceeding to allocation screen.
"""

import pytest
from decimal import Decimal
from unittest.mock import Mock

from app.bpm_flows.interim_payments.flows import fetch_driver_and_lease_details, create_interim_payment_record
from app.interim_payments.models import InterimPayment, PaymentMethod
from app.interim_payments.services import InterimPaymentService
from app.interim_payments.router import reset_allocation_step
from app.users.models import User
from app.core.db import get_db


class TestEditPaymentCapability:

    @pytest.fixture
    def db_session(self):
        """Database session fixture."""
        db = next(get_db())
        try:
            yield db
        finally:
            db.close()

    def create_test_driver(self, db_session):
        """Helper to create a test driver."""
        from app.drivers.models import Driver
        driver = Driver(
            first_name="Test",
            last_name="Driver",
            email="test@example.com",
            phone="555-0123"
        )
        db_session.add(driver)
        db_session.commit()
        return driver

    def create_test_lease(self, db_session, driver_id: int):
        """Helper to create a test lease."""
        from app.leases.models import Lease
        from app.medallions.models import Medallion
        from app.vehicles.models import Vehicle

        # Create medallion and vehicle
        medallion = Medallion(medallion_number="TEST-123")
        vehicle = Vehicle(vin="TESTVIN123", make="Test", model="Vehicle")
        db_session.add_all([medallion, vehicle])
        db_session.commit()

        # Create lease
        lease = Lease(
            lease_id="TEST-LEASE-001",
            medallion_id=medallion.id,
            vehicle_id=vehicle.id
        )
        db_session.add(lease)
        db_session.commit()
        return lease

    def test_fetch_prefills_existing_payment(self, db_session):
        """Step 210 fetch returns existing payment for pre-filling"""
        # Setup: Create payment
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        payment = InterimPayment(
            payment_id='INTPAY-2025-00001',
            case_no='CASE-2025-00001',
            driver_id=driver.id,
            lease_id=lease.id,
            total_amount=Decimal('600.00'),
            payment_method=PaymentMethod.CASH,
            payment_date='2025-10-21',
            notes='Test payment'
        )
        db_session.add(payment)
        db_session.commit()

        # Create case entity
        from app.bpm.services import bpm_service
        bpm_service.create_case_entity(
            db_session, payment.case_no, "interim_payment", "id", str(payment.id)
        )

        # Action: Fetch payment details (edit mode)
        result = fetch_driver_and_lease_details(
            db_session,
            case_no=payment.case_no,
            case_params={'tlc_license_no': driver.tlc_license.tlc_license_number}
        )

        # Assert: Existing payment returned
        assert result['existing_payment'] is not None
        assert result['existing_payment']['payment_amount'] == 600.00
        assert result['existing_payment']['payment_method'] == 'CASH'
        assert result['existing_payment']['notes'] == 'Test payment'


    def test_update_payment_clears_allocations(self, db_session):
        """Modifying payment amount clears existing allocations"""
        # Setup: Create payment with allocations
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        payment = InterimPayment(
            payment_id='INTPAY-2025-00002',
            case_no='CASE-2025-00002',
            driver_id=driver.id,
            lease_id=lease.id,
            total_amount=Decimal('600.00'),
            payment_method=PaymentMethod.CASH,
            payment_date='2025-10-21',
            allocations=[
                {'category': 'REPAIRS', 'reference_id': 'INV-2457', 'amount': Decimal('600.00')}
            ]
        )
        db_session.add(payment)
        db_session.commit()

        # Create case entity
        from app.bpm.services import bpm_service
        bpm_service.create_case_entity(
            db_session, payment.case_no, "interim_payment", "id", str(payment.id)
        )

        # Confirm allocations exist
        assert len(payment.allocations) == 1

        # Action: Update payment amount
        result = create_interim_payment_record(
            db_session,
            case_no=payment.case_no,
            step_data={
                'driver_id': driver.id,
                'lease_id': lease.id,
                'payment_amount': Decimal('650.00'),  # Changed from 600
                'payment_method': 'CASH',
                'payment_date': '2025-10-21'
            }
        )

        # Assert: Allocations cleared
        db_session.refresh(payment)
        assert len(payment.allocations) == 0
        assert payment.total_amount == Decimal('650.00')
        assert result['allocations_cleared'] is True


    def test_reset_allocation_allows_edit(self, db_session):
        """Reset allocation endpoint clears allocations"""
        # Setup
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        payment = InterimPayment(
            payment_id='INTPAY-2025-00003',
            case_no='CASE-2025-00003',
            driver_id=driver.id,
            lease_id=lease.id,
            total_amount=Decimal('600.00'),
            payment_method=PaymentMethod.CASH,
            payment_date='2025-10-21',
            allocations=[
                {'category': 'REPAIRS', 'reference_id': 'INV-2457', 'amount': Decimal('300.00')},
                {'category': 'EZPASS', 'reference_id': 'EZ-6789', 'amount': Decimal('300.00')}
            ]
        )
        db_session.add(payment)
        db_session.commit()

        # Create case entity
        from app.bpm.services import bpm_service
        bpm_service.create_case_entity(
            db_session, payment.case_no, "interim_payment", "id", str(payment.id)
        )

        # Action: Reset allocation
        mock_user = Mock(spec=User)
        mock_user.id = 1

        result = reset_allocation_step(
            case_no=payment.case_no,
            db=db_session,
            current_user=mock_user
        )

        # Assert: Allocations cleared
        db_session.refresh(payment)
        assert len(payment.allocations) == 0
        assert result['next_step'] == '210'


    def test_create_mode_works_normally(self, db_session):
        """Creating new payment works as before"""
        # Setup
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        case_no = 'CASE-2025-00004'

        # Action: Create new payment
        result = create_interim_payment_record(
            db_session,
            case_no=case_no,
            step_data={
                'driver_id': driver.id,
                'lease_id': lease.id,
                'payment_amount': Decimal('700.00'),
                'payment_method': 'CHECK',
                'payment_date': '2025-10-22',
                'notes': 'New payment test'
            }
        )

        # Assert: Payment created
        assert result['operation'] == 'CREATE'
        assert 'interim_payment_id' in result

        # Verify in database
        from app.bpm.services import bpm_service
        case_entity = bpm_service.get_case_entity(db_session, case_no=case_no)
        assert case_entity is not None

        service = InterimPaymentService(db_session)
        payment = service.repo.get_payment_by_id(int(case_entity.identifier_value))
        assert payment.total_amount == Decimal('700.00')
        assert payment.payment_method == PaymentMethod.CHECK