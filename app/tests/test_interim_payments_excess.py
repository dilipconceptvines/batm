import pytest
from decimal import Decimal
from datetime import datetime, timedelta

from app.core.db import get_db
from app.ledger.services import LedgerService
from app.ledger.models import BalanceStatus
from app.ledger.exceptions import InvalidLedgerOperationError


class TestExcessToLeaseSchedule:
    """
    Test suite for excess amount handling in interim payments.
    Tests the new functionality that applies excess to lease schedule installments.
    """

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

        lease = Lease(
            lease_number="TEST-LEASE-001",
            medallion_id=medallion.id,
            vehicle_id=vehicle.id,
            weekly_payment=Decimal('300.00')
        )
        db_session.add(lease)
        db_session.commit()
        return lease

    def create_lease_installment(self, db_session, lease_id: int, installment_number: int,
                                installment_amount: float, status: str, due_date=None):
        """Helper to create a lease schedule installment."""
        from app.leases.models import LeaseSchedule

        if due_date is None:
            due_date = datetime.now().date() + timedelta(days=installment_number * 7)

        installment = LeaseSchedule(
            lease_id=lease_id,
            installment_number=installment_number,
            installment_due_date=due_date,
            installment_amount=installment_amount,
            installment_status=status,
            posted_to_ledger=0 if status == 'Scheduled' else 1
        )
        db_session.add(installment)
        db_session.commit()
        return installment

    def create_ledger_balance(self, db_session, category: str, reference_id: str, balance: Decimal):
        """Helper to create a ledger balance."""
        from app.ledger.models import LedgerBalance, PostingCategory

        balance_obj = LedgerBalance(
            category=PostingCategory[category],
            reference_id=reference_id,
            balance=balance,
            original_amount=balance,
            status=BalanceStatus.OPEN
        )
        db_session.add(balance_obj)
        db_session.commit()
        return balance_obj

    def test_excess_applied_to_single_lease_installment(self, db_session):
        """
        SCENARIO: Driver pays $600, allocates $500 to repairs, $100 excess to lease
        EXPECTED: Excess applied to next upcoming lease installment
        """
        # Setup
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        # Create lease schedule with $300/week installments
        installment = self.create_lease_installment(
            db_session,
            lease_id=lease.id,
            installment_number=5,
            installment_amount=300.00,
            status='Scheduled'
        )

        # Create repair balance
        repair_balance = self.create_ledger_balance(
            db_session,
            category='REPAIRS',
            reference_id='INV-2457',
            balance=Decimal('500.00')
        )

        # Action: Apply interim payment
        ledger_service = LedgerService(db_session)
        postings = ledger_service.apply_interim_payment(
            payment_amount=Decimal('600.00'),
            allocations={'INV-2457': Decimal('500.00')},
            driver_id=driver.id,
            lease_id=lease.id,
            payment_method='CASH'
        )

        # Assert
        assert len(postings) == 2  # Repair + Lease installment

        # Check repair posting
        repair_posting = next(p for p in postings if p.category.value == 'REPAIRS')
        assert repair_posting.amount == Decimal('500.00')
        assert repair_posting.reference_id == 'INV-2457'

        # Check lease installment posting
        lease_posting = next(p for p in postings if p.category.value == 'LEASE')
        assert lease_posting.amount == Decimal('100.00')
        assert lease_posting.reference_id == str(installment.id)

        # Check installment balance updated
        db_session.refresh(installment)
        balance = ledger_service.repo.get_balance_by_reference_id(str(installment.id))
        assert balance.balance == Decimal('200.00')  # $300 - $100
        assert balance.status == BalanceStatus.OPEN
        assert installment.installment_status == 'Posted'  # Not fully paid yet


    def test_excess_applied_to_multiple_lease_installments(self, db_session):
        """
        SCENARIO: Driver pays $1000, allocates $200, $800 excess covers 2.5 installments
        EXPECTED: First installment fully paid, second fully paid, third partially paid
        """
        # Setup
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        # Create 3 lease installments
        inst1 = self.create_lease_installment(db_session, lease.id, 1, 300.00, 'Scheduled')
        inst2 = self.create_lease_installment(db_session, lease.id, 2, 300.00, 'Scheduled')
        inst3 = self.create_lease_installment(db_session, lease.id, 3, 300.00, 'Scheduled')

        # Action
        ledger_service = LedgerService(db_session)
        postings = ledger_service.apply_interim_payment(
            payment_amount=Decimal('1000.00'),
            allocations={'MISC-001': Decimal('200.00')},
            driver_id=driver.id,
            lease_id=lease.id,
            payment_method='ACH'
        )

        # Assert: 4 postings (1 misc + 3 lease installments)
        assert len(postings) == 4

        # Check installment 1: Fully paid
        db_session.refresh(inst1)
        balance1 = ledger_service.repo.get_balance_by_reference_id(str(inst1.id))
        assert balance1.balance == Decimal('0.00')
        assert balance1.status == BalanceStatus.CLOSED
        assert inst1.installment_status == 'Paid'

        # Check installment 2: Fully paid
        db_session.refresh(inst2)
        balance2 = ledger_service.repo.get_balance_by_reference_id(str(inst2.id))
        assert balance2.balance == Decimal('0.00')
        assert balance2.status == BalanceStatus.CLOSED
        assert inst2.installment_status == 'Paid'

        # Check installment 3: Partially paid ($200 of $300)
        db_session.refresh(inst3)
        balance3 = ledger_service.repo.get_balance_by_reference_id(str(inst3.id))
        assert balance3.balance == Decimal('100.00')
        assert balance3.status == BalanceStatus.OPEN
        assert inst3.installment_status == 'Posted'  # Not fully paid


    def test_excess_fails_when_no_lease_installments(self, db_session):
        """
        SCENARIO: Driver pays $600, allocates $500, but lease has ended (no installments)
        EXPECTED: Transaction fails with clear error message
        """
        # Setup
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)
        # No installments created (lease ended)

        # Action & Assert
        ledger_service = LedgerService(db_session)
        with pytest.raises(InvalidLedgerOperationError, match="no scheduled lease installments found"):
            ledger_service.apply_interim_payment(
                payment_amount=Decimal('600.00'),
                allocations={'MISC-001': Decimal('500.00')},
                driver_id=driver.id,
                lease_id=lease.id,
                payment_method='CASH'
            )


    def test_excess_creates_balance_for_future_installment(self, db_session):
        """
        SCENARIO: Prepayment - installment not yet posted to ledger
        EXPECTED: System creates ledger balance entry for future installment
        """
        # Setup
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        # Create FUTURE installment (status = Scheduled, not Posted)
        future_inst = self.create_lease_installment(
            db_session,
            lease_id=lease.id,
            installment_number=10,
            installment_amount=300.00,
            status='Scheduled',
            due_date=datetime.now().date() + timedelta(days=70)
        )

        # Confirm no ledger balance exists yet
        ledger_service = LedgerService(db_session)
        assert ledger_service.repo.get_balance_by_reference_id(str(future_inst.id)) is None

        # Action: Apply payment with excess
        postings = ledger_service.apply_interim_payment(
            payment_amount=Decimal('300.00'),
            allocations={},
            driver_id=driver.id,
            lease_id=lease.id,
            payment_method='CASH'
        )

        # Assert: Ledger balance created for future installment
        balance = ledger_service.repo.get_balance_by_reference_id(str(future_inst.id))
        assert balance is not None
        assert balance.original_amount == Decimal('300.00')
        assert balance.balance == Decimal('0.00')  # Fully paid by excess
        assert balance.status == BalanceStatus.CLOSED

        # Installment marked as paid
        db_session.refresh(future_inst)
        assert future_inst.installment_status == 'Paid'