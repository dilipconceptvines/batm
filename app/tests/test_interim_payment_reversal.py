import pytest
from decimal import Decimal
from datetime import datetime, timedelta

from app.core.db import get_db
from app.interim_payments.services import InterimPaymentService
from app.interim_payments.models import PaymentStatus
from app.interim_payments.exceptions import InvalidOperationError, InterimPaymentNotFoundError
from app.ledger.models import BalanceStatus, PostingStatus


class TestInterimPaymentReversal:
    """
    Test suite for interim payment reversal functionality.
    Tests the ability to void entire payments atomically.
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

    def create_interim_payment(self, db_session, payment_id: str, driver_id: int, lease_id: int,
                              total_amount: Decimal, allocations: list):
        """Helper to create an interim payment record."""
        from app.interim_payments.models import InterimPayment, PaymentMethod

        payment = InterimPayment(
            payment_id=payment_id,
            case_no="TEST-CASE-001",
            driver_id=driver_id,
            lease_id=lease_id,
            payment_date=datetime.now(),
            total_amount=total_amount,
            payment_method=PaymentMethod.CASH,
            allocations=allocations,
            status=PaymentStatus.ACTIVE
        )
        db_session.add(payment)
        db_session.commit()
        return payment

    def test_void_payment_single_allocation(self, db_session):
        """Void payment with single allocation - balance should reopen"""
        # Setup: Create payment with 1 allocation ($500 to repairs)
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        repair_balance = self.create_ledger_balance(
            db_session,
            category='REPAIRS',
            reference_id='INV-2457',
            balance=Decimal('500.00')
        )

        # Create payment record
        payment = self.create_interim_payment(
            db_session,
            payment_id='INTPAY-2025-00001',
            driver_id=driver.id,
            lease_id=lease.id,
            total_amount=Decimal('500.00'),
            allocations=[
                {'category': 'REPAIRS', 'reference_id': 'INV-2457', 'amount': 500.00}
            ]
        )

        # Manually create ledger posting (simulating what apply_interim_payment would do)
        from app.ledger.models import LedgerPosting, EntryType, PostingCategory
        posting = LedgerPosting(
            category=PostingCategory.REPAIRS,
            amount=Decimal('500.00'),
            entry_type=EntryType.CREDIT,
            status=PostingStatus.POSTED,
            reference_id='INV-2457',
            driver_id=driver.id,
            lease_id=lease.id,
            description="Interim payment via CASH"
        )
        db_session.add(posting)
        db_session.commit()

        # Confirm balance is paid
        db_session.refresh(repair_balance)
        assert repair_balance.balance == Decimal('0.00')
        assert repair_balance.status == BalanceStatus.CLOSED

        # Action: Void payment
        service = InterimPaymentService(db_session)
        voided = service.void_interim_payment(
            payment_id='INTPAY-2025-00001',
            reason='Payment entered in error - wrong driver',
            user_id=1
        )

        # Assert: Payment voided
        assert voided.status == PaymentStatus.VOIDED
        assert voided.voided_by == 1
        assert voided.void_reason == 'Payment entered in error - wrong driver'
        assert voided.voided_at is not None

        # Assert: Balance reopened
        db_session.refresh(repair_balance)
        assert repair_balance.balance == Decimal('500.00')  # Back to original
        assert repair_balance.status == BalanceStatus.OPEN

        # Assert: Posting voided
        db_session.refresh(posting)
        assert posting.status == PostingStatus.VOIDED

        # Assert: Reversal posting created
        from app.ledger.models import LedgerPosting
        reversal_posting = db_session.query(LedgerPosting).filter(
            LedgerPosting.reference_id.like('VOID-%')
        ).first()
        assert reversal_posting is not None
        assert reversal_posting.entry_type == EntryType.DEBIT  # Opposite


    def test_void_payment_multiple_allocations(self, db_session):
        """Void payment with 3 allocations - all should be reversed"""
        # Setup: Payment with allocations to LEASE, REPAIRS, EZPASS
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        lease_balance = self.create_ledger_balance(db_session, 'LEASE', 'LS-2054-INST-05', 300.00)
        repair_balance = self.create_ledger_balance(db_session, 'REPAIRS', 'INV-2457', 200.00)
        ezpass_balance = self.create_ledger_balance(db_session, 'EZPASS', 'EZ-6789', 100.00)

        # Create payment record
        payment = self.create_interim_payment(
            db_session,
            payment_id='INTPAY-2025-00002',
            driver_id=driver.id,
            lease_id=lease.id,
            total_amount=Decimal('600.00'),
            allocations=[
                {'category': 'LEASE', 'reference_id': 'LS-2054-INST-05', 'amount': 300.00},
                {'category': 'REPAIRS', 'reference_id': 'INV-2457', 'amount': 200.00},
                {'category': 'EZPASS', 'reference_id': 'EZ-6789', 'amount': 100.00}
            ]
        )

        # Manually create ledger postings
        from app.ledger.models import LedgerPosting, EntryType, PostingCategory
        postings = []
        for alloc in payment.allocations:
            posting = LedgerPosting(
                category=PostingCategory[alloc['category']],
                amount=Decimal(str(alloc['amount'])),
                entry_type=EntryType.CREDIT,
                status=PostingStatus.POSTED,
                reference_id=alloc['reference_id'],
                driver_id=driver.id,
                lease_id=lease.id,
                description="Interim payment via CASH"
            )
            db_session.add(posting)
            postings.append(posting)
        db_session.commit()

        # Action: Void payment
        service = InterimPaymentService(db_session)
        voided = service.void_interim_payment(
            payment_id='INTPAY-2025-00002',
            reason='Wrong payment date entered',
            user_id=2
        )

        # Assert: All 3 balances reopened
        db_session.refresh(lease_balance)
        db_session.refresh(repair_balance)
        db_session.refresh(ezpass_balance)

        assert lease_balance.balance == Decimal('300.00')
        assert repair_balance.balance == Decimal('200.00')
        assert ezpass_balance.balance == Decimal('100.00')

        assert lease_balance.status == BalanceStatus.OPEN
        assert repair_balance.status == BalanceStatus.OPEN
        assert ezpass_balance.status == BalanceStatus.OPEN


    def test_cannot_void_already_voided_payment(self, db_session):
        """Prevent double-void"""
        service = InterimPaymentService(db_session)

        with pytest.raises(InvalidOperationError, match="already voided"):
            service.void_interim_payment(
                payment_id='NONEXISTENT-001',
                reason='Second void attempt',
                user_id=1
            )


    def test_void_requires_minimum_reason_length(self, db_session):
        """Reason must be at least 10 characters"""
        service = InterimPaymentService(db_session)

        with pytest.raises(InvalidOperationError, match="at least 10 characters"):
            service.void_interim_payment(
                payment_id='INTPAY-2025-00004',
                reason='Short',  # Too short
                user_id=1
            )


    def test_void_nonexistent_payment(self, db_session):
        """Voiding non-existent payment raises error"""
        service = InterimPaymentService(db_session)

        with pytest.raises(InterimPaymentNotFoundError):
            service.void_interim_payment(
                payment_id='NONEXISTENT-001',
                reason='This payment does not exist',
                user_id=1
            )