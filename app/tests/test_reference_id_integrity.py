import pytest
from decimal import Decimal

from app.core.db import get_db
from app.ledger.services import LedgerService
from app.ledger.models import BalanceStatus
from app.ledger.exceptions import InvalidLedgerOperationError


class TestReferenceIDIntegrity:
    """
    Test suite for reference ID integrity in ledger postings.
    Tests that postings use original reference_id and payment tracking fields.
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

    def test_posting_uses_original_reference_id(self, db_session):
        """
        Verify that ledger posting reference_id matches the original obligation reference_id
        """
        # Setup
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        # Create repair balance with specific reference_id
        repair_balance = self.create_ledger_balance(
            db_session,
            category='REPAIRS',
            reference_id='INV-2457',  # Original reference
            balance=Decimal('500.00')
        )

        # Action: Apply interim payment
        ledger_service = LedgerService(db_session)
        postings = ledger_service.apply_interim_payment(
            payment_amount=Decimal('500.00'),
            allocations={'INV-2457': Decimal('500.00')},
            driver_id=driver.id,
            lease_id=lease.id,
            payment_method='CASH'
        )

        # Assert: Posting uses ORIGINAL reference_id (not modified)
        assert len(postings) == 1
        posting = postings[0]
        assert posting.reference_id == 'INV-2457'  # ✅ EXACT match
        assert posting.reference_id != 'PAYMENT-CASH-INV-2457'  # ❌ NOT modified

        # Payment method tracked separately
        assert posting.payment_method == 'CASH'
        assert posting.payment_source == 'INTERIM_PAYMENT'
        assert 'CASH' in posting.description


    def test_query_payments_by_reference_id(self, db_session):
        """
        Verify we can easily query all payments for a specific obligation
        """
        # Setup
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        repair_balance = self.create_ledger_balance(db_session, 'REPAIRS', 'INV-2457', 1000.00)

        ledger_service = LedgerService(db_session)

        # Make 3 partial payments on different days
        ledger_service.apply_interim_payment(
            payment_amount=Decimal('300.00'),
            allocations={'INV-2457': Decimal('300.00')},
            driver_id=driver.id,
            lease_id=lease.id,
            payment_method='CASH'
        )

        ledger_service.apply_interim_payment(
            payment_amount=Decimal('400.00'),
            allocations={'INV-2457': Decimal('400.00')},
            driver_id=driver.id,
            lease_id=lease.id,
            payment_method='CHECK'
        )

        ledger_service.apply_interim_payment(
            payment_amount=Decimal('300.00'),
            allocations={'INV-2457': Decimal('300.00')},
            driver_id=driver.id,
            lease_id=lease.id,
            payment_method='ACH'
        )

        # Query: Get ALL payments for this invoice
        from app.ledger.models import LedgerPosting, EntryType

        all_payments = (
            db_session.query(LedgerPosting)
            .filter(
                LedgerPosting.reference_id == 'INV-2457',
                LedgerPosting.entry_type == EntryType.CREDIT,
                LedgerPosting.payment_source == 'INTERIM_PAYMENT'
            )
            .all()
        )

        # Assert: Found all 3 payments
        assert len(all_payments) == 3
        assert sum(p.amount for p in all_payments) == Decimal('1000.00')

        # Verify payment methods tracked
        methods = {p.payment_method for p in all_payments}
        assert methods == {'CASH', 'CHECK', 'ACH'}


    def test_multiple_allocations_same_payment(self, db_session):
        """
        One payment allocated to multiple obligations - each uses correct reference_id
        """
        # Setup
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        repair_balance = self.create_ledger_balance(db_session, 'REPAIRS', 'INV-2457', 300.00)
        loan_balance = self.create_ledger_balance(db_session, 'LOANS', 'LN-3001', 200.00)
        ezpass_balance = self.create_ledger_balance(db_session, 'EZPASS', 'EZ-6789', 100.00)

        # Action: Single payment, 3 allocations
        ledger_service = LedgerService(db_session)
        postings = ledger_service.apply_interim_payment(
            payment_amount=Decimal('600.00'),
            allocations={
                'INV-2457': Decimal('300.00'),
                'LN-3001': Decimal('200.00'),
                'EZ-6789': Decimal('100.00')
            },
            driver_id=driver.id,
            lease_id=lease.id,
            payment_method='ACH'
        )

        # Assert: 3 postings, each with correct original reference_id
        assert len(postings) == 3

        reference_ids = {p.reference_id for p in postings}
        assert reference_ids == {'INV-2457', 'LN-3001', 'EZ-6789'}

        # All have same payment tracking
        assert all(p.payment_method == 'ACH' for p in postings)
        assert all(p.payment_source == 'INTERIM_PAYMENT' for p in postings)


    def test_excess_postings_use_installment_reference_id(self, db_session):
        """
        Verify that excess payments to lease installments use the installment ID as reference_id
        """
        # Setup
        driver = self.create_test_driver(db_session)
        lease = self.create_test_lease(db_session, driver_id=driver.id)

        # Create lease schedule installment
        from app.leases.models import LeaseSchedule
        installment = LeaseSchedule(
            lease_id=lease.id,
            installment_number=5,
            installment_due_date="2025-12-31",
            installment_amount=300.00,
            installment_status='Scheduled'
        )
        db_session.add(installment)
        db_session.commit()

        # Action: Apply payment with excess that will go to lease installment
        ledger_service = LedgerService(db_session)
        postings = ledger_service.apply_interim_payment(
            payment_amount=Decimal('600.00'),
            allocations={},  # No explicit allocations - all goes to excess
            driver_id=driver.id,
            lease_id=lease.id,
            payment_method='CASH'
        )

        # Assert: Posting uses installment ID as reference_id
        assert len(postings) == 1
        posting = postings[0]
        assert posting.reference_id == str(installment.id)  # ✅ Installment ID
        assert posting.category.value == 'LEASE'
        assert posting.payment_method == 'CASH'
        assert posting.payment_source == 'INTERIM_PAYMENT'
        assert 'Installment #5' in posting.description