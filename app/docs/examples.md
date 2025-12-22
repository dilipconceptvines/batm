"""
LEDGER SYSTEM - USAGE EXAMPLES & API GUIDE
==========================================

This file demonstrates how to use the Ledger system in various scenarios.
"""

from datetime import datetime, timedelta
from decimal import Decimal
from sqlalchemy.orm import Session

from app.ledger.service import LedgerService
from app.ledger.models import (
    PostingType,
    PostingCategory,
    PaymentReferenceType,
)


# ============================================================================
# HELPER FUNCTION: GET PAYMENT PERIOD
# ============================================================================

def get_current_payment_period():
    """Get current Sunday-Saturday payment period"""
    today = datetime.now()
    
    # Find current or next Sunday
    days_until_sunday = (6 - today.weekday()) % 7
    if days_until_sunday == 0 and today.weekday() != 6:
        days_until_sunday = 7
    
    sunday = today + timedelta(days=days_until_sunday)
    sunday = sunday.replace(hour=0, minute=0, second=0, microsecond=0)
    saturday = sunday + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
    return sunday, saturday


# ============================================================================
# EXAMPLE 1: RECORD EZPASS TOLL OBLIGATION
# ============================================================================

def example_1_record_ezpass_toll(db: Session):
    """
    Scenario: Driver crosses George Washington Bridge, EZPass toll recorded
    
    Business Flow:
    1. EZPass CSV imported
    2. Toll matched to driver/vehicle
    3. Obligation created in ledger
    """
    service = LedgerService(db)
    start, end = get_current_payment_period()
    
    # Create obligation for EZPass toll
    posting, balance = service.create_obligation(
        driver_id=123,
        lease_id=456,
        category=PostingCategory.EZPASS,
        amount=Decimal('16.00'),  # GWB toll
        reference_type='EZPASS_TRANSACTION',
        reference_id='EZP-20251026-001',
        payment_period_start=start,
        payment_period_end=end,
        due_date=end,  # Due end of week
        description='George Washington Bridge toll - Oct 26, 2025 14:35'
    )
    
    db.commit()
    
    print(f"✓ EZPass toll obligation created:")
    print(f"  Posting ID: {posting.posting_id}")
    print(f"  Balance ID: {balance.balance_id}")
    print(f"  Amount: ${balance.outstanding_balance}")
    print(f"  Status: {balance.status}")
    
    return posting, balance


# ============================================================================
# EXAMPLE 2: RECORD CURB EARNINGS (CREDIT)
# ============================================================================

def example_2_record_curb_earnings(db: Session):
    """
    Scenario: Driver completes trips, CURB deposits earnings to BAT
    
    Business Flow:
    1. CURB API data imported
    2. Calculate net earnings (after taxes)
    3. Create CREDIT posting for earnings
    """
    service = LedgerService(db)
    start, end = get_current_payment_period()
    
    # Driver earned $500 gross, $450 net after taxes
    earnings_posting = service.create_posting(
        driver_id=123,
        lease_id=456,
        posting_type=PostingType.CREDIT,
        category=PostingCategory.EARNINGS,
        amount=Decimal('450.00'),  # Net earnings
        source_type='CURB_EARNINGS',
        source_id='CURB-WEEK-20251026',
        payment_period_start=start,
        payment_period_end=end,
        description='Weekly CURB credit card earnings (net of taxes)'
    )
    
    db.commit()
    
    print(f"✓ CURB earnings recorded:")
    print(f"  Posting ID: {earnings_posting.posting_id}")
    print(f"  Amount: ${earnings_posting.amount}")
    print(f"  Type: {earnings_posting.posting_type}")
    
    return earnings_posting


# ============================================================================
# EXAMPLE 3: WEEKLY DTR PAYMENT ALLOCATION (HIERARCHY)
# ============================================================================

def example_3_weekly_dtr_allocation(db: Session):
    """
    Scenario: Sunday 5:00 AM - Weekly DTR generation and payment allocation
    
    Business Flow:
    1. Calculate total earnings for week
    2. Apply payment following strict hierarchy
    3. Generate DTR showing net payment to driver
    
    Payment Hierarchy:
    1. TAXES (highest priority)
    2. EZPASS
    3. LEASE
    4. PVB
    5. TLC
    6. REPAIRS
    7. LOANS
    8. MISC (lowest priority)
    """
    service = LedgerService(db)
    start, end = get_current_payment_period()
    
    # Driver earned $500 this week
    weekly_earnings = Decimal('500.00')
    
    # Apply payment following hierarchy
    result = service.apply_payment_with_hierarchy(
        driver_id=123,
        lease_id=456,
        payment_amount=weekly_earnings,
        payment_period_start=start,
        payment_period_end=end,
        source_type='DTR_WEEKLY_ALLOCATION',
        source_id='DTR-20251026',
        allocation_type=PaymentReferenceType.DTR_ALLOCATION,
        notes='Weekly DTR payment allocation'
    )
    
    db.commit()
    
    print(f"✓ Weekly payment allocated:")
    print(f"  Total Earnings: ${result.total_payment}")
    print(f"  Amount Allocated: ${result.total_allocated}")
    print(f"  Net to Driver: ${result.remaining_unallocated}")
    print(f"  Allocations Made: {len(result.allocations)}")
    
    print("\n  Payment Breakdown:")
    for alloc in result.allocations:
        print(f"    - Balance {alloc.balance_id}: ${alloc.amount_allocated}")
    
    return result


# ============================================================================
# EXAMPLE 4: INTERIM PAYMENT (DRIVER PAYS SPECIFIC OBLIGATION)
# ============================================================================

def example_4_interim_payment(db: Session):
    """
    Scenario: Driver makes ad-hoc payment for specific violation
    
    Business Flow:
    1. Driver brings cash/check to office
    2. Staff applies payment to specific balance
    3. Payment bypasses hierarchy (targeted payment)
    """
    service = LedgerService(db)
    
    # Driver wants to pay specific PVB violation
    balance_id = 'LB-2025-000123'  # Specific violation
    
    # First, create payment posting
    start, end = get_current_payment_period()
    payment_posting = service.create_posting(
        driver_id=123,
        lease_id=456,
        posting_type=PostingType.CREDIT,
        category=PostingCategory.EARNINGS,  # Payment source
        amount=Decimal('115.00'),
        source_type='INTERIM_PAYMENT_CASH',
        source_id='CASH-20251026-001',
        payment_period_start=start,
        payment_period_end=end,
        description='Driver interim payment - PVB violation'
    )
    
    # Apply payment to specific balance
    allocation, balance = service.apply_payment_to_balance(
        balance_id=balance_id,
        payment_amount=Decimal('115.00'),
        payment_posting_id=payment_posting.posting_id,
        allocation_type=PaymentReferenceType.INTERIM_PAYMENT,
        notes='Cash payment received at office'
    )
    
    db.commit()
    
    print(f"✓ Interim payment applied:")
    print(f"  Payment Amount: ${allocation.amount_allocated}")
    print(f"  Balance ID: {balance.balance_id}")
    print(f"  Remaining: ${balance.outstanding_balance}")
    print(f"  Status: {balance.status}")
    
    return allocation, balance


# ============================================================================
# EXAMPLE 5: ERROR CORRECTION (VOID AND REPOST)
# ============================================================================

def example_5_void_and_repost(db: Session):
    """
    Scenario: Staff entered wrong amount, needs to correct
    
    Business Flow:
    1. Identify incorrect posting
    2. Void the posting (creates reversal)
    3. Create corrected posting
    4. Both original and correction remain in ledger
    """
    service = LedgerService(db)
    
    # Original posting ID (entered as $25 instead of $35)
    incorrect_posting_id = 'LP-2025-000456'
    
    # Step 1: Void incorrect posting
    original, reversal = service.void_posting(
        posting_id=incorrect_posting_id,
        reason='Incorrect amount - should be $35.00 not $25.00',
        user_id=1
    )
    
    db.commit()
    
    print(f"✓ Posting voided:")
    print(f"  Original: {original.posting_id} (VOIDED)")
    print(f"  Reversal: {reversal.posting_id}")
    
    # Step 2: Create corrected posting
    start, end = get_current_payment_period()
    corrected_posting = service.create_posting(
        driver_id=123,
        lease_id=456,
        posting_type=PostingType.DEBIT,
        category=PostingCategory.EZPASS,
        amount=Decimal('35.00'),  # Correct amount
        source_type='EZPASS_TRANSACTION',
        source_id='EZP-CORRECTED-001',
        payment_period_start=start,
        payment_period_end=end,
        description='Corrected EZPass toll amount'
    )
    
    db.commit()
    
    print(f"✓ Corrected posting created:")
    print(f"  New Posting: {corrected_posting.posting_id}")
    print(f"  Amount: ${corrected_posting.amount}")
    
    return original, reversal, corrected_posting


# ============================================================================
# EXAMPLE 6: CHECK DRIVER BALANCE
# ============================================================================

def example_6_check_driver_balance(db: Session):
    """
    Scenario: Staff or driver wants to see current balance
    
    Business Flow:
    1. Query real-time balance
    2. Show breakdown by category
    3. Display total outstanding
    """
    service = LedgerService(db)
    
    balance_summary = service.get_driver_balance(
        driver_id=123,
        lease_id=456
    )
    
    print(f"✓ Driver Balance Summary:")
    print(f"  Driver ID: {balance_summary['driver_id']}")
    print(f"  Lease ID: {balance_summary['lease_id']}")
    print(f"  Total Outstanding: ${balance_summary['total_outstanding']}")
    print(f"\n  Breakdown by Category:")
    
    for category in balance_summary['by_category']:
        print(f"    {category['category'].value}:")
        print(f"      Total Obligations: ${category['total_obligations']}")
        print(f"      Total Paid: ${category['total_paid']}")
        print(f"      Outstanding: ${category['outstanding_balance']}")
        print(f"      Open Balances: {category['open_balance_count']}")
    
    return balance_summary


# ============================================================================
# EXAMPLE 7: QUERY POSTINGS WITH FILTERS
# ============================================================================

def example_7_query_postings(db: Session):
    """
    Scenario: Finance team needs to review all EZPass postings
    
    Business Flow:
    1. Query postings with filters
    2. Export to CSV/Excel for analysis
    """
    service = LedgerService(db)
    
    from app.ledger.schemas import PostingFilters
    
    # Query all EZPASS postings for driver
    filters = PostingFilters(
        driver_id=123,
        category=PostingCategory.EZPASS,
        status=PostingStatus.POSTED,
        limit=100
    )
    
    postings = service.get_postings(filters)
    
    print(f"✓ Found {len(postings)} EZPass postings:")
    for posting in postings[:5]:  # Show first 5
        print(f"  {posting.posting_id}: ${posting.amount} - {posting.description}")
    
    return postings


# ============================================================================
# EXAMPLE 8: COMPLETE WORKFLOW - IMPORT TO DTR
# ============================================================================

def example_8_complete_workflow(db: Session):
    """
    Complete weekly workflow from import to DTR
    
    This demonstrates the full cycle:
    1. Import obligations (EZPass, Lease)
    2. Import earnings (CURB)
    3. Run DTR allocation
    4. Generate net payment
    """
    service = LedgerService(db)
    start, end = get_current_payment_period()
    
    print("=== COMPLETE WEEKLY WORKFLOW ===\n")
    
    # STEP 1: Import obligations
    print("STEP 1: Import Obligations")
    
    # EZPass toll
    ezpass_posting, ezpass_balance = service.create_obligation(
        driver_id=123,
        lease_id=456,
        category=PostingCategory.EZPASS,
        amount=Decimal('45.00'),
        reference_type='EZPASS_TRANSACTION',
        reference_id='EZP-WEEK-001',
        payment_period_start=start,
        payment_period_end=end
    )
    print(f"  ✓ EZPass: ${ezpass_balance.outstanding_balance}")
    
    # Lease payment
    lease_posting, lease_balance = service.create_obligation(
        driver_id=123,
        lease_id=456,
        category=PostingCategory.LEASE,
        amount=Decimal('400.00'),
        reference_type='LEASE_SCHEDULE',
        reference_id='LEASE-WEEK-001',
        payment_period_start=start,
        payment_period_end=end
    )
    print(f"  ✓ Lease: ${lease_balance.outstanding_balance}")
    
    db.commit()
    
    # STEP 2: Check balance before payment
    print("\nSTEP 2: Balance Before Payment")
    balance_before = service.get_driver_balance(123, 456)
    print(f"  Total Outstanding: ${balance_before['total_outstanding']}")
    
    # STEP 3: Import earnings and allocate
    print("\nSTEP 3: Import Earnings & Allocate")
    result = service.apply_payment_with_hierarchy(
        driver_id=123,
        lease_id=456,
        payment_amount=Decimal('500.00'),
        payment_period_start=start,
        payment_period_end=end,
        source_type='DTR_WEEKLY_ALLOCATION',
        source_id='DTR-WEEK-001'
    )
    print(f"  Total Earnings: ${result.total_payment}")
    print(f"  Applied to Obligations: ${result.total_allocated}")
    print(f"  Net to Driver: ${result.remaining_unallocated}")
    
    db.commit()
    
    # STEP 4: Check balance after payment
    print("\nSTEP 4: Balance After Payment")
    balance_after = service.get_driver_balance(123, 456)
    print(f"  Total Outstanding: ${balance_after['total_outstanding']}")
    
    # STEP 5: Summary
    print("\n=== WEEKLY SUMMARY ===")
    print(f"Earnings: ${result.total_payment}")
    print(f"Obligations Paid: ${result.total_allocated}")
    print(f"Net Payment to Driver: ${result.remaining_unallocated}")
    print(f"Remaining Balance: ${balance_after['total_outstanding']}")
    
    return result


# ============================================================================
# FASTAPI ENDPOINT USAGE EXAMPLES
# ============================================================================

"""
API USAGE EXAMPLES (HTTP Requests)
===================================

1. CREATE OBLIGATION
POST /ledger/obligations
{
    "driver_id": 123,
    "lease_id": 456,
    "category": "EZPASS",
    "original_amount": 25.50,
    "reference_type": "EZPASS_TRANSACTION",
    "reference_id": "EZP-20251026-001",
    "payment_period_start": "2025-10-26T00:00:00",
    "payment_period_end": "2025-11-01T23:59:59",
    "description": "GWB toll"
}

2. GET DRIVER BALANCE
GET /ledger/balances/driver/123/lease/456

Response:
{
    "driver_id": 123,
    "lease_id": 456,
    "total_outstanding": 445.00,
    "by_category": [
        {
            "category": "EZPASS",
            "total_obligations": 45.00,
            "total_paid": 0.00,
            "outstanding_balance": 45.00,
            "open_balance_count": 3
        },
        {
            "category": "LEASE",
            "total_obligations": 400.00,
            "total_paid": 0.00,
            "outstanding_balance": 400.00,
            "open_balance_count": 1
        }
    ]
}

3. APPLY PAYMENT WITH HIERARCHY
POST /ledger/payments/apply-hierarchy
{
    "driver_id": 123,
    "lease_id": 456,
    "payment_amount": 500.00,
    "payment_period_start": "2025-10-26T00:00:00",
    "payment_period_end": "2025-11-01T23:59:59",
    "source_type": "DTR_WEEKLY_ALLOCATION",
    "source_id": "DTR-20251026"
}

4. VOID POSTING
POST /ledger/postings/void
{
    "posting_id": "LP-2025-000456",
    "reason": "Incorrect amount entered"
}

5. QUERY POSTINGS
GET /ledger/postings?driver_id=123&category=EZPASS&limit=50

6. QUERY BALANCES
GET /ledger/balances?driver_id=123&status=OPEN&limit=50
"""


# ============================================================================
# MAIN - RUN EXAMPLES
# ============================================================================

if __name__ == '__main__':
    from app.core.db import SessionLocal
    
    db = SessionLocal()
    
    try:
        print("Running Ledger System Examples...\n")
        
        # Run complete workflow
        example_8_complete_workflow(db)
        
    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()