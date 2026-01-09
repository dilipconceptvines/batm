"""ezpass composite indices

Revision ID: e1c518a260d3
Revises: 694e35a4b91a
Create Date: 2025-12-26 16:13:42.681849

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e1c518a260d3'
down_revision: Union[str, Sequence[str], None] = '694e35a4b91a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Add strategic composite indexes for EZPass transactions table.
    
    These indexes are designed to optimize queries with millions of records:
    - Composite indexes for commonly used filter combinations
    - Single-column indexes for foreign keys and frequently filtered columns
    - Covering indexes for common query patterns
    """
    
    print("="*80)
    print("ADDING STRATEGIC INDEXES FOR EZPASS TRANSACTIONS")
    print("="*80)
    
    # ==================================================================
    # STEP 1: Single-column indexes for basic filtering
    # ==================================================================
    print("\n1. Creating single-column indexes...")
    
    # Status index (frequently used for filtering)
    op.create_index(
        'idx_ezpass_status',
        'ezpass_transactions',
        ['status']
    )
    
    # Transaction datetime index (used for date range filtering)
    op.create_index(
        'idx_ezpass_transaction_datetime',
        'ezpass_transactions',
        ['transaction_datetime']
    )
    
    # Posting date index (used for posted date filtering)
    op.create_index(
        'idx_ezpass_posting_date',
        'ezpass_transactions',
        ['posting_date']
    )
    
    # Amount index (used for amount range filtering)
    op.create_index(
        'idx_ezpass_amount',
        'ezpass_transactions',
        ['amount']
    )
    
    # Tag/Plate index (frequently searched)
    op.create_index(
        'idx_ezpass_tag_plate',
        'ezpass_transactions',
        ['tag_or_plate']
    )
    
    # Transaction ID already has unique index from model
    
    # Entry/Exit plaza indexes
    op.create_index(
        'idx_ezpass_entry_plaza',
        'ezpass_transactions',
        ['entry_plaza']
    )
    
    op.create_index(
        'idx_ezpass_exit_plaza',
        'ezpass_transactions',
        ['exit_plaza']
    )
    
    # Agency index
    op.create_index(
        'idx_ezpass_agency',
        'ezpass_transactions',
        ['agency']
    )
    
    print("✓ Single-column indexes created")
    
    # ==================================================================
    # STEP 2: Composite indexes for optimized query patterns
    # ==================================================================
    print("\n2. Creating composite indexes for optimized queries...")
    
    # Most common query pattern: status + transaction_datetime
    # Used for listing transactions by status and date range
    op.create_index(
        'idx_ezpass_status_datetime',
        'ezpass_transactions',
        ['status', 'transaction_datetime']
    )
    
    # Driver + datetime (for driver-specific queries)
    op.create_index(
        'idx_ezpass_driver_datetime',
        'ezpass_transactions',
        ['driver_id', 'transaction_datetime']
    )
    
    # Vehicle + datetime (for vehicle-specific queries)
    op.create_index(
        'idx_ezpass_vehicle_datetime',
        'ezpass_transactions',
        ['vehicle_id', 'transaction_datetime']
    )
    
    # Medallion + datetime (for medallion-specific queries)
    op.create_index(
        'idx_ezpass_medallion_datetime',
        'ezpass_transactions',
        ['medallion_id', 'transaction_datetime']
    )
    
    # Lease + datetime (for lease-specific queries)
    op.create_index(
        'idx_ezpass_lease_datetime',
        'ezpass_transactions',
        ['lease_id', 'transaction_datetime']
    )
    
    # Import batch + status (for import processing queries)
    op.create_index(
        'idx_ezpass_import_status',
        'ezpass_transactions',
        ['import_id', 'status']
    )
    
    # Status + driver + datetime (for ready-to-post queries)
    op.create_index(
        'idx_ezpass_ready_for_ledger',
        'ezpass_transactions',
        ['status', 'driver_id', 'transaction_datetime']
    )
    
    # Posting date + status (for posted transactions queries)
    op.create_index(
        'idx_ezpass_posted_transactions',
        'ezpass_transactions',
        ['posting_date', 'status']
    )
    
    print("✓ Composite indexes created")
    
    # ==================================================================
    # STEP 3: Print summary
    # ==================================================================
    print("\n" + "="*80)
    print("EZPASS INDEXES MIGRATION COMPLETED SUCCESSFULLY")
    print("="*80)
    print("\nIndexes created:")
    print("  - 8 single-column indexes")
    print("  - 8 composite indexes")
    print("\nTotal: 16 strategic indexes for optimal query performance")
    print("="*80)


def downgrade() -> None:
    """Remove the strategic indexes"""
    
    print("Removing EZPass strategic indexes...")
    
    # Drop composite indexes
    op.drop_index('idx_ezpass_posted_transactions', 'ezpass_transactions')
    op.drop_index('idx_ezpass_ready_for_ledger', 'ezpass_transactions')
    op.drop_index('idx_ezpass_import_status', 'ezpass_transactions')
    op.drop_index('idx_ezpass_lease_datetime', 'ezpass_transactions')
    op.drop_index('idx_ezpass_medallion_datetime', 'ezpass_transactions')
    op.drop_index('idx_ezpass_vehicle_datetime', 'ezpass_transactions')
    op.drop_index('idx_ezpass_driver_datetime', 'ezpass_transactions')
    op.drop_index('idx_ezpass_status_datetime', 'ezpass_transactions')
    
    # Drop single-column indexes
    op.drop_index('idx_ezpass_agency', 'ezpass_transactions')
    op.drop_index('idx_ezpass_exit_plaza', 'ezpass_transactions')
    op.drop_index('idx_ezpass_entry_plaza', 'ezpass_transactions')
    op.drop_index('idx_ezpass_tag_plate', 'ezpass_transactions')
    op.drop_index('idx_ezpass_amount', 'ezpass_transactions')
    op.drop_index('idx_ezpass_posting_date', 'ezpass_transactions')
    op.drop_index('idx_ezpass_transaction_datetime', 'ezpass_transactions')
    op.drop_index('idx_ezpass_status', 'ezpass_transactions')
    
    print("All EZPass indexes removed")
