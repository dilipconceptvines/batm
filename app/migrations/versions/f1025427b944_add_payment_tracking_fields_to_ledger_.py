"""add_payment_tracking_fields_to_ledger_postings

Revision ID: f1025427b944
Revises: b1867efcda62
Create Date: 2025-12-28 19:38:50.960169

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f1025427b944'
down_revision: Union[str, Sequence[str], None] = 'b1867efcda62'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add payment tracking fields to ledger_postings table."""
    # Add payment_source field (tracks origin of payment)
    op.add_column('ledger_postings', 
        sa.Column('payment_source', sa.String(50), nullable=True))
    
    # Add payment_method field (CASH, CHECK, ACH, etc.)
    op.add_column('ledger_postings', 
        sa.Column('payment_method', sa.String(20), nullable=True))
    
    # Add description field for human-readable descriptions
    op.add_column('ledger_postings', 
        sa.Column('description', sa.String(255), nullable=True))
    
    # Create index for querying interim payments
    op.create_index(
        'ix_ledger_postings_payment_source',
        'ledger_postings',
        ['payment_source']
    )
    
    # Backfill existing interim payment postings
    # Identify them by description pattern or reference_id pattern
    op.execute("""
        UPDATE ledger_postings 
        SET payment_source = 'INTERIM_PAYMENT'
        WHERE reference_id LIKE 'PAYMENT-%'
          AND entry_type = 'CREDIT'
          AND category IN ('REPAIRS', 'LOANS', 'EZPASS', 'LEASE', 'PVB', 'TLC')
    """)
    
    # Extract payment method from reference_id pattern
    op.execute("""
        UPDATE ledger_postings 
        SET payment_method = 
            CASE 
                WHEN reference_id LIKE 'PAYMENT-CASH-%' THEN 'CASH'
                WHEN reference_id LIKE 'PAYMENT-CHECK-%' THEN 'CHECK'
                WHEN reference_id LIKE 'PAYMENT-ACH-%' THEN 'ACH'
                ELSE NULL
            END
        WHERE payment_source = 'INTERIM_PAYMENT'
    """)
    
    # Set description for backfilled records
    op.execute("""
        UPDATE ledger_postings 
        SET description = CONCAT('Interim payment via ', COALESCE(payment_method, 'UNKNOWN'))
        WHERE payment_source = 'INTERIM_PAYMENT'
          AND description IS NULL
    """)


def downgrade() -> None:
    """Remove payment tracking fields from ledger_postings table."""
    op.drop_index('ix_ledger_postings_payment_source', 'ledger_postings')
    op.drop_column('ledger_postings', 'description')
    op.drop_column('ledger_postings', 'payment_method')
    op.drop_column('ledger_postings', 'payment_source')
