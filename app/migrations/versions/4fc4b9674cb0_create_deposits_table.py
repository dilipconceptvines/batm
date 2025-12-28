"""Create deposits table

Revision ID: 4fc4b9674cb0
Revises: ee935892a1e9
Create Date: 2025-12-28 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4fc4b9674cb0'
down_revision: Union[str, Sequence[str], None] = 'ee935892a1e9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create depositstatus enum
    deposit_status_enum = sa.Enum('PENDING', 'PARTIALLY_PAID', 'PAID', 'HELD', 'REFUNDED', name='depositstatus')
    deposit_status_enum.create(op.get_bind(), checkfirst=True)

    # Create collectionmethod enum
    collection_method_enum = sa.Enum('CASH', 'CHECK', 'ACH', name='collectionmethod')
    collection_method_enum.create(op.get_bind(), checkfirst=True)

    # Create deposits table
    op.create_table('deposits',
        sa.Column('deposit_id', sa.String(length=50), nullable=False, comment='Unique deposit identifier in format DEP-{LEASE_ID}-01'),
        sa.Column('lease_id', sa.Integer(), nullable=False, comment='Foreign key to leases table, unique constraint ensures one deposit per lease'),
        sa.Column('driver_tlc_license', sa.String(length=20), nullable=True, comment='Driver\'s TLC license number for quick lookups'),
        sa.Column('vehicle_vin', sa.String(length=17), nullable=True, comment='Vehicle VIN for reference'),
        sa.Column('vehicle_plate', sa.String(length=10), nullable=True, comment='Vehicle license plate for reference'),
        sa.Column('required_amount', sa.Numeric(precision=10, scale=2), nullable=False, server_default='0.00', comment='Required deposit amount (default: 1 week lease fee)'),
        sa.Column('collected_amount', sa.Numeric(precision=10, scale=2), nullable=False, server_default='0.00', comment='Total amount collected so far'),
        sa.Column('outstanding_amount', sa.Numeric(precision=10, scale=2), nullable=False, server_default='0.00', comment='Remaining amount to be collected (required - collected)'),
        sa.Column('initial_collection_amount', sa.Numeric(precision=10, scale=2), nullable=True, comment='Amount of initial collection'),
        sa.Column('collection_method', collection_method_enum, nullable=True, comment='Method used for collecting the deposit'),
        sa.Column('deposit_status', deposit_status_enum, nullable=False, comment='Current status of the deposit'),
        sa.Column('lease_start_date', sa.Date(), nullable=True, comment='Lease start date for reference'),
        sa.Column('lease_termination_date', sa.Date(), nullable=True, comment='Date when lease was terminated'),
        sa.Column('hold_expiry_date', sa.Date(), nullable=True, comment='Date when 30-day hold period expires'),
        sa.Column('refund_amount', sa.Numeric(precision=10, scale=2), nullable=True, comment='Amount refunded to driver'),
        sa.Column('refund_date', sa.Date(), nullable=True, comment='Date when refund was processed'),
        sa.Column('refund_method', sa.String(length=50), nullable=True, comment='Method used for refund (Cash, Check, ACH)'),
        sa.Column('refund_reference', sa.String(length=100), nullable=True, comment='Reference number for refund transaction'),
        sa.Column('reminder_flags', sa.JSON(), nullable=True, comment='JSON object tracking reminder alerts (e.g., {\'week1\': true, \'week2\': true})'),
        sa.Column('notes', sa.Text(), nullable=True, comment='Additional notes about the deposit'),
        sa.Column('created_by', sa.Integer(), nullable=True, comment='User who created this record'),
        sa.Column('modified_by', sa.Integer(), nullable=True, comment='User who last modified this record'),
        sa.Column('created_on', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True, comment='Timestamp when this record was created'),
        sa.Column('updated_on', sa.DateTime(timezone=True), nullable=True, comment='Timestamp when this record was last updated'),
        sa.PrimaryKeyConstraint('deposit_id'),
        sa.ForeignKeyConstraint(['lease_id'], ['leases.id'], ),
        sa.ForeignKeyConstraint(['created_by'], ['users.id'], ondelete='SET NULL', onupdate='CASCADE'),
        sa.ForeignKeyConstraint(['modified_by'], ['users.id'], ondelete='SET NULL', onupdate='CASCADE'),
        sa.UniqueConstraint('lease_id')
    )

    # Create indexes
    op.create_index('idx_deposits_status_lease', 'deposits', ['deposit_status', 'lease_id'], unique=False)
    op.create_index('idx_deposits_tlc_status', 'deposits', ['driver_tlc_license', 'deposit_status'], unique=False)
    op.create_index('idx_deposits_hold_expiry', 'deposits', ['hold_expiry_date', 'deposit_status'], unique=False)
    op.create_index(op.f('ix_deposits_deposit_id'), 'deposits', ['deposit_id'], unique=False)
    op.create_index(op.f('ix_deposits_driver_tlc_license'), 'deposits', ['driver_tlc_license'], unique=False)
    op.create_index(op.f('ix_deposits_lease_id'), 'deposits', ['lease_id'], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop indexes
    op.drop_index(op.f('ix_deposits_lease_id'), table_name='deposits')
    op.drop_index(op.f('ix_deposits_driver_tlc_license'), table_name='deposits')
    op.drop_index(op.f('ix_deposits_deposit_id'), table_name='deposits')
    op.drop_index('idx_deposits_hold_expiry', table_name='deposits')
    op.drop_index('idx_deposits_tlc_status', table_name='deposits')
    op.drop_index('idx_deposits_status_lease', table_name='deposits')

    # Drop table
    op.drop_table('deposits')

    # Drop enums
    collection_method_enum = sa.Enum('CASH', 'CHECK', 'ACH', name='collectionmethod')
    collection_method_enum.drop(op.get_bind(), checkfirst=True)

    deposit_status_enum = sa.Enum('PENDING', 'PARTIALLY_PAID', 'PAID', 'HELD', 'REFUNDED', name='depositstatus')
    deposit_status_enum.drop(op.get_bind(), checkfirst=True)