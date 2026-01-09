"""add_status_tracking_to_interim_payments

Revision ID: 6c0ca4dacc07
Revises: f1025427b944
Create Date: 2025-12-28 19:44:48.905886

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6c0ca4dacc07'
down_revision: Union[str, Sequence[str], None] = 'f1025427b944'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add status tracking fields to interim_payments table."""
    # Create enum type for payment status
    payment_status = sa.Enum('ACTIVE', 'VOIDED', name='payment_status')
    payment_status.create(op.get_bind())

    # Add status field (default ACTIVE)
    op.add_column('interim_payments',
        sa.Column('status',
                  payment_status,
                  nullable=False,
                  server_default='ACTIVE'))

    # Add void tracking fields
    op.add_column('interim_payments',
        sa.Column('voided_at', sa.DateTime(timezone=True), nullable=True))

    op.add_column('interim_payments',
        sa.Column('voided_by', sa.Integer, nullable=True))

    op.add_column('interim_payments',
        sa.Column('void_reason', sa.Text, nullable=True))

    # Add foreign key to users table
    op.create_foreign_key(
        'fk_interim_payments_voided_by',
        'interim_payments',
        'users',
        ['voided_by'],
        ['id']
    )

    # Create index for querying by status
    op.create_index(
        'ix_interim_payments_status',
        'interim_payments',
        ['status']
    )


def downgrade() -> None:
    """Remove status tracking fields from interim_payments table."""
    op.drop_index('ix_interim_payments_status', 'interim_payments')
    op.drop_constraint('fk_interim_payments_voided_by', 'interim_payments')
    op.drop_column('interim_payments', 'void_reason')
    op.drop_column('interim_payments', 'voided_by')
    op.drop_column('interim_payments', 'voided_at')
    op.drop_column('interim_payments', 'status')

    # Drop enum type
    sa.Enum(name='payment_status').drop(op.get_bind())
