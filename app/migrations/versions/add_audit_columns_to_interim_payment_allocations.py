"""Add audit columns to interim_payment_allocations table

Revision ID: add_audit_interim_alloc
Revises: 
Create Date: 2026-01-09 08:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime
from sqlalchemy.sql import func


# revision identifiers, used by Alembic.
revision = 'add_audit_interim_alloc'
down_revision = None  # Replace with the actual previous migration revision
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add modified_by column
    op.add_column(
        'interim_payment_allocations',
        sa.Column(
            'modified_by',
            sa.Integer(),
            sa.ForeignKey('users.id', ondelete='SET NULL', onupdate='CASCADE'),
            nullable=True,
            comment='User who last modified this record'
        )
    )
    
    # Add updated_on column with default to current timestamp
    op.add_column(
        'interim_payment_allocations',
        sa.Column(
            'updated_on',
            sa.DateTime(timezone=True),
            server_default=func.now(),
            onupdate=func.now(),
            nullable=True,
            comment='Timestamp when this record was last updated'
        )
    )
    
    # Add is_archived column
    op.add_column(
        'interim_payment_allocations',
        sa.Column(
            'is_archived',
            sa.Boolean(),
            nullable=True,
            default=False,
            comment='Flag indicating if the record is archived'
        )
    )
    
    # Add is_active column if it doesn't exist
    # First, check if column exists by trying to get its info
    # (Alembic doesn't have a direct way, so we'll add it unconditionally)
    op.add_column(
        'interim_payment_allocations',
        sa.Column(
            'is_active',
            sa.Boolean(),
            nullable=True,
            default=True,
            comment='Flag to keep track if record is active or not'
        )
    )


def downgrade() -> None:
    # Remove columns in reverse order
    op.drop_column('interim_payment_allocations', 'is_active')
    op.drop_column('interim_payment_allocations', 'is_archived')
    op.drop_column('interim_payment_allocations', 'updated_on')
    op.drop_column('interim_payment_allocations', 'modified_by')