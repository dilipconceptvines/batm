"""Add receipt and case fields to misc expenses

Revision ID: 694e35a4b91a
Revises: 31da4b3e47cd
Create Date: 2025-12-26 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '694e35a4b91a'
down_revision: Union[str, Sequence[str], None] = '31da4b3e47cd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add payment_type enum and column
    payment_type_enum = sa.Enum('EXPENSE', 'CREDIT', name='paymenttype')
    payment_type_enum.create(op.get_bind(), checkfirst=True)
    
    op.add_column('miscellaneous_expenses',
        sa.Column('payment_type', payment_type_enum, nullable=False, server_default='EXPENSE',
                  comment='EXPENSE (charge) or CREDIT (payment)'))
    
    # Drop case_no field (no longer needed)
    op.drop_column('miscellaneous_expenses', 'case_no')
    
    # Add receipt storage fields (consistent with driver_loans, interim_payments, repair_invoices)
    op.add_column('miscellaneous_expenses', 
        sa.Column('receipt_s3_key', sa.String(length=512), nullable=True, comment='S3 key where the expense receipt PDF is stored'))
    op.add_column('miscellaneous_expenses', 
        sa.Column('receipt_url', sa.String(length=1024), nullable=True, comment='Presigned URL for accessing the expense receipt'))
    
    # Create index for faster lookups
    op.create_index('idx_misc_pay_type_status', 'miscellaneous_expenses', ['payment_type', 'status'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop index first
    op.drop_index('idx_misc_pay_type_status', table_name='miscellaneous_expenses')
    
    # Drop columns
    op.drop_column('miscellaneous_expenses', 'receipt_url')
    op.drop_column('miscellaneous_expenses', 'receipt_s3_key')
    
    # Add case_no back
    op.add_column('miscellaneous_expenses', 
        sa.Column('case_no', sa.String(length=50), nullable=True))
    
    op.drop_column('miscellaneous_expenses', 'payment_type')
    
    # Drop enum type
    payment_type_enum = sa.Enum('EXPENSE', 'CREDIT', name='paymenttype')
    payment_type_enum.drop(op.get_bind(), checkfirst=True)
