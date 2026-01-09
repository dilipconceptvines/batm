"""update ledger_postings category enum to include full miscellaneous names

Revision ID: db897e004f52
Revises: e1bbf5c06371
Create Date: 2025-12-29 18:34:07.141454

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'db897e004f52'
down_revision: Union[str, Sequence[str], None] = 'e1bbf5c06371'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Alter the category enum to include full miscellaneous names for both tables
    op.execute("""
        ALTER TABLE ledger_postings 
        MODIFY COLUMN category ENUM(
            'LEASE', 
            'REPAIR', 
            'LOAN', 
            'EZPASS', 
            'PVB', 
            'TLC', 
            'TAXES', 
            'MISC', 
            'EARNINGS', 
            'INTERIM_PAYMENT', 
            'DEPOSIT', 
            'CANCELLATION_FEE',
            'MISCELLANEOUS_EXPENSE',
            'MISCELLANEOUS_CREDIT'
        )
    """)
    
    op.execute("""
        ALTER TABLE ledger_balances 
        MODIFY COLUMN category ENUM(
            'LEASE', 
            'REPAIR', 
            'LOAN', 
            'EZPASS', 
            'PVB', 
            'TLC', 
            'TAXES', 
            'MISC', 
            'EARNINGS', 
            'INTERIM_PAYMENT', 
            'DEPOSIT', 
            'CANCELLATION_FEE',
            'MISCELLANEOUS_EXPENSE',
            'MISCELLANEOUS_CREDIT'
        )
    """)


def downgrade() -> None:
    """Downgrade schema."""
    # Revert to the original enum values for both tables
    op.execute("""
        ALTER TABLE ledger_postings 
        MODIFY COLUMN category ENUM(
            'LEASE', 
            'REPAIR', 
            'LOAN', 
            'EZPASS', 
            'PVB', 
            'TLC', 
            'TAXES', 
            'MISC', 
            'EARNINGS', 
            'INTERIM_PAYMENT', 
            'DEPOSIT', 
            'CANCELLATION_FEE'
        )
    """)
    
    op.execute("""
        ALTER TABLE ledger_balances 
        MODIFY COLUMN category ENUM(
            'LEASE', 
            'REPAIR', 
            'LOAN', 
            'EZPASS', 
            'PVB', 
            'TLC', 
            'TAXES', 
            'MISC', 
            'EARNINGS', 
            'INTERIM_PAYMENT', 
            'DEPOSIT', 
            'CANCELLATION_FEE'
        )
    """)
