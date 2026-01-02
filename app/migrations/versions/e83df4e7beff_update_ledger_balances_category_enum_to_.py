"""update ledger_balances category enum to include full miscellaneous names

Revision ID: e83df4e7beff
Revises: db897e004f52
Create Date: 2025-12-29 18:56:29.423427

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e83df4e7beff'
down_revision: Union[str, Sequence[str], None] = 'db897e004f52'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Alter the category enum to include full miscellaneous names
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
    # Revert to the original enum values
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
