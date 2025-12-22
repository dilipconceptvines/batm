"""empty message

Revision ID: abab11b9bf99
Revises: 342cbcdb8ccf, aaed9d2917f3
Create Date: 2025-12-11 16:08:42.109653

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'abab11b9bf99'
down_revision: Union[str, Sequence[str], None] = ('342cbcdb8ccf', 'aaed9d2917f3')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
