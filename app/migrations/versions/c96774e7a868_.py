"""empty message

Revision ID: c96774e7a868
Revises: 6c0ca4dacc07
Create Date: 2025-12-29 15:54:03.445842

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c96774e7a868'
down_revision: Union[str, Sequence[str], None] = ('6c0ca4dacc07',)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
