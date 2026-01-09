"""empty message

Revision ID: 5502ff6526d6
Revises: c96774e7a868
Create Date: 2025-12-29 16:37:47.909146

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5502ff6526d6'
down_revision: Union[str, Sequence[str], None] = ('c96774e7a868',)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
