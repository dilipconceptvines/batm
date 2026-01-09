"""empty message

Revision ID: 12ec3e31fa46
Revises: 5502ff6526d6
Create Date: 2025-12-29 16:59:32.902685

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '12ec3e31fa46'
down_revision: Union[str, Sequence[str], None] = ('5502ff6526d6',)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
