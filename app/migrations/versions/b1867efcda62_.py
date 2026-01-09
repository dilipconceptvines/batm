"""empty message

Revision ID: b1867efcda62
Revises: e1c518a260d3
Create Date: 2025-12-28 09:06:16.264237

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b1867efcda62'
down_revision: Union[str, Sequence[str], None] = ('e1c518a260d3',)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
