"""empty message

Revision ID: 31da4b3e47cd
Revises: 6da7778a4874, 8f23375d69ec, c6a6e5273d74
Create Date: 2025-12-24 14:46:41.911686

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '31da4b3e47cd'
down_revision: Union[str, Sequence[str], None] = ('6da7778a4874', '8f23375d69ec', 'c6a6e5273d74')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
