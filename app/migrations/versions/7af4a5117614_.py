"""empty message

Revision ID: 7af4a5117614
Revises: 7f62697feffc, ee935892a1e9
Create Date: 2025-12-22 05:17:57.970357

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7af4a5117614'
down_revision: Union[str, Sequence[str], None] = ('7f62697feffc', 'ee935892a1e9')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
