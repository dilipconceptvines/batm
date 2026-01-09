"""empty message

Revision ID: 864b656198b6
Revises: 12ec3e31fa46
Create Date: 2025-12-29 17:11:07.553064

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '864b656198b6'
down_revision: Union[str, Sequence[str], None] = ('12ec3e31fa46',)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
