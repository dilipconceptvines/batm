"""empty message

Revision ID: e1bbf5c06371
Revises: 0aa271840799, 5e4aee56d43a
Create Date: 2025-12-29 18:14:28.559592

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e1bbf5c06371'
down_revision: Union[str, Sequence[str], None] = ('0aa271840799', '5e4aee56d43a')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
