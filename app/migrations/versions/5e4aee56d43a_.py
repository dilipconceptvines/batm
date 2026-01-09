"""empty message

Revision ID: 5e4aee56d43a
Revises: 4fc4b9674cb0, 864b656198b6
Create Date: 2025-12-29 17:12:52.549831

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5e4aee56d43a'
down_revision: Union[str, Sequence[str], None] = ('4fc4b9674cb0', '864b656198b6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
