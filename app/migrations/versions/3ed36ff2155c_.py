"""empty message

Revision ID: 3ed36ff2155c
Revises: 1cc54269c0fa, add_audit_interim_alloc
Create Date: 2026-01-09 14:33:43.204674

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3ed36ff2155c'
down_revision: Union[str, Sequence[str], None] = ('1cc54269c0fa', 'add_audit_interim_alloc')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
