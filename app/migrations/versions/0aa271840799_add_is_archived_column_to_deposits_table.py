"""add is_archived column to deposits table

Revision ID: 0aa271840799
Revises: 6c0ca4dacc07
Create Date: 2025-12-29 13:00:54.929541

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0aa271840799'
down_revision: Union[str, Sequence[str], None] = '6c0ca4dacc07'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add is_archived column to deposits table
    op.add_column('deposits', sa.Column('is_archived', sa.Boolean(), nullable=True, default=False, comment='Flag indicating if the record is archived'))
    # Add is_active column as well, since AuditMixin has both
    op.add_column('deposits', sa.Column('is_active', sa.Boolean(), nullable=True, default=True, comment='Flag to keep track of record is active or not'))


def downgrade() -> None:
    """Downgrade schema."""
    # Remove the added columns
    op.drop_column('deposits', 'is_active')
    op.drop_column('deposits', 'is_archived')
