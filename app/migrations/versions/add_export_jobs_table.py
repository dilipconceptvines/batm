"""create export_jobs table

Revision ID: add_export_jobs_table
Revises: 579cf62667a8
Create Date: 2025-01-07 16:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = 'add_export_jobs_table'
down_revision: Union[str, None] = '579cf62667a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create export_jobs table"""
    
    # Create export_jobs table
    op.create_table(
        'export_jobs',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('export_type', sa.Enum('EZPASS', 'PVB', 'CURB', 'LEDGER_POSTINGS', 'LEDGER_BALANCES', name='exporttype'), nullable=False),
        sa.Column('format', sa.Enum('excel', 'csv', 'pdf', 'json', name='exportformat'), nullable=False),
        sa.Column('status', sa.Enum('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', name='exportstatus'), nullable=False),
        sa.Column('celery_task_id', sa.String(length=255), nullable=True),
        sa.Column('filters', sa.JSON(), nullable=True),
        sa.Column('file_url', sa.String(length=500), nullable=True),
        sa.Column('file_name', sa.String(length=255), nullable=True),
        sa.Column('total_records', sa.Integer(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('created_by', sa.Integer(), nullable=False),
        sa.Column('updated_by', sa.Integer(), nullable=True),
        sa.Column('created_on', sa.DateTime(), nullable=True),
        sa.Column('updated_on', sa.DateTime(), nullable=True),
        
        # Primary key
        sa.PrimaryKeyConstraint('id'),
        
        # Foreign keys
        sa.ForeignKeyConstraint(['created_by'], ['users.id'], name='fk_export_jobs_created_by'),
        
        # Indexes for performance
        sa.Index('ix_export_jobs_export_type', 'export_type'),
        sa.Index('ix_export_jobs_status', 'status'),
        sa.Index('ix_export_jobs_celery_task_id', 'celery_task_id'),
        sa.Index('ix_export_jobs_created_at', 'created_at'),
        sa.Index('ix_export_jobs_created_by', 'created_by'),
    )


def downgrade() -> None:
    """Drop export_jobs table"""
    op.drop_table('export_jobs')
    
    # Drop custom enums
    op.execute("DROP TYPE IF EXISTS exporttype")
    op.execute("DROP TYPE IF EXISTS exportformat")
    op.execute("DROP TYPE IF EXISTS exportstatus")
