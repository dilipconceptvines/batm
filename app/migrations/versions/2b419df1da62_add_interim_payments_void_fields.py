"""add interim payments void fields

Revision ID: 2b419df1da62
Revises: add_export_jobs_table
Create Date: 2026-01-09 11:06:53.412344

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


# revision identifiers, used by Alembic.
revision: str = '2b419df1da62'
down_revision: Union[str, Sequence[str], None] = 'add_export_jobs_table'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def column_exists(table_name, column_name):
    """Check if a column exists in the table"""
    bind = op.get_bind()
    inspector = Inspector.from_engine(bind)
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    return column_name in columns


def index_exists(table_name, index_name):
    """Check if an index exists on the table"""
    bind = op.get_bind()
    inspector = Inspector.from_engine(bind)
    indexes = [idx['name'] for idx in inspector.get_indexes(table_name)]
    return index_name in indexes


def foreign_key_exists(table_name, fk_name):
    """Check if a foreign key exists on the table"""
    bind = op.get_bind()
    inspector = Inspector.from_engine(bind)
    foreign_keys = inspector.get_foreign_keys(table_name)
    fk_names = [fk['name'] for fk in foreign_keys if fk.get('name')]
    return fk_name in fk_names

def upgrade() -> None:
    """Add void-related fields and status to interim_payments table"""
    
    print("Starting migration: add_interim_payments_void_fields")
    
    # 1. Add status column if it doesn't exist
    if not column_exists('interim_payments', 'status'):
        print("  ✓ Adding 'status' column...")
        op.add_column('interim_payments', 
            sa.Column('status', 
                      sa.Enum('ACTIVE', 'VOIDED', name='paymentstatus'),
                      nullable=False, 
                      server_default='ACTIVE',
                      comment='Status of the interim payment'))
        
        # Add index on status
        if not index_exists('interim_payments', 'ix_interim_payments_status'):
            print("  ✓ Adding index on 'status' column...")
            op.create_index('ix_interim_payments_status', 'interim_payments', ['status'])
    else:
        print("  ⊘ Column 'status' already exists, skipping...")
    
    # 2. Add voided_at timestamp if it doesn't exist
    if not column_exists('interim_payments', 'voided_at'):
        print("  ✓ Adding 'voided_at' column...")
        op.add_column('interim_payments',
            sa.Column('voided_at', sa.DateTime(), nullable=True,
                      comment='Timestamp when payment was voided'))
    else:
        print("  ⊘ Column 'voided_at' already exists, skipping...")
    
    # 3. Add voided_by user reference if it doesn't exist
    if not column_exists('interim_payments', 'voided_by'):
        print("  ✓ Adding 'voided_by' column...")
        op.add_column('interim_payments',
            sa.Column('voided_by', sa.Integer, nullable=True,
                      comment='User ID who voided the payment'))
        
        # Add foreign key for voided_by
        if not foreign_key_exists('interim_payments', 'fk_interim_payments_voided_by_users'):
            print("  ✓ Adding foreign key constraint on 'voided_by'...")
            op.create_foreign_key(
                'fk_interim_payments_voided_by_users',
                'interim_payments', 'users',
                ['voided_by'], ['id']
            )
    else:
        print("  ⊘ Column 'voided_by' already exists, skipping...")
    
    # 4. Add void_reason text field if it doesn't exist
    if not column_exists('interim_payments', 'void_reason'):
        print("  ✓ Adding 'void_reason' column...")
        op.add_column('interim_payments',
            sa.Column('void_reason', sa.String(500), nullable=True,
                      comment='Reason for voiding the payment'))
    else:
        print("  ⊘ Column 'void_reason' already exists, skipping...")
    
    print("Migration completed successfully!")


def downgrade() -> None:
    """Remove void-related fields"""
    
    print("Starting downgrade: remove void-related fields")
    
    # Drop index first if it exists
    if index_exists('interim_payments', 'ix_interim_payments_status'):
        print("  ✓ Dropping index 'ix_interim_payments_status'...")
        op.drop_index('ix_interim_payments_status', table_name='interim_payments')
    
    # Drop foreign key if it exists
    if foreign_key_exists('interim_payments', 'fk_interim_payments_voided_by_users'):
        print("  ✓ Dropping foreign key constraint...")
        try:
            op.drop_constraint('fk_interim_payments_voided_by_users', 'interim_payments', type_='foreignkey')
        except Exception as e:
            print(f"  ⚠ Warning: Could not drop foreign key: {e}")
    
    # Drop columns if they exist (in reverse order)
    columns_to_drop = ['void_reason', 'voided_by', 'voided_at', 'status']
    
    for column in columns_to_drop:
        if column_exists('interim_payments', column):
            print(f"  ✓ Dropping column '{column}'...")
            op.drop_column('interim_payments', column)
        else:
            print(f"  ⊘ Column '{column}' does not exist, skipping...")
    
    print("Downgrade completed successfully!")
