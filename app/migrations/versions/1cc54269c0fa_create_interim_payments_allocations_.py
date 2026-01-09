"""create interim payments allocations table

Revision ID: 1cc54269c0fa
Revises: 2b419df1da62
Create Date: 2026-01-09 11:12:07.993563

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1cc54269c0fa'
down_revision: Union[str, Sequence[str], None] = '2b419df1da62'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create structured interim_payment_allocations table"""
    
    op.create_table(
        'interim_payment_allocations',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('interim_payment_id', sa.Integer(), nullable=False, 
                  comment='Foreign key to interim_payments'),
        sa.Column('ledger_balance_id', sa.String(36), nullable=False,
                  comment='Foreign key to ledger_balances'),
        sa.Column('category', sa.String(50), nullable=False,
                  comment='Category of obligation (LEASE, REPAIR, etc)'),
        sa.Column('reference_id', sa.String(255), nullable=False,
                  comment='Reference ID of the original obligation'),
        sa.Column('allocated_amount', sa.Numeric(10, 2), nullable=False,
                  comment='Amount allocated to this obligation'),
        sa.Column('balance_before', sa.Numeric(10, 2), nullable=True,
                  comment='Balance before this allocation'),
        sa.Column('balance_after', sa.Numeric(10, 2), nullable=True,
                  comment='Balance after this allocation'),
        sa.Column('created_on', sa.DateTime(), nullable=False,
                  server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('created_by', sa.Integer(), nullable=True),
        
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['interim_payment_id'], ['interim_payments.id'], 
                                name='fk_allocations_interim_payment',
                                ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['ledger_balance_id'], ['ledger_balances.id'],
                                name='fk_allocations_ledger_balance'),
        sa.ForeignKeyConstraint(['created_by'], ['users.id'],
                                name='fk_allocations_created_by')
    )
    
    # Create indexes for faster queries
    op.create_index('ix_allocations_interim_payment_id', 
                    'interim_payment_allocations', ['interim_payment_id'])
    op.create_index('ix_allocations_category', 
                    'interim_payment_allocations', ['category'])
    op.create_index('ix_allocations_reference_id', 
                    'interim_payment_allocations', ['reference_id'])
    op.create_index('ix_allocations_ledger_balance_id',
                    'interim_payment_allocations', ['ledger_balance_id'])


def downgrade() -> None:
    """Drop interim_payment_allocations table"""
    op.drop_index('ix_allocations_ledger_balance_id', table_name='interim_payment_allocations')
    op.drop_index('ix_allocations_reference_id', table_name='interim_payment_allocations')
    op.drop_index('ix_allocations_category', table_name='interim_payment_allocations')
    op.drop_index('ix_allocations_interim_payment_id', table_name='interim_payment_allocations')
    op.drop_table('interim_payment_allocations')
