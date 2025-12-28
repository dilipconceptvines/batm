"""Migrate existing deposit data from leases to deposits table

Revision ID: 3c73231e1014
Revises: e9ec2212954d
Create Date: 2025-12-27 22:50:50.531335

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3c73231e1014'
down_revision: Union[str, Sequence[str], None] = '4fc4b9674cb0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Data migration: Migrate existing deposit_amount_paid to deposits table
    # This query joins leases with lease_driver and drivers to get the TLC license
    # Only migrates where deposit_amount_paid IS NOT NULL

    op.execute("""
        INSERT INTO deposits (
            deposit_id,
            lease_id,
            driver_tlc_license,
            vehicle_vin,
            vehicle_plate,
            required_amount,
            collected_amount,
            outstanding_amount,
            collection_method,
            deposit_status,
            lease_start_date,
            notes,
            created_by,
            created_on
        )
        SELECT
            CONCAT('DEP-', l.id, '-01') as deposit_id,
            l.id as lease_id,
            dtl.tlc_license_number as driver_tlc_license,
            v.vin as vehicle_vin,
            NULL as vehicle_plate,
            l.deposit_amount_paid as required_amount,
            l.deposit_amount_paid as collected_amount,
            0.00 as outstanding_amount,
            'CASH' as collection_method,
            CASE
                WHEN l.deposit_amount_paid > 0 THEN 'PAID'
                ELSE 'PENDING'
            END as deposit_status,
            l.lease_start_date,
            'Migrated from legacy deposit_amount_paid field' as notes,
            l.created_by,
            NOW() as created_on
        FROM leases l
        LEFT JOIN lease_drivers ld ON ld.lease_id = l.id AND ld.is_active = 1
        LEFT JOIN drivers d ON d.id = ld.driver_id
        LEFT JOIN driver_tlc_license dtl ON d.tlc_license_number_id = dtl.id
        LEFT JOIN vehicles v ON v.id = l.vehicle_id
        WHERE l.deposit_amount_paid IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM deposits d2 WHERE d2.lease_id = l.id
        )
    """)


def downgrade() -> None:
    """Downgrade schema."""
    # Reverse data migration: Copy deposit data back to deposit_amount_paid
    # This updates the leases table with the collected_amount from deposits

    op.execute("""
        UPDATE leases l
        INNER JOIN deposits d ON d.lease_id = l.id
        SET l.deposit_amount_paid = d.collected_amount
        WHERE d.deposit_status IN ('PAID', 'HELD')
    """)

    # Delete all deposit records (they will be recreated on next upgrade)
    op.execute("DELETE FROM deposits")
