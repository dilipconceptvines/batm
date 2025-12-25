"""curb modules revision

Revision ID: 6da7778a4874
Revises: ee935892a1e9
Create Date: 2025-12-24 10:52:38.748366

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = '6da7778a4874'
down_revision: Union[str, Sequence[str], None] = 'ee935892a1e9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply the migration"""
    
    # Get database connection
    conn = op.get_bind()
    
    # =========================================================================
    # STEP 1: Backup existing curb_trips table (if it exists)
    # =========================================================================
    
    # Check if curb_trips table exists
    inspector = sa.inspect(conn)
    existing_tables = inspector.get_table_names()
    
    if 'curb_trips' in existing_tables:
        print("Found existing curb_trips table, creating backup...")
        
        # Create backup table with current data
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS curb_trips_backup_pre_rewrite
            AS SELECT * FROM curb_trips
        """))
        
        # Log backup info
        backup_count = conn.execute(text(
            "SELECT COUNT(*) FROM curb_trips_backup_pre_rewrite"
        )).scalar()
        print(f"Backed up {backup_count} existing trip records")
        
        # Drop all foreign key constraints first
        print("Dropping foreign key constraints...")
        fk_constraints = inspector.get_foreign_keys('curb_trips')
        for fk in fk_constraints:
            try:
                op.drop_constraint(fk['name'], 'curb_trips', type_='foreignkey')
            except:
                pass  # Constraint might not exist
        
        # Drop all indexes
        print("Dropping indexes...")
        indexes = inspector.get_indexes('curb_trips')
        for idx in indexes:
            try:
                op.drop_index(idx['name'], table_name='curb_trips')
            except:
                pass  # Index might not exist
        
        # Drop the table completely (this removes enum definitions)
        print("Dropping curb_trips table...")
        op.drop_table('curb_trips')
        print("Old curb_trips table dropped successfully")
    else:
        print("No existing curb_trips table found, proceeding with fresh creation")
    
    # =========================================================================
    # STEP 2: Create curb_accounts table
    # =========================================================================
    
    print("Creating curb_accounts table...")
    op.create_table(
        'curb_accounts',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('account_name', sa.String(100), nullable=False),
        sa.Column('merchant_id', sa.String(50), nullable=False),
        sa.Column('username', sa.String(100), nullable=False),
        sa.Column('password', sa.String(255), nullable=False),
        sa.Column('api_url', sa.String(255), nullable=False, 
                  server_default='https://api.taxitronic.org/vts_service/taxi_service.asmx'),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='1'),
        sa.Column('reconciliation_mode', 
                  sa.Enum('server', 'local', name='reconciliation_mode_enum'), 
                  nullable=False, server_default='local'),
        sa.Column('is_archived', sa.Boolean(), nullable=True, server_default='0'),
        sa.Column('created_by', sa.Integer(), nullable=True),
        sa.Column('modified_by', sa.Integer(), nullable=True),
        sa.Column('created_on', sa.DateTime(timezone=True), nullable=True, 
                  server_default=sa.text('NOW()')),
        sa.Column('updated_on', sa.DateTime(timezone=True), nullable=True, 
                  onupdate=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['created_by'], ['users.id'], 
                               onupdate='CASCADE', ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['modified_by'], ['users.id'], 
                               onupdate='CASCADE', ondelete='SET NULL'),
        mysql_charset='utf8mb4',
        mysql_collate='utf8mb4_unicode_ci'
    )
    
    # Create indexes for curb_accounts
    op.create_index('idx_account_name', 'curb_accounts', ['account_name'], unique=True)
    op.create_index('idx_account_active', 'curb_accounts', ['is_active'])
    
    print("curb_accounts table created successfully")
    
    # =========================================================================
    # STEP 3: Create new curb_trips table with simplified schema
    # =========================================================================
    
    print("Creating new curb_trips table...")
    op.create_table(
        'curb_trips',
        # Primary key
        sa.Column('id', sa.Integer(), nullable=False),
        
        # Source identification
        sa.Column('curb_trip_id', sa.String(255), nullable=False, 
                  comment='Unique identifier from CURB API (format: {PERIOD}-{ID})'),
        sa.Column('account_id', sa.Integer(), nullable=False, 
                  comment='Which CURB account this trip came from'),
        
        # Processing status (NEW SIMPLIFIED ENUM)
        sa.Column('status', 
                  sa.Enum('IMPORTED', 'POSTED_TO_LEDGER', name='curb_trip_status_enum'), 
                  nullable=False, server_default='IMPORTED',
                  comment='Current processing status'),
        
        # Entity associations (nullable until mapped)
        sa.Column('driver_id', sa.Integer(), nullable=True, 
                  comment='Internal driver ID (mapped from curb_driver_id)'),
        sa.Column('lease_id', sa.Integer(), nullable=True, 
                  comment='Active lease during this trip'),
        sa.Column('vehicle_id', sa.Integer(), nullable=True, 
                  comment='Vehicle used for this trip'),
        sa.Column('medallion_id', sa.Integer(), nullable=True, 
                  comment='Medallion number'),
        
        # Raw CURB identifiers
        sa.Column('curb_driver_id', sa.String(100), nullable=False, 
                  comment='Driver ID from CURB system'),
        sa.Column('curb_cab_number', sa.String(100), nullable=False, 
                  comment='Cab/Medallion number from CURB'),
        
        # Trip timestamps
        sa.Column('start_time', sa.DateTime(timezone=True), nullable=False, 
                  comment='Trip start datetime (used for 3-hour windowing)'),
        sa.Column('end_time', sa.DateTime(timezone=True), nullable=False, 
                  comment='Trip end datetime'),
        
        # Financial data (CASH trips only)
        sa.Column('fare', sa.Numeric(10, 2), nullable=False, server_default='0.00', 
                  comment='Base fare amount'),
        sa.Column('tips', sa.Numeric(10, 2), nullable=False, server_default='0.00', 
                  comment='Tip amount'),
        sa.Column('tolls', sa.Numeric(10, 2), nullable=False, server_default='0.00', 
                  comment='Toll charges'),
        sa.Column('extras', sa.Numeric(10, 2), nullable=False, server_default='0.00', 
                  comment='Extra charges'),
        sa.Column('total_amount', sa.Numeric(10, 2), nullable=False, server_default='0.00', 
                  comment='Total trip amount'),
        
        # Tax & fee breakdown
        sa.Column('surcharge', sa.Numeric(10, 2), nullable=False, server_default='0.00', 
                  comment='State Surcharge (MTA Tax)'),
        sa.Column('improvement_surcharge', sa.Numeric(10, 2), nullable=False, server_default='0.00', 
                  comment='Improvement Surcharge (TIF)'),
        sa.Column('congestion_fee', sa.Numeric(10, 2), nullable=False, server_default='0.00', 
                  comment='Congestion Fee'),
        sa.Column('airport_fee', sa.Numeric(10, 2), nullable=False, server_default='0.00', 
                  comment='Airport Fee'),
        sa.Column('cbdt_fee', sa.Numeric(10, 2), nullable=False, server_default='0.00', 
                  comment='Congestion Relief Zone Toll (CBDT)'),
        
        # Payment info (NEW SIMPLIFIED ENUM)
        sa.Column('payment_type', 
                  sa.Enum('CASH', 'CREDIT_CARD', 'UNKNOWN', name='payment_type_enum'), 
                  nullable=False, server_default='CASH',
                  comment='Payment method (we only import CASH)'),
        
        # Reconciliation tracking
        sa.Column('reconciliation_id', sa.String(100), nullable=True, 
                  comment='Reconciliation batch ID sent to CURB API'),
        sa.Column('reconciled_at', sa.DateTime(timezone=True), nullable=True, 
                  comment='When this trip was marked as reconciled'),
        
        # Ledger integration (NEW FIELDS)
        sa.Column('ledger_posting_ref', sa.String(255), nullable=True, 
                  comment='Reference ID of the ledger posting created for this trip'),
        sa.Column('posted_to_ledger_at', sa.DateTime(timezone=True), nullable=True, 
                  comment='When this trip was posted to the ledger'),

        # Curb trip coordinates
        sa.Column('start_long', sa.Numeric(10, 7), nullable=True, 
                  comment='Starting longitude of the trip.'),
        sa.Column('start_lat', sa.Numeric(10, 7), nullable=True, 
                  comment='Starting latitude of the trip.'),
        sa.Column('end_long', sa.Numeric(10, 7), nullable=True, 
                  comment='Ending longitude of the trip.'),
        sa.Column('end_lat', sa.Numeric(10, 7), nullable=True, 
                  comment='Ending latitude of the trip.'),

        # Transaction date
        sa.Column('transaction_date', sa.DateTime(timezone=True), nullable=True, 
                  comment='Date of the transaction as per CURB records.'),

        # Number of services
        sa.Column('num_service', sa.Integer(), nullable=True,
                    comment='Number of services rendered during the trip.'),
        
        # Additional trip data
        sa.Column('distance_miles', sa.Numeric(10, 2), nullable=True, 
                  comment='Trip distance in miles'),
        sa.Column('num_passengers', sa.Integer(), nullable=True, 
                  comment='Number of passengers'),
        
        # Audit fields
        sa.Column('is_archived', sa.Boolean(), nullable=True, server_default='0', 
                  comment='Flag indicating if the record is archived'),
        sa.Column('is_active', sa.Boolean(), nullable=True, server_default='1', 
                  comment='Flag to keep track of record is active or not'),
        sa.Column('created_by', sa.Integer(), nullable=True, 
                  comment='User who created this record'),
        sa.Column('modified_by', sa.Integer(), nullable=True, 
                  comment='User who last modified this record'),
        sa.Column('created_on', sa.DateTime(timezone=True), nullable=True, 
                  server_default=sa.text('NOW()'), 
                  comment='Timestamp when this record was created'),
        sa.Column('updated_on', sa.DateTime(timezone=True), nullable=True, 
                  onupdate=sa.text('NOW()'), 
                  comment='Timestamp when this record was last updated'),
        
        # Constraints
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('curb_trip_id', name='uq_curb_trip_id'),
        sa.ForeignKeyConstraint(['account_id'], ['curb_accounts.id'], 
                               onupdate='CASCADE', ondelete='RESTRICT'),
        sa.ForeignKeyConstraint(['driver_id'], ['drivers.id'], 
                               onupdate='CASCADE', ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['lease_id'], ['leases.id'], 
                               onupdate='CASCADE', ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['vehicle_id'], ['vehicles.id'], 
                               onupdate='CASCADE', ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['medallion_id'], ['medallions.id'], 
                               onupdate='CASCADE', ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['created_by'], ['users.id'], 
                               onupdate='CASCADE', ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['modified_by'], ['users.id'], 
                               onupdate='CASCADE', ondelete='SET NULL'),
        
        mysql_charset='utf8mb4',
        mysql_collate='utf8mb4_unicode_ci'
    )
    
    print("curb_trips table created successfully")
    
    # =========================================================================
    # STEP 4: Create strategic indexes (CRITICAL for performance)
    # =========================================================================
    
    print("Creating strategic indexes...")
    
    # Basic single-column indexes
    op.create_index('idx_curb_trips_id', 'curb_trips', ['id'])
    op.create_index('idx_curb_trips_curb_trip_id', 'curb_trips', ['curb_trip_id'], unique=True)
    op.create_index('idx_curb_trips_account_id', 'curb_trips', ['account_id'])
    op.create_index('idx_curb_trips_status', 'curb_trips', ['status'])
    op.create_index('idx_curb_trips_driver_id', 'curb_trips', ['driver_id'])
    op.create_index('idx_curb_trips_lease_id', 'curb_trips', ['lease_id'])
    op.create_index('idx_curb_trips_vehicle_id', 'curb_trips', ['vehicle_id'])
    op.create_index('idx_curb_trips_medallion_id', 'curb_trips', ['medallion_id'])
    op.create_index('idx_curb_trips_payment_type', 'curb_trips', ['payment_type'])
    op.create_index('idx_curb_trips_reconciliation_id', 'curb_trips', ['reconciliation_id'])
    op.create_index('idx_curb_trips_ledger_ref', 'curb_trips', ['ledger_posting_ref'])
    op.create_index('idx_curb_trips_curb_cab', 'curb_trips', ['curb_cab_number'])
    op.create_index('idx_curb_trips_curb_driver', 'curb_trips', ['curb_driver_id'])
    
    # Strategic composite indexes for optimized queries
    op.create_index('idx_curb_trip_time_status', 'curb_trips', ['start_time', 'status'])
    op.create_index('idx_curb_payment_status', 'curb_trips', ['payment_type', 'status'])
    op.create_index('idx_curb_driver_time', 'curb_trips', ['driver_id', 'start_time'])
    op.create_index('idx_curb_lease_time', 'curb_trips', ['lease_id', 'start_time'])
    op.create_index('idx_curb_account_time', 'curb_trips', ['account_id', 'start_time'])
    op.create_index('idx_curb_ready_for_ledger', 'curb_trips', ['status', 'driver_id', 'start_time'])
    
    print("All indexes created successfully")
    
    # =========================================================================
    # STEP 6: Print migration summary
    # =========================================================================
    
    print("\n" + "="*80)
    print("CURB MODULE REWRITE MIGRATION COMPLETED SUCCESSFULLY")
    print("="*80)


def downgrade() -> None:
    """Revert the migration"""
    
    print("Reverting CURB module rewrite...")
    
    # Drop new tables
    op.drop_table('curb_trips')
    op.drop_table('curb_accounts')
    
    # Drop new enum types
    op.execute("DROP TYPE IF EXISTS curb_trip_status_enum")
    op.execute("DROP TYPE IF EXISTS payment_type_enum")
    op.execute("DROP TYPE IF EXISTS reconciliation_mode_enum")
    
    # Optionally restore from backup
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing_tables = inspector.get_table_names()
    
    if 'curb_trips_backup_pre_rewrite' in existing_tables:
        print("Restoring from backup...")
        conn.execute(text("""
            CREATE TABLE curb_trips
            AS SELECT * FROM curb_trips_backup_pre_rewrite
        """))
        print("Old curb_trips table restored from backup")
    
    print("Downgrade completed")
