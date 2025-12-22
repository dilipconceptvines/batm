# app/leases/lease_schedule_service.py

"""
Lease Schedule Service

Handles lease fee schedule posting to centralized ledger
Implements weekly automation and manual posting capabilities
"""

from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from app.leases.models import Lease, LeaseSchedule
from app.leases.schemas import LeaseStatus
from app.ledger.services import LedgerService
from app.ledger.repository import LedgerRepository
from app.ledger.models import PostingCategory
from app.utils.logger import get_logger

logger = get_logger(__name__)


class LeaseScheduleService:
    """
    Service for posting lease fees to ledger
    Handles weekly automation and manual posting
    """
    
    def __init__(self, db: Session):
        self.db = db
        # Create repository instance and pass it to service
        ledger_repo = LedgerRepository(db)
        self.ledger_service = LedgerService(ledger_repo)
    
    def post_weekly_lease_fees(
        self,
        target_date: Optional[date] = None,
        lease_id: Optional[int] = None
    ) -> Dict:
        """
        Post lease fees for the upcoming week to ledger
        
        This method finds all lease schedule entries that are due and haven't
        been posted to the ledger yet, then creates ledger postings for them.
        
        Args:
            target_date: Date to post fees for (defaults to today)
            lease_id: Optional specific lease to post (defaults to all active leases)
            
        Returns:
            Dictionary containing:
                - success_count: Number of successfully posted fees
                - failure_count: Number of failed postings
                - posted_schedules: List of posted schedule IDs
                - failed_schedules: List of failed schedule details
                - total_amount_posted: Sum of all posted amounts
                
        Process:
            1. Find all unposted schedule entries due this week
            2. For each entry:
                - Validate lease is active
                - Get driver information from lease
                - Create DEBIT posting in ledger
                - Mark schedule entry as posted
            3. Return summary of results
        """
        try:
            if target_date is None:
                target_date = date.today()
            
            logger.info(f"Starting weekly lease fee posting for date: {target_date}")
            
            # Build query for unposted lease schedules
            query = self.db.query(LeaseSchedule).filter(
                LeaseSchedule.is_active == True,
                LeaseSchedule.posted_to_ledger == 0,
                LeaseSchedule.installment_due_date <= target_date
            )
            
            # Filter by specific lease if provided
            if lease_id:
                query = query.filter(LeaseSchedule.lease_id == lease_id)
            
            # Join with Lease to ensure lease is active
            query = query.join(
                Lease,
                and_(
                    LeaseSchedule.lease_id == Lease.id,
                    Lease.lease_status.in_([
                        LeaseStatus.ACTIVE,
                        LeaseStatus.IN_PROGRESS
                    ])
                )
            )
            
            unposted_schedules = query.all()
            
            logger.info(f"Found {len(unposted_schedules)} unposted lease schedule entries")
            
            if not unposted_schedules:
                return {
                    'success_count': 0,
                    'failure_count': 0,
                    'posted_schedules': [],
                    'failed_schedules': [],
                    'total_amount_posted': Decimal('0.00')
                }
            
            success_count = 0
            failure_count = 0
            posted_schedules = []
            failed_schedules = []
            total_amount_posted = Decimal('0.00')
            
            # Process each schedule entry
            for schedule in unposted_schedules:
                try:
                    # Get lease details
                    lease = self.db.query(Lease).filter(
                        Lease.id == schedule.lease_id
                    ).first()
                    
                    if not lease:
                        logger.warning(f"Lease {schedule.lease_id} not found for schedule {schedule.id}")
                        failed_schedules.append({
                            'schedule_id': schedule.id,
                            'lease_id': schedule.lease_id,
                            'error': 'Lease not found'
                        })
                        failure_count += 1
                        continue
                    
                    # Get driver from lease_drivers
                    from app.leases.models import LeaseDriver
                    driver_lease = self.db.query(LeaseDriver).filter(
                        LeaseDriver.lease_id == lease.id,
                        LeaseDriver.is_active == True,
                        or_(
                            LeaseDriver.driver_role.in_(['DL', 'NL']),
                            LeaseDriver.is_additional_driver == False
                        )
                    ).first()
                    
                    if not driver_lease:
                        logger.warning(f"No active driver found for lease {lease.id}")
                        failed_schedules.append({
                            'schedule_id': schedule.id,
                            'lease_id': lease.id,
                            'error': 'No active driver found'
                        })
                        failure_count += 1
                        continue
                    
                    # Get driver ID
                    from app.drivers.models import Driver
                    driver = self.db.query(Driver).filter(
                        Driver.driver_id == driver_lease.driver_id
                    ).first()
                    
                    if not driver:
                        logger.warning(f"Driver {driver_lease.driver_id} not found")
                        failed_schedules.append({
                            'schedule_id': schedule.id,
                            'lease_id': lease.id,
                            'error': f'Driver {driver_lease.driver_id} not found'
                        })
                        failure_count += 1
                        continue
                    
                    # Ensure amount is Decimal
                    amount = Decimal(str(schedule.installment_amount))
                    
                    # Calculate payment period - ensure datetime objects
                    period_start_date = schedule.period_start_date
                    period_end_date = schedule.period_end_date
                    
                    # Convert date to datetime if needed
                    if isinstance(period_start_date, date) and not isinstance(period_start_date, datetime):
                        period_start = datetime.combine(period_start_date, datetime.min.time())
                    else:
                        period_start = period_start_date
                    
                    if isinstance(period_end_date, date) and not isinstance(period_end_date, datetime):
                        # End of day for period_end
                        period_end = datetime.combine(period_end_date, datetime.max.time().replace(microsecond=0))
                    else:
                        period_end = period_end_date
                    
                    # Ensure timezone aware
                    if period_start.tzinfo is None:
                        period_start = period_start.replace(tzinfo=timezone.utc)
                    if period_end.tzinfo is None:
                        period_end = period_end.replace(tzinfo=timezone.utc)
                    
                    # Due date should be datetime object for ledger service
                    due_date_value = schedule.installment_due_date
                    if isinstance(due_date_value, date) and not isinstance(due_date_value, datetime):
                        due_date_value = datetime.combine(due_date_value, datetime.max.time().replace(microsecond=0))
                    
                    # Ensure timezone aware
                    if due_date_value.tzinfo is None:
                        due_date_value = due_date_value.replace(tzinfo=timezone.utc)
                    
                    # Create posting description
                    if schedule.installment_number == 1:
                        description = f"Lease fee - Installment {schedule.installment_number} (Prorated)"
                    else:
                        description = f"Lease fee - Installment {schedule.installment_number}"
                    
                    # Post to ledger using create_obligation (correct function name)
                    posting, balance = self.ledger_service.create_obligation(
                        driver_id=int(driver.id),
                        lease_id=int(lease.id),
                        category=PostingCategory.LEASE,
                        amount=amount,
                        reference_id=str(schedule.id),
                    )
                    
                    # Mark schedule as posted
                    schedule.posted_to_ledger = 1
                    schedule.posted_on = datetime.now(timezone.utc)
                    schedule.ledger_posting_id = posting.id
                    schedule.ledger_balance_id = balance.id
                    
                    self.db.flush()
                    
                    posted_schedules.append(schedule.id)
                    total_amount_posted += amount
                    success_count += 1
                    
                    logger.info(
                        f"Posted lease fee: Schedule {schedule.id}, "
                        f"Lease {lease.id}, Amount ${amount}"
                    )
                    
                except Exception as e:
                    logger.error(
                        f"Failed to post schedule {schedule.id}: {str(e)}",
                        exc_info=True
                    )
                    failed_schedules.append({
                        'schedule_id': schedule.id,
                        'lease_id': schedule.lease_id if schedule.lease_id else None,
                        'error': str(e)
                    })
                    failure_count += 1
                    continue
            
            # Commit all successful postings
            self.db.commit()
            
            logger.info(
                f"Lease fee posting completed: "
                f"{success_count} posted, {failure_count} failed, "
                f"Total amount: ${total_amount_posted}"
            )
            
            return {
                'success_count': success_count,
                'failure_count': failure_count,
                'posted_schedules': posted_schedules,
                'failed_schedules': failed_schedules,
                'total_amount_posted': float(total_amount_posted)
            }
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Lease fee posting failed: {str(e)}", exc_info=True)
            raise
    
    def get_unposted_schedule_entries(
        self,
        lease_id: Optional[int] = None,
        driver_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[LeaseSchedule]:
        """
        Get all unposted lease schedule entries
        
        Args:
            lease_id: Filter by specific lease
            driver_id: Filter by specific driver
            start_date: Filter by due date >= start_date
            end_date: Filter by due date <= end_date
            
        Returns:
            List of unposted LeaseSchedule entries
        """
        try:
            query = self.db.query(LeaseSchedule).filter(
                LeaseSchedule.is_active == True,
                LeaseSchedule.posted_to_ledger == 0
            )
            
            if lease_id:
                query = query.filter(LeaseSchedule.lease_id == lease_id)
            
            if start_date:
                query = query.filter(LeaseSchedule.installment_due_date >= start_date)
            
            if end_date:
                query = query.filter(LeaseSchedule.installment_due_date <= end_date)
            
            if driver_id:
                # Join with lease and lease_drivers
                from app.leases.models import LeaseDriver
                query = query.join(
                    Lease,
                    LeaseSchedule.lease_id == Lease.id
                ).join(
                    LeaseDriver,
                    and_(
                        LeaseDriver.lease_id == Lease.id,
                        LeaseDriver.driver_id == driver_id,
                        LeaseDriver.is_active == True
                    )
                )
            
            query = query.order_by(LeaseSchedule.installment_due_date.asc())
            
            return query.all()
            
        except Exception as e:
            logger.error(f"Error getting unposted schedules: {str(e)}", exc_info=True)
            raise
    
    def post_single_schedule_entry(
        self,
        schedule_id: int
    ) -> Dict:
        """
        Post a single lease schedule entry to ledger
        
        Args:
            schedule_id: ID of the lease schedule entry
            
        Returns:
            Dictionary with posting result
        """
        try:
            schedule = self.db.query(LeaseSchedule).filter(
                LeaseSchedule.id == schedule_id
            ).first()
            
            if not schedule:
                raise ValueError(f"Schedule entry {schedule_id} not found")
            
            if schedule.posted_to_ledger == 1:
                raise ValueError(f"Schedule entry {schedule_id} already posted")
            
            # Post just this schedule by setting target date far in future
            # and filtering by the specific lease
            result = self.post_weekly_lease_fees(
                target_date=date.today() + timedelta(days=365),
                lease_id=schedule.lease_id
            )
            
            # Check if our specific schedule was posted
            if schedule_id in result['posted_schedules']:
                # Refresh the schedule to get updated values
                self.db.refresh(schedule)
                
                logger.info(f"Successfully posted schedule entry {schedule_id}")
                return {
                    'success': True,
                    'schedule_id': schedule_id,
                    'posting_id': schedule.ledger_posting_id,
                    'balance_id': schedule.ledger_balance_id
                }
            else:
                # Find the error for this schedule
                error_msg = 'Unknown error'
                for failed in result['failed_schedules']:
                    if failed['schedule_id'] == schedule_id:
                        error_msg = failed['error']
                        break
                raise ValueError(error_msg)
                
        except Exception as e:
            logger.error(f"Failed to post schedule entry {schedule_id}: {str(e)}", exc_info=True)
            raise
    
    def get_posting_statistics(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict:
        """
        Get statistics about lease fee postings
        
        Args:
            start_date: Start date for stats
            end_date: End date for stats
            
        Returns:
            Dictionary with statistics
        """
        try:
            from sqlalchemy import func
            
            query = self.db.query(LeaseSchedule).filter(
                LeaseSchedule.is_active == True
            )
            
            if start_date:
                query = query.filter(LeaseSchedule.installment_due_date >= start_date)
            
            if end_date:
                query = query.filter(LeaseSchedule.installment_due_date <= end_date)
            
            total_schedules = query.count()
            
            posted_schedules = query.filter(LeaseSchedule.posted_to_ledger == 1).count()
            
            unposted_schedules = query.filter(LeaseSchedule.posted_to_ledger == 0).count()
            
            total_amount = query.with_entities(
                func.sum(LeaseSchedule.installment_amount)
            ).scalar() or Decimal('0.00')
            
            posted_amount = query.filter(LeaseSchedule.posted_to_ledger == 1).with_entities(
                func.sum(LeaseSchedule.installment_amount)
            ).scalar() or Decimal('0.00')
            
            unposted_amount = query.filter(LeaseSchedule.posted_to_ledger == 0).with_entities(
                func.sum(LeaseSchedule.installment_amount)
            ).scalar() or Decimal('0.00')
            
            return {
                'total_schedules': total_schedules,
                'posted_schedules': posted_schedules,
                'unposted_schedules': unposted_schedules,
                'total_amount': float(total_amount),
                'posted_amount': float(posted_amount),
                'unposted_amount': float(unposted_amount),
                'posting_rate': round((posted_schedules / total_schedules * 100), 2) if total_schedules > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error getting posting statistics: {str(e)}", exc_info=True)
            raise


# Create singleton instance for import
def get_lease_schedule_service(db: Session) -> LeaseScheduleService:
    """Factory function to get LeaseScheduleService instance"""
    return LeaseScheduleService(db)