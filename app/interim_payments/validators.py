# app/interim_payments/validators.py (NEW FILE)

"""
Validation rules for interim payments as per specification Section 8.
"""

from decimal import Decimal
from typing import List, Dict, Any

from sqlalchemy.orm import Session

from app.interim_payments.exceptions import InvalidAllocationError
from app.ledger.models import LedgerBalance, BalanceStatus, PostingCategory
from app.utils.logger import get_logger

logger = get_logger(__name__)


class InterimPaymentValidator:
    """
    Centralized validation for interim payment operations.
    Implements all rules from documentation Section 8.
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    def validate_payment_creation(
        self,
        driver_id: int,
        lease_id: int,
        total_amount: Decimal,
        payment_method: str,
        allocations: List[Dict[str, Any]]
    ) -> None:
        """
        Validates all rules before creating an interim payment.
        
        Raises:
            InvalidAllocationError: If any validation fails
        """
        
        # 8.1 General Rules
        self._validate_general_rules(driver_id, lease_id, total_amount, payment_method)
        
        # 8.2 Obligation Selection Rules
        self._validate_obligation_selection(allocations, driver_id, lease_id)
        
        # Validate total allocation
        self._validate_allocation_total(total_amount, allocations)
    
    def _validate_general_rules(
        self,
        driver_id: int,
        lease_id: int,
        total_amount: Decimal,
        payment_method: str
    ) -> None:
        """8.1 General Rules validation"""
        
        # Mandatory fields
        if not driver_id:
            raise InvalidAllocationError("Driver ID is required")
        if not lease_id:
            raise InvalidAllocationError("Lease ID is required")
        if not payment_method:
            raise InvalidAllocationError("Payment method is required")
        
        # Positive amounts only
        if total_amount <= 0:
            raise InvalidAllocationError("Payment amount must be greater than zero")
        
        # Category restriction - no TAXES allowed
        # This is validated in obligation selection
    
    def _validate_obligation_selection(
        self,
        allocations: List[Dict[str, Any]],
        driver_id: int,
        lease_id: int
    ) -> None:
        """8.2 Obligation Selection Rules validation"""
        
        if not allocations or len(allocations) == 0:
            raise InvalidAllocationError("At least one allocation is required")
        
        for idx, alloc in enumerate(allocations):
            balance_id = alloc.get("balance_id")
            category = alloc.get("category", "").upper()
            amount = Decimal(str(alloc.get("amount", 0)))
            
            # Validate balance_id exists
            if not balance_id:
                raise InvalidAllocationError(
                    f"Allocation #{idx+1}: balance_id is required"
                )
            
            # Fetch the ledger balance
            balance = self.db.query(LedgerBalance).filter(
                LedgerBalance.id == balance_id
            ).first()
            
            if not balance:
                raise InvalidAllocationError(
                    f"Allocation #{idx+1}: Ledger balance with ID {balance_id} not found"
                )
            
            # FIXED: Validate balance is OPEN (not closed)
            if balance.status == BalanceStatus.CLOSED:
                raise InvalidAllocationError(
                    f"Allocation #{idx+1}: Cannot apply payment to closed obligation "
                    f"(Reference: {balance.reference_id}). This balance has already been fully paid."
                )
            
            # Validate balance belongs to correct driver and lease
            if balance.driver_id != driver_id:
                raise InvalidAllocationError(
                    f"Allocation #{idx+1}: Balance {balance_id} does not belong to driver {driver_id}"
                )
            
            if balance.lease_id != lease_id:
                raise InvalidAllocationError(
                    f"Allocation #{idx+1}: Balance {balance_id} does not belong to lease {lease_id}"
                )
            
            # Validate category matches
            if category != balance.category.value.upper():
                raise InvalidAllocationError(
                    f"Allocation #{idx+1}: Category mismatch. "
                    f"Allocation says '{category}' but balance is '{balance.category.value}'"
                )
            
            # Validate amount is positive
            if amount <= 0:
                raise InvalidAllocationError(
                    f"Allocation #{idx+1}: Amount must be greater than zero"
                )
            
            # Validate category restriction - no TAXES
            restricted_categories = [PostingCategory.TAXES]
            if balance.category in restricted_categories:
                raise InvalidAllocationError(
                    f"Allocation #{idx+1}: Interim Payments cannot be applied to "
                    f"statutory {balance.category.value} (MTA, TIF, Congestion, CBDT, Airport)"
                )
            
            # Partial allocations allowed - no need to check if amount < balance
            # Exact allocations will close the obligation
            # Excess will be handled separately
    
    def _validate_allocation_total(
        self,
        total_amount: Decimal,
        allocations: List[Dict[str, Any]]
    ) -> None:
        """8.1 General Rules: No over-allocation"""
        
        total_allocated = sum(
            Decimal(str(alloc.get("amount", 0))) 
            for alloc in allocations
        )
        
        if total_allocated > total_amount:
            raise InvalidAllocationError(
                f"Total allocated amount (${total_allocated:.2f}) cannot exceed "
                f"payment amount (${total_amount:.2f})"
            )
        
        logger.info(
            f"Allocation validation passed: ${total_allocated:.2f} of ${total_amount:.2f} allocated",
            excess=float(total_amount - total_allocated)
        )
    
    def validate_void_operation(
        self,
        payment_id: str,
        reason: str
    ) -> None:
        """
        Validate void operation requirements.
        """
        from app.interim_payments.models import InterimPayment, PaymentStatus
        
        # Validate reason length
        if not reason or len(reason.strip()) < 10:
            raise InvalidAllocationError(
                "Void reason must be at least 10 characters"
            )
        
        # Check payment exists and status
        payment = self.db.query(InterimPayment).filter(
            InterimPayment.payment_id == payment_id
        ).first()
        
        if not payment:
            raise InvalidAllocationError(f"Payment {payment_id} not found")
        
        if payment.status == PaymentStatus.VOIDED:
            raise InvalidAllocationError(
                f"Payment {payment_id} is already voided on "
                f"{payment.voided_at.strftime('%Y-%m-%d %H:%M:%S')}"
            )