## app/bpm/services.py

# Standard Library Imports
import copy
from datetime import date, datetime, time, timedelta
from typing import List, Optional, Union

# Third party imports
from sqlalchemy import and_, asc, desc, func, select
from sqlalchemy.orm import Session, aliased

from app.bpm.exception import CaseStopException

# Local imports
from app.bpm.models import (
    SLA,
    Case,
    CaseEntity,
    CaseReassignment,
    CaseStatus,
    CaseStep,
    CaseStepConfig,
    CaseStepConfigPath,
    CaseType,
    CaseTypeFirstStep,
    case_step_config_role_table,
)
from app.users.models import Role, User
from app.utils.logger import get_logger

logger = get_logger(__name__)


class BPMService:
    def __init__(self):
        self.created_at = datetime.now()
        logger.info(f"[CREATED] {self} at {self.created_at}")

    def __del__(self):
        destroyed_at = datetime.now()
        logger.info(f"[DESTROYED] {self} at {destroyed_at}")

    """Service for BPM operations"""

    def has_required_role(self, user: User, step_config: CaseStepConfig) -> bool:
        """Check if the user has the required role for the step configuration"""
        try:
            # Get roles for the specified step configuration
            step_config_roles = step_config.roles

            user_roles = set(role.name for role in user.roles)
            # Check if the user has at least one required role
            required_roles = set(role.name for role in step_config_roles)
            # Returns True if there's an intersection
            return not required_roles.isdisjoint(user_roles)
        except Exception as e:
            logger.error("Error checking if user has required role: %s", e)
            raise e

    def get_cases(
        self,
        db: Session,
        case_no: Optional[str] = None,
        case_id: Optional[int] = None,
        case_type_name: Optional[str] = None,
        step_id: Optional[str] = None,
        case_step_config_id: Optional[int] = None,
        case_status: Optional[str] = None,
        sort_order: Optional[str] = "asc",
        multiple: bool = False,
        unique: bool = False,
    ) -> Union[Case, List[Case]]:
        """Get cases by case number or case id"""
        try:
            query = db.query(Case).join(CaseStatus)
            if case_no:
                query = query.filter(Case.case_no == case_no)
            if case_id:
                query = query.filter(Case.id == case_id)
            if case_type_name:
                query = query.join(CaseType).filter(CaseType.name == case_type_name)
            if case_step_config_id:
                query = query.join(
                    CaseStepConfig, Case.case_step_config_id == CaseStepConfig.id
                ).filter(CaseStepConfig.id == case_step_config_id)
            if step_id:
                query = query.join(
                    CaseStepConfig, Case.case_step_config_id == CaseStepConfig.id
                ).filter(CaseStepConfig.step_id == step_id)
            if case_status:
                case_statuses = case_status.split(",")
                status_ids = [
                    self.get_case_status(db, status).id for status in case_statuses
                ]
                query = query.filter(Case.case_status_id.in_(status_ids))

            query = query.order_by(
                desc(Case.created_on) if sort_order == "desc" else asc(Case.created_on)
            )

            if multiple:
                if unique:
                    return query.distinct().all()
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting cases: %s", e, exc_info=True)
            raise e

    def get_case_obj(self, db: Session, case_no: str):
        try:
            query = db.query(Case).filter(Case.case_no == case_no)
            case_obj = query.first()
            return case_obj
        except Exception as e:
            logger.error("Error getting case object: %s", e, exc_info=True)
            raise e

    def get_case_types(
        self, db: Session, prefix: Optional[str] = None, multiple: bool = False
    ) -> Union[CaseType, List[CaseType]]:
        """Get case type by prefix"""
        try:
            query = db.query(CaseType)
            if prefix:
                query = query.filter(CaseType.prefix == prefix)

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting case type: %s", e, exc_info=True)
            raise e

    def get_case_status(self, db: Session, status_name: str) -> CaseStatus:
        """Get case status by name"""
        try:
            return db.query(CaseStatus).filter(CaseStatus.name == status_name).first()
        except Exception as e:
            logger.error("Error getting case status: %s", e)
            raise e

    def get_first_step_configs(
        self, db: Session, case_type: Optional[CaseType] = None, multiple: bool = False
    ) -> Union[CaseStepConfig, List[CaseStepConfig]]:
        """Get the first step configuration for a case type"""
        try:
            # Get the first step configuration for the case type
            query = db.query(CaseTypeFirstStep)
            if case_type:
                query = query.filter(CaseTypeFirstStep.case_type_id == case_type.id)

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting first step configuration: %s", e)
            raise e

    def get_sla(
        self,
        db: Session,
        case_step_config: Optional[CaseStepConfig] = None,
        escalation_level: Optional[int] = None,
        multiple: bool = False,
    ) -> Union[SLA, List[SLA]]:
        """Get SLA by case step config and escalation level"""
        try:
            query = db.query(SLA)
            if case_step_config:
                query = query.filter(SLA.case_step_config_id == case_step_config.id)
            if escalation_level:
                query = query.filter(SLA.escalation_level == escalation_level)

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting SLA: %s", e)
            raise e

    def get_case_step_information(
        self, db: Session, case_type: str, logged_in_user: User, case
    ):
        """Get case step information"""
        try:
            first_step = (
                db.query(CaseTypeFirstStep)
                .join(CaseType, CaseTypeFirstStep.case_type_id == CaseType.id)
                .filter(CaseType.prefix == case_type)
                .first()
            )

            if not first_step:
                raise ValueError(
                    f"First step has not been configured for casetype {case_type}"
                )

            role_ids = set([str(role.id) for role in logged_in_user.roles])
            # case_step_configs = []

            case_step_configs = (
                db.query(
                    CaseStep.name.label("step_name"),  # CaseStep name
                    CaseStepConfig.step_name.label(
                        "config_step_name"
                    ),  # CaseStepConfig step_name
                    CaseStepConfig.step_id.label(
                        "config_step_id"
                    ),  # CaseStepConfig step_id
                    CaseStepConfig.next_step_id.label(
                        "next_step_id"
                    ),  # CaseStepConfig next_step_id
                    func.group_concat(Role.id).label("roles"),
                    CaseStep.weight.label("order"),  # Include the order column,
                    CaseStepConfig.current_assignee_id,
                )
                .join(CaseType, CaseStepConfig.case_type_id == CaseType.id)
                .join(CaseStep, CaseStep.id == CaseStepConfig.case_step_id)
                .outerjoin(
                    case_step_config_role_table,
                    CaseStepConfig.id
                    == case_step_config_role_table.c.case_step_config_id,
                )
                .outerjoin(Role, Role.id == case_step_config_role_table.c.role_id)
                .filter(CaseType.prefix == case_type)
                .order_by(asc(CaseStep.weight))
                .group_by(
                    CaseStep.name,
                    CaseStepConfig.step_name,
                    CaseStepConfig.step_id,
                    CaseStepConfig.next_step_id,
                    CaseStep.weight,  # Include the order column in the GROUP BY clause,
                    CaseStepConfig.current_assignee_id,
                )
                .all()
            )

            grouped_steps = {}
            configs_by_step_id = {
                config.config_step_id: config for config in case_step_configs
            }

            logger.info("configs_by_step_id: %s", configs_by_step_id)

            if not first_step.first_step:
                # If first step is not defined
                for current_config in case_step_configs:
                    logger.info("current_config: %s", current_config)

                    # Check if the current logged in user has the roles to access the
                    case_role_ids = set(
                        current_config.roles.split(",") if current_config.roles else []
                    )
                    if not role_ids.intersection(case_role_ids):
                        logger.info("Case role has no intersection")
                        # Move to the next step in the sequence
                        current_step_id = current_config.next_step_id
                        continue
                    #     # Group substeps by step_name
                    if current_config.step_name not in grouped_steps:
                        grouped_steps[current_config.step_name] = {
                            # Split roles string
                            "step_name": current_config.step_name,
                            "sub_steps": [],
                        }

                    grouped_steps[current_config.step_name]["sub_steps"].append(
                        {
                            "step_name": current_config.config_step_name,
                            "step_id": current_config.config_step_id,
                            "step_data": {},
                        }
                    )
            else:
                visited_steps = set()
                current_step_id = first_step.first_step.step_id
                while current_step_id:
                    if current_step_id in visited_steps:
                        raise ValueError(
                            "Circular reference detected in next_step_id sequence"
                        )
                    visited_steps.add(current_step_id)

                    current_config = configs_by_step_id.get(current_step_id)
                    if not current_config:
                        break

                    case_role_ids = set(
                        current_config.roles.split(",") if current_config.roles else []
                    )
                    has_access, current_assignee, assigned_roles = self.check_access(
                        db,
                        case,
                        current_step_id,
                        logged_in_user,
                        role_ids,
                        configs_by_step_id,
                    )

                    # Get original assignee information if reassignments exist
                    original_assignee_info = self.get_original_assignee_info(
                        db, case.case_no, current_step_id
                    )

                    if current_config.step_name not in grouped_steps:
                        grouped_steps[current_config.step_name] = {
                            "step_name": current_config.step_name,
                            "sub_steps": [],
                        }

                    grouped_steps[current_config.step_name]["sub_steps"].append(
                        {
                            "step_name": current_config.config_step_name,
                            "step_id": current_config.config_step_id,
                            "step_data": {},
                            "has_access": has_access,
                            "current_assignee_user": current_assignee,
                            "current_assignee_role": assigned_roles,
                            "original_assignee_user": original_assignee_info[
                                "original_user"
                            ],
                            "original_assignee_roles": original_assignee_info[
                                "original_roles"
                            ],
                            "has_reassignment": original_assignee_info[
                                "has_reassignment"
                            ],
                        }
                    )

                    current_step_id = current_config.next_step_id
                    if current_step_id:
                        has_access2, next_step_assignee, next_step_assigned_roles = (
                            self.check_access(
                                db,
                                case,
                                current_step_id,
                                logged_in_user,
                                role_ids,
                                configs_by_step_id,
                            )
                        )
                        grouped_steps[current_config.step_name]["sub_steps"][-1][
                            "next_assignee_user"
                        ] = next_step_assignee
                        grouped_steps[current_config.step_name]["sub_steps"][-1][
                            "next_assignee_role"
                        ] = next_step_assigned_roles

            return grouped_steps
        except Exception as e:
            logger.exception("Error getting case step information: %s", e)
            raise e

    def get_case_step_config(
        self,
        db: Session,
        step_id: Optional[str] = None,
        case_step_config_id: Optional[int] = None,
        multiple: bool = False,
    ) -> Union[CaseStepConfig, List[CaseStepConfig]]:
        """Get case step config by step id"""
        try:
            query = db.query(CaseStepConfig)
            if step_id:
                query = query.filter(CaseStepConfig.step_id == step_id)
            if case_step_config_id:
                query = query.filter(CaseStepConfig.id == case_step_config_id)

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting case step config: %s", e)
            raise e

    def get_case_step_config_path(
        self,
        db: Session,
        case_step_config_id: Optional[int] = None,
        multiple: bool = False,
    ) -> Union[CaseStepConfigPath, List[CaseStepConfigPath]]:
        """Get case step config path by case step config id"""
        try:
            query = db.query(CaseStepConfigPath)
            if case_step_config_id:
                query = query.filter(
                    CaseStepConfigPath.case_step_config_id == case_step_config_id
                )

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting case step config path: %s", e)
            raise e

    def get_case_info(
        self,
        db: Session,
        case_no: str,
        step_id: Optional[str] = None,
        sort_order: Optional[str] = "desc",
        multiple: bool = False,
    ) -> Union[Case, List[Case]]:
        """Get case info by case number"""
        try:
            query = (
                db.query(Case, CaseStepConfig)
                .join(Case, Case.case_type_id == CaseStepConfig.case_type_id)
                .filter(Case.case_no == case_no)
            )
            if step_id:
                query = query.filter(CaseStepConfig.step_id == step_id)

            query = query.order_by(
                desc(Case.created_on) if sort_order == "desc" else asc(Case.created_on)
            )
            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting case info: %s", e)
            raise e

    def get_cases_info(
        self,
        db: Session,
        closed_cases: List[str],
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ):
        """Get cases info"""
        try:
            case_query = (
                db.query(
                    Case.case_no,
                    User.first_name.label("current_step_assignee"),
                    User.id.label("current_user_id"),
                    Case.role_id,
                    CaseStepConfig.step_name,
                    Case.sla_id,
                    CaseStepConfig.id.label("case_step_id"),
                    Case.id,
                    CaseType.name.label("case_type"),
                    CaseStatus.name.label("case_status"),
                    func.max(Case.created_on).label("latest_created_on"),
                )
                .outerjoin(User, Case.user_id == User.id)
                .join(CaseType, CaseType.id == Case.case_type_id)
                .join(CaseStatus, CaseStatus.id == Case.case_status_id)
                .join(CaseStepConfig, Case.case_step_config_id == CaseStepConfig.id)
                .join(
                    case_step_config_role_table,
                    CaseStepConfig.id
                    == case_step_config_role_table.c.case_step_config_id,
                )
                .join(Role, case_step_config_role_table.c.role_id == Role.id)
                .filter(
                    CaseStatus.name.in_(
                        ["Open", "In Progress"]
                    ),  # Only show Open and In Progress cases
                    ~Case.case_no.in_(
                        closed_cases
                    ),  # Exclude cases that have any Closed status
                )
                .group_by(
                    Case.case_no,
                    User.first_name,
                    User.id,
                    Case.role_id,
                    CaseStepConfig.step_name,
                    Case.sla_id,
                    CaseStepConfig.id,
                    Case.id,
                    CaseType.name,
                    CaseStatus.name,
                )
            )

            if from_date and to_date:
                from_datetime = datetime.combine(from_date, time(0, 0, 0))
                to_datetime = datetime.combine(to_date, time(23, 59, 59))

                case_query = case_query.having(
                    and_(
                        func.max(Case.created_on) >= from_datetime,
                        func.max(Case.created_on) <= to_datetime,
                    )
                )

            case_query = case_query.order_by(desc(Case.created_on))

            return case_query.all()
        except Exception as e:
            logger.error("Error getting cases info: %s", e)
            raise e

    def get_case_details(self, db: Session, case: Case) -> dict:
        """Retrieve detailed information about a case, including case type, status, SLA, and user activity."""
        # Step 1: Get the first case entry for the given case_no
        first_case = self.get_cases(db, case_no=case.case_no)

        # Step 2: Get the previous case entry for the given case_no
        previous_case_rec = self.get_cases(db, case_no=case.case_no, sort_order="desc")
        previous_case_rec_created_by = (
            previous_case_rec.users if previous_case_rec else None
        )
        previous_case_rec_created_by_name = (
            previous_case_rec_created_by.name if previous_case_rec_created_by else ""
        )

        if not previous_case_rec_created_by_name:
            previous_case_rec_created_by_name = (
                previous_case_rec.role.name if previous_case_rec else "Unknown"
            )

        created_by = first_case.users
        created_by_name = created_by.name if created_by else "Unknown"
        associated_sla = self.get_sla(db, case_step_config=case.sla_id)
        target_date = None
        if associated_sla:
            target_date = case.latest_created_on + timedelta(
                minutes=associated_sla.time_limit
            )

        # Step 6: Compile the results
        case_details = {
            "case_id": case.id,
            "case_no": case.case_no,
            "case_status": case.case_status,
            "case_step": case.step_name,
            "case_step_id": case.case_step_id,
            "case_type": case.case_type,
            "status": case.case_status,
            "created_by": created_by_name,
            "current_step_assignee": case.current_step_assignee,
            "current_user_id": case.current_user_id,
            "created_on": first_case.created_on.strftime("%Y-%m-%d %H:%M:%S")
            if first_case.created_on
            else "-",
            "target_date": target_date.strftime("%Y-%m-%d %H:%M:%S")
            if target_date
            else "-",
            "updated_date": case.latest_created_on,
            "last_updated_by": previous_case_rec_created_by_name,
            "last_updated_on": previous_case_rec.created_on
            if previous_case_rec
            else "-",
        }

        return case_details

    def generate_case_number(self, db: Session, case_type_prefix: str) -> str:
        """Generate a unique case number with the given prefix."""
        try:
            last_case = (
                db.query(Case)
                .filter(Case.case_no.like(f"{case_type_prefix}%"))
                .order_by(desc(Case.case_no))
                .first()
            )

            if last_case:
                last_number = int(last_case.case_no[len(case_type_prefix) :])
                new_number = last_number + 1
            else:
                new_number = 1

            return f"{case_type_prefix}{str(new_number).zfill(6)}"  # e.g., 'ABC000001'
        except Exception as e:
            logger.error("Error generating case number: %s", e)
            raise e

    def create_case(self, db: Session, prefix: str, user: User) -> Case:
        """Create a new case"""
        try:
            # Get the case type
            case_type = self.get_case_types(db, prefix=prefix)

            # Get the first step configuration ID for case type
            case_step_config = self.get_first_step_configs(db, case_type=case_type)

            if not case_step_config.first_step:
                logger.info(
                    "There is no first step config, the case would be in the Open status without a first step"
                )
            else:
                # If a user is configured for the step then the user creating the case should match
                # Fetch the corresponding step config object
                step_config_object = case_step_config.first_step
                if (
                    step_config_object.next_assignee
                    and step_config_object.next_assignee != user
                ):
                    raise ValueError(
                        f"User '{user.first_name}' does not have the required permission for the initial step configuration."
                    )

                # Step 5: Check if the user has the required role for this step configuration
                if step_config_object.roles and not self.has_required_role(
                    user, step_config_object
                ):
                    raise ValueError(
                        f"User '{user.first_name}' does not have the required role for the initial step configuration."
                    )

            # Step 6: Get the 'Open' case status ID
            case_status_info = self.get_case_status(db, "Open")

            # Step 7: Fetch SLA for configured first step
            if case_step_config.first_step:
                sla = self.get_sla(db, case_step_config.first_step, 1)
            else:
                sla = None

            # Step 8: Create the Case object
            new_case = Case(
                case_no=self.generate_case_number(db, case_type.prefix),
                case_type_id=case_type.id,
                case_status_id=case_status_info.id,
                case_step_config_id=step_config_object.id
                if case_step_config.first_step
                else None,
                user_id=user.id,
                sla=sla,
                created_by=user.id,
            )

            # Add the new case to the session and commit
            db.add(new_case)
            db.commit()
            db.refresh(new_case)
            logger.info("Case created successfully: %s", new_case.id)
            return new_case
        except Exception as e:
            logger.error("Error creating case: %s", e)
            raise e

    def move_task_to_step(self, db: Session, case_no: str, step_id: str) -> dict:
        """Move the case to a specific step"""
        try:
            case = self.get_cases(db, case_no=case_no, sort_order="desc")
            if not case:
                raise ValueError("Case not found")

            # Get the step config for the case
            step_config = self.get_case_step_config(db, step_id=step_id)
            if not step_config:
                raise ValueError("Step config not found")

            # Check the permissions
            next_user_id = step_config.next_assignee_id

            next_role_id = None
            if step_config.roles:
                # Assuming we take the first role if multiple roles exist
                next_role_id = step_config.roles[0].id

            # Create a new case
            new_case = Case(
                case_no=case_no,
                case_type_id=case.case_type_id,
                # Assuming status remains the same unless specified
                case_status_id=case.case_status_id,
                case_step_config_id=step_config.id,
                user_id=next_user_id,
                role_id=next_role_id,
            )
            db.add(new_case)
            db.commit()
            db.refresh(new_case)
            logger.info(
                "Case '%s' has been moved to step '%s'.", case_no, step_config.id
            )
            return {
                "case": new_case,
                "user_id": next_user_id,
                "step_config": step_config,
            }
        except Exception as e:
            logger.error("Error moving case to step: %s", e)
            raise e

    def move_task_to_next_step(self, db: Session, case_no: str, user: User) -> dict:
        """Move the case to the next step"""
        try:
            case = self.get_cases(db, case_no=case_no, sort_order="desc")
            if not case:
                raise ValueError("Case not found")

            case_step_config = case.case_step_config
            if not case_step_config.next_step_id:
                raise CaseStopException(
                    "This case is already at its final step; no next step available."
                )

            # Step 2: Get the next step configuration based on the current configuration
            next_step_config = self.get_case_step_config(
                db, step_id=case_step_config.next_step_id
            )

            # Check the permissions
            next_user_id = next_step_config.next_assignee_id

            next_role_id = None
            if next_step_config.roles:
                # Assuming we take the first role if multiple roles exist
                next_role_id = next_step_config.roles[0].id

            # Step 4: If the current step is open, the case is being moved for the first time.
            #         Case status needs to be In Progress
            if case.case_status.name == "Open":
                in_progress_case_status = self.get_case_status(db, "In Progress")
            else:
                in_progress_case_status = case.case_status

            # Step 4: Create a new Case record for the next step
            new_case = Case(
                case_no=case_no,
                case_type_id=case.case_type_id,
                case_status_id=in_progress_case_status.id,
                case_step_config_id=next_step_config.id,
                user_id=next_user_id,
                role_id=next_role_id,
                created_by=user.id,
            )
            db.add(new_case)
            db.commit()
            db.refresh(new_case)
            logger.info("Case current state %s", new_case.case_status.name)
            return {
                "case": new_case,
                "user_id": next_user_id,
                "step_config": next_step_config,
            }
        except Exception as e:
            logger.error("Error moving case to next step: %s", e)
            raise e

    def mark_case_as_closed(self, db: Session, case_no: str):
        """Mark the case as closed"""
        try:
            logger.info("Closing the case here . . . . . .")
            # TODO: CaseStatus is hardcoded, this needs to move to a config

            # Step 1: Retrieve the CaseStatus object for "closed"
            closed_status = self.get_case_status(db, "Closed")
            if not closed_status:
                logger.error("No 'Closed' status found in CaseStatus.")
                return False

            # Step 2: Retrieve the existing case by case_no
            original_case = self.get_cases(db, case_no=case_no, sort_order="desc")

            if not original_case:
                logger.error("No case found with case number '%s'", case_no)
                return False

            # TODO: Remove case status hard coding
            # We also need to check if the case has already been closed
            if original_case.case_status.name == "Closed":
                logger.error("Case %s has already been closed", case_no)
                return False

            # Step 3: Create a new Case instance with the same data but different status
            # Create a shallow copy of the original case
            new_case = copy.copy(original_case)
            new_case = Case(
                case_no=case_no,
                case_type_id=original_case.case_type_id,
                case_status_id=closed_status.id,
                case_step_config_id=original_case.case_step_config_id,
                user_id=original_case.user_id,
                role_id=original_case.role_id,
            )

            # Step 4: Add the new case to the session and commit
            db.add(new_case)
            db.commit()
            db.refresh(new_case)
            logger.info("Case '%s' has been marked as closed.", case_no)
            return new_case
        except Exception as e:
            logger.error("Error marking case as closed: %s", e)
            raise e

    def create_case_entity(
        self,
        db: Session,
        case_no: str,
        entity_name: str,
        identifier: str,
        identifier_value: str,
    ) -> CaseEntity:
        """Create a new case entity"""
        try:
            case_entity = CaseEntity(
                case_no=case_no,
                entity_name=entity_name,
                identifier=identifier,
                identifier_value=identifier_value,
                is_active=True,
            )
            db.add(case_entity)
            db.flush()
            db.refresh(case_entity)
            return case_entity
        except Exception as e:
            logger.error("Error creating case entity: %s", str(e))
            raise e

    def fetch_latest_case_based_on_case_type(
        self,
        db: Session,
        entity_name: str,
        identifier_value: int,
        case_type_prefix: str,
    ):
        latest_case = (
            db.query(CaseEntity)
            .filter(
                CaseEntity.entity_name == entity_name,
                CaseEntity.identifier_value == identifier_value,
                CaseEntity.case_no.startswith(case_type_prefix),
            )
            .order_by(CaseEntity.created_on.desc())
            .first()
        )
        return latest_case

    def get_case_entity(
        self,
        db: Session,
        case_no: Optional[str] = None,
        entity_name: Optional[str] = None,
        identifier: Optional[str] = None,
        identifier_value: Optional[str] = None,
        multiple: bool = False,
    ) -> Union[CaseEntity, List[CaseEntity]]:
        """Get a case entity by case number, entity name, identifier, or identifier value"""
        try:
            query = db.query(CaseEntity)
            if case_no:
                query = query.filter(CaseEntity.case_no == case_no)
            if entity_name:
                query = query.filter(CaseEntity.entity_name == entity_name)
            if identifier:
                query = query.filter(CaseEntity.identifier == identifier)
            if identifier_value:
                query = query.filter(CaseEntity.identifier_value == identifier_value)

            if multiple:
                return query.all()
            return query.first()
        except Exception as e:
            logger.error("Error getting case entity: %s", str(e))
            raise e

    def check_access(
        self,
        db: Session,
        case,
        current_step_id,
        logged_in_user,
        role_ids,
        configs_by_step_id,
    ):
        """Checks if the logged-in user has access to the given step_id."""
        assigned_user = None
        assigned_roles = []

        # Fetch latest case_reassignment entry for this case and step_id
        latest_reassignment = (
            db.query(CaseReassignment)
            .filter(
                CaseReassignment.case_no == case.case_no,
                CaseReassignment.step_id == current_step_id,
            )
            # Get the latest reassignment
            .order_by(desc(CaseReassignment.created_on))
            .first()
        )

        if latest_reassignment:
            if latest_reassignment.user_id:
                assigned_user = (
                    db.query(User)
                    .filter(User.id == latest_reassignment.user_id)
                    .first()
                )

                # Fetch role details
                assigned_roles = (
                    db.query(Role).filter(Role.id == latest_reassignment.role_id).all()
                )
                assigned_roles = [
                    {"id": role.id, "name": role.name} for role in assigned_roles
                ]

                return (
                    latest_reassignment.user_id == logged_in_user.id,
                    {
                        "id": assigned_user.id if assigned_user else None,
                        "first_name": assigned_user.first_name
                        if assigned_user
                        else None,
                        "middle_name": assigned_user.middle_name
                        if assigned_user
                        else None,
                        "last_name": assigned_user.last_name if assigned_user else None,
                    },
                    assigned_roles,
                )

            elif latest_reassignment.role_id:
                # Fetch role details
                assigned_roles = (
                    db.query(Role).filter(Role.id == latest_reassignment.role_id).all()
                )
                assigned_roles = [
                    {"id": role.id, "name": role.name} for role in assigned_roles
                ]

                return (
                    str(latest_reassignment.role_id) in role_ids,
                    None,
                    assigned_roles,
                )

            else:
                return False, None, []

        # If no reassignment entry exists, check case_step_configs
        current_config = configs_by_step_id.get(current_step_id)
        if current_config:
            if current_config.current_assignee_id:
                assigned_user = (
                    db.query(User)
                    .filter(User.id == current_config.current_assignee_id)
                    .first()
                )

                step_role_ids = (
                    set(current_config.roles.split(",")) if current_config.roles else []
                )
                assigned_roles = db.query(Role).filter(Role.id.in_(step_role_ids)).all()
                assigned_roles = [
                    {"id": role.id, "name": role.name} for role in assigned_roles
                ]

                return (
                    current_config.current_assignee_id == logged_in_user.id,
                    {
                        "id": assigned_user.id if assigned_user else None,
                        "first_name": assigned_user.first_name
                        if assigned_user
                        else None,
                        "middle_name": assigned_user.middle_name
                        if assigned_user
                        else None,
                        "last_name": assigned_user.last_name if assigned_user else None,
                    },
                    assigned_roles,
                )

            else:
                step_role_ids = (
                    set(current_config.roles.split(",")) if current_config.roles else []
                )
                assigned_roles = db.query(Role).filter(Role.id.in_(step_role_ids)).all()
                assigned_roles = [
                    {"id": role.id, "name": role.name} for role in assigned_roles
                ]

                return bool(role_ids.intersection(step_role_ids)), None, assigned_roles

        return False, None, []  # Default to no access if none of the conditions are met

    def get_original_assignee_info(
        self, db: Session, case_no: str, step_id: str
    ) -> dict:
        """
        Returns the original user and role information for a case step if there are reassignments.
        Checks the latest reassignment to get user_id_at_assignment and roles_at_assignment.

        Args:
            db: Database session
            case_no: Case number
            step_id: Step ID to check

        Returns:
            dict: Contains original_user and original_roles information, or None if no reassignment
        """
        try:
            # Fetch latest case_reassignment entry for this case and step_id
            latest_reassignment = (
                db.query(CaseReassignment)
                .filter(
                    CaseReassignment.case_no == case_no,
                    CaseReassignment.step_id == step_id,
                )
                # Get the latest reassignment
                .order_by(desc(CaseReassignment.created_on))
                .first()
            )
            if not latest_reassignment:
                return {
                    "original_user": None,
                    "original_roles": [],
                    "has_reassignment": False,
                }

            # Get original user info from reassignment record
            original_user_info = None
            if latest_reassignment.user_id_at_assignment:
                original_user = (
                    db.query(User)
                    .filter(User.id == latest_reassignment.user_id_at_assignment)
                    .first()
                )
                if original_user:
                    original_user_info = {
                        "id": original_user.id,
                        "first_name": original_user.first_name,
                        "middle_name": original_user.middle_name,
                        "last_name": original_user.last_name,
                        "name": original_user.name,
                    }

            # Get original roles info from reassignment record
            original_roles_info = []
            if latest_reassignment.roles_at_assignment:
                original_roles_info = [
                    {"id": role.id, "name": role.name}
                    for role in latest_reassignment.roles_at_assignment
                ]

            return {
                "original_user": original_user_info,
                "original_roles": original_roles_info,
                "has_reassignment": True,
                "reassignment_date": latest_reassignment.created_on,
                "assigned_by_user": latest_reassignment.creator.name
                if latest_reassignment.creator
                else None,
            }

        except Exception as e:
            logger.error("Error getting original assignee info: %s", e, exc_info=True)
            return {
                "original_user": None,
                "original_roles": [],
                "has_reassignment": False,
                "error": str(e),
            }


class CaseReassignService:
    @classmethod
    def get_latest_case(cls, session: Session, case_no: str) -> Case:
        """Find the latest case entry by case number."""

        latest_case = (
            session.query(Case)
            .filter_by(case_no=case_no)
            .order_by(asc(Case.created_on))
            .all()
        )

        logger.info("----------------------------------------")
        logger.info("Count of cases %s", (len(latest_case)))
        # for c in latest_case:
        #     logger.info("step id %s, step name %s " %
        #                 (c.case_step_config.step_id, c.case_step_config.step_name))
        # logger.info("----------------------------------------\n")
        latest_case_info = latest_case[-1]

        # logger.info("current step id %s, current step name %s " %
        #             (latest_case_info.case_step_config.step_id, latest_case_info.case_step_config.step_name))

        if not latest_case_info:
            raise ValueError(f"Case with case_no '{case_no}' does not exist.")
        return latest_case_info

    @classmethod
    def assign_user_to_case(
        cls, db, logged_in_user, case_no, role_id, user_id, current_step_only=False
    ):
        """
        This function will take the role and id to assign the case to user
        """
        latest_case = cls.get_latest_case(db, case_no)
        if user_id:
            cls.update_case_reassignments_user(
                db, logged_in_user, latest_case, role_id, user_id, current_step_only
            )
        else:
            cls.update_case_reassignments_role(
                db, logged_in_user, latest_case, role_id, current_step_only
            )
        # Add new case record
        new_case = Case(
            case_no=case_no,
            case_type_id=latest_case.case_type_id,
            case_status_id=latest_case.case_status_id,
            case_step_config_id=latest_case.case_step_config_id,
            user_id=user_id,
            role_id=role_id,
            sla=latest_case.sla,
            created_by=logged_in_user.id,
        )
        db.add(new_case)
        db.flush()

        return "Ok"

    @classmethod
    def update_case_reassignments_user(
        cls, db, logged_in_user, case, role_id, new_user_id, current_step_only=False
    ):
        """
        Updates case_reassignments for all steps until user_id changes or is None.
        If current_step_only is True, only updates the current step.
        """
        current_step_config = case.case_step_config

        if current_step_only:
            # Only process the current step
            ordered_steps = [current_step_config]
        else:
            # Fetch all steps for this case type from case_step_configs
            case_step_configs = (
                db.query(CaseStepConfig)
                .filter(CaseStepConfig.case_type_id == case.case_type_id)
                .all()
            )
            steps_dict = {step.step_id: step for step in case_step_configs}
            ordered_steps = []
            step_id = current_step_config.step_id

            while step_id in steps_dict:
                step = steps_dict[step_id]
                ordered_steps.append(step)
                step_id = step.next_step_id  # Move to the next step

                if step_id is None:
                    break  # Stop if there's no next step

        # Fetch latest existing reassignments only for steps in steps_dict and maintain order
        # Alias for the CaseReassignment table
        CaseReassignmentAlias = aliased(CaseReassignment)

        # Subquery to get the latest reassignment per step
        latest_reassignments_subquery = (
            db.query(
                CaseReassignment.step_id,
                func.max(CaseReassignment.created_on).label("latest_created_on"),
            )
            .filter(CaseReassignment.case_no == case.case_no)
            .group_by(CaseReassignment.step_id)
            .subquery()
        )

        # Fetch only the latest reassignments per step
        existing_reassignments = (
            db.query(CaseReassignmentAlias)
            .join(
                latest_reassignments_subquery,
                (
                    CaseReassignmentAlias.step_id
                    == latest_reassignments_subquery.c.step_id
                )
                & (
                    CaseReassignmentAlias.created_on
                    == latest_reassignments_subquery.c.latest_created_on
                ),
            )
            .filter(CaseReassignmentAlias.case_no == case.case_no)
            .all()
        )
        # Maintain order based on ordered_steps
        reassignment_dict = {r.step_id: r for r in existing_reassignments}
        ordered_reassignments = [
            reassignment_dict[step.step_id]
            for step in ordered_steps
            if step.step_id in reassignment_dict
        ]

        # user who is reassigning the case
        previous_user_id = case.user_id

        # trying to reassign to same user currently on that step
        if previous_user_id == new_user_id:
            return {"Step is already assigned to that user"}

        first_reassignment_done = False

        for step_config in ordered_steps:
            previous_reassignment = next(
                (r for r in ordered_reassignments if r.step_id == step_config.step_id),
                None,
            )

            if not first_reassignment_done:
                # Create a new reassignment record
                case_reassignment = CaseReassignment(
                    case_no=case.case_no,
                    step_id=step_config.step_id,
                    user_id=new_user_id,
                    role_id=role_id,
                    user_id_at_assignment=case.user_id,
                    created_by=logged_in_user.id,
                )
                db.add(case_reassignment)
                db.flush()  # Flush to get the ID

                # Add roles at assignment (roles of the current assignee before reassignment)
                if case.user_id:
                    # Get roles from current case assignee (who is being reassigned FROM)
                    current_assignee = (
                        db.query(User).filter(User.id == case.user_id).first()
                    )
                    if current_assignee and current_assignee.roles:
                        case_reassignment.roles_at_assignment = current_assignee.roles
                elif step_config.roles:
                    # Fallback to step config roles if no user assigned
                    case_reassignment.roles_at_assignment = step_config.roles

                # Add assigned by roles (logged in user's roles)
                case_reassignment.assigned_by_roles = logged_in_user.roles

                first_reassignment_done = True

            # if the step was reassigned before
            elif previous_reassignment and first_reassignment_done:
                if previous_reassignment.user_id != previous_user_id:
                    break  # Stop if we encounter a step reassigned to a different user

                # Create a new reassignment record
                case_reassignment = CaseReassignment(
                    case_no=case.case_no,
                    step_id=step_config.step_id,
                    user_id=new_user_id,
                    role_id=role_id,
                    user_id_at_assignment=previous_reassignment.user_id
                    if previous_reassignment
                    else step_config.current_assignee_id,
                    created_by=logged_in_user.id,
                )
                db.add(case_reassignment)
                db.flush()  # Flush to get the ID

                # Add roles at assignment (from previous assignee, not original)
                if previous_reassignment:
                    # Get roles from the previous assignee (user who was assigned before this reassignment)
                    if previous_reassignment.user_id:
                        previous_user = (
                            db.query(User)
                            .filter(User.id == previous_reassignment.user_id)
                            .first()
                        )
                        if previous_user and previous_user.roles:
                            case_reassignment.roles_at_assignment = previous_user.roles
                    else:
                        # Previous was role-only assignment, use step config roles
                        case_reassignment.roles_at_assignment = step_config.roles
                else:
                    # No previous reassignment, use current step assignee
                    if step_config.current_assignee_id:
                        current_user = (
                            db.query(User)
                            .filter(User.id == step_config.current_assignee_id)
                            .first()
                        )
                        if current_user and current_user.roles:
                            case_reassignment.roles_at_assignment = current_user.roles
                    elif step_config.roles:
                        case_reassignment.roles_at_assignment = step_config.roles

                # Add assigned by roles (logged in user's roles)
                case_reassignment.assigned_by_roles = logged_in_user.roles

            # if step was not reassigned before
            elif not previous_reassignment and first_reassignment_done:
                if step_config.current_assignee_id == previous_user_id:
                    # Create a new reassignment record
                    case_reassignment = CaseReassignment(
                        case_no=case.case_no,
                        step_id=step_config.step_id,
                        user_id=new_user_id,
                        role_id=role_id,
                        user_id_at_assignment=step_config.current_assignee_id,
                        created_by=logged_in_user.id,
                    )
                    db.add(case_reassignment)
                    db.flush()  # Flush to get the ID

                    # Add roles at assignment (roles of the originally assigned user)
                    if step_config.current_assignee_id:
                        original_user = (
                            db.query(User)
                            .filter(User.id == step_config.current_assignee_id)
                            .first()
                        )
                        if original_user and original_user.roles:
                            case_reassignment.roles_at_assignment = original_user.roles
                    elif step_config.roles:
                        # Fallback to step config roles if no user assigned
                        case_reassignment.roles_at_assignment = step_config.roles

                    # Add assigned by roles (logged in user's roles)
                    case_reassignment.assigned_by_roles = logged_in_user.roles
                else:
                    break
            else:
                break

        db.flush()
        return (
            f"Reassignment completed starting from step {current_step_config.step_id}."
        )

    @classmethod
    def update_case_reassignments_role(
        cls, db, logged_in_user, case, role_id, current_step_only=False
    ):
        """
        Updates case_reassignments for the step when role_id is changed.
        The current_step_only flag is noted but role reassignment typically only affects current step.
        """
        current_step_config = case.case_step_config
        # Fetch existing reassignments for this case
        existing_reassignment = (
            db.query(CaseReassignment)
            .filter(
                CaseReassignment.case_no == case.case_no,
                CaseReassignment.step_id == current_step_config.step_id,
            )
            .order_by(CaseReassignment.created_on.desc())
            .first()
        )
        # If reassigned before, compare role_id
        if existing_reassignment:
            if existing_reassignment.role_id != role_id:
                case_reassignment = CaseReassignment(
                    case_no=case.case_no,
                    step_id=current_step_config.step_id,
                    user_id=None,  # Clear user assignment - role only
                    role_id=role_id,  # Update role
                    user_id_at_assignment=existing_reassignment.user_id,
                    created_by=logged_in_user.id,
                )
                db.add(case_reassignment)
                db.flush()  # Flush to get the ID

                # Add roles at assignment (from previous assignee)
                if existing_reassignment.user_id:
                    # Get roles from the previous assignee user
                    previous_user = (
                        db.query(User)
                        .filter(User.id == existing_reassignment.user_id)
                        .first()
                    )
                    if previous_user and previous_user.roles:
                        case_reassignment.roles_at_assignment = previous_user.roles
                else:
                    # Previous was role-only, use step config roles
                    case_reassignment.roles_at_assignment = [
                        existing_reassignment.roles
                    ]

                # Add assigned by roles (logged in user's roles)
                case_reassignment.assigned_by_roles = logged_in_user.roles
            else:
                return f"Step is already assigned to this role{current_step_config.step_id}."

        else:
            # Check case_step_config_role table for entries with the step_id
            existing_config_roles = db.execute(
                # Use lowercase table name
                select(case_step_config_role_table).where(
                    case_step_config_role_table.c.case_step_config_id
                    == case.case_step_config_id,
                    case_step_config_role_table.c.role_id == role_id,
                )
            ).first()
            if not existing_config_roles:
                # No existing role mapping, create new reassignment
                case_reassignment = CaseReassignment(
                    case_no=case.case_no,
                    step_id=current_step_config.step_id,
                    user_id=None,  # Clear user assignment - role only
                    role_id=role_id,  # Assign the new role
                    user_id_at_assignment=current_step_config.current_assignee_id,
                    created_by=logged_in_user.id,
                )
                db.add(case_reassignment)
                db.flush()  # Flush to get the ID

                # Add roles at assignment (roles of the originally assigned user)
                if current_step_config.current_assignee_id:
                    original_user = (
                        db.query(User)
                        .filter(User.id == current_step_config.current_assignee_id)
                        .first()
                    )
                    if original_user and original_user.roles:
                        case_reassignment.roles_at_assignment = original_user.roles
                elif current_step_config.roles:
                    # Fallback to step config roles if no user assigned
                    case_reassignment.roles_at_assignment = current_step_config.roles

                # Add assigned by roles (logged in user's roles)
                case_reassignment.assigned_by_roles = logged_in_user.roles
            else:
                return f"Step is already assigned to this role{current_step_config.step_id}."

        db.flush()
        return f"Reassignment updated for step {current_step_config.step_id}."


bpm_service = BPMService()
