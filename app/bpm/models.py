## app/bpm/models.py

# Standard library imports
import json

# Third party imports
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

# Local imports
from app.core.db import Base
from app.users.models import AuditMixin, Role


class SLA(Base, AuditMixin):
    """
    SLA Table
    """

    __tablename__ = "slas"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(
        String(255), unique=True, nullable=False, index=True, comment="Name of the SLA"
    )
    case_step_config_id = Column(
        Integer, ForeignKey("case_step_configs.id"), nullable=False
    )
    time_limit = Column(
        Integer, nullable=False, comment="Time limit in minutes for the SLA"
    )
    is_active = Column(
        Boolean,
        default=True,
        nullable=False,
        comment="Flag to indicate if the SLA is active",
    )

    # Escalation level for SLA
    escalation_level = Column(
        Integer, nullable=False, comment="Escalation level for SLA"
    )

    # Role or User assignment for SLA
    role_id = Column(
        Integer,
        ForeignKey("roles.id"),
        nullable=True,
        comment="Role assigned to this SLA",
    )
    user_id = Column(
        Integer,
        ForeignKey("users.id"),
        nullable=True,
        comment="User assigned to this SLA",
    )

    # Relationships
    case_step_configs = relationship(
        "CaseStepConfig", back_populates="slas", foreign_keys=[case_step_config_id]
    )
    roles = relationship("Role", back_populates="slas", foreign_keys=[role_id])
    users = relationship("User", back_populates="slas", foreign_keys=[user_id])


class CaseStatus(Base, AuditMixin):
    """
    CaseStatus model
    """

    __tablename__ = "case_statuses"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True)

    cases = relationship(
        "Case", foreign_keys="Case.case_status_id", back_populates="case_status"
    )


class CaseType(Base, AuditMixin):
    """
    CaseType model
    """

    __tablename__ = "case_types"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True)
    prefix = Column(String(255), unique=True, index=True)  # Prefix for case type
    first_steps = relationship("CaseTypeFirstStep", back_populates="case_type")

    # Relationship to CaseStepConfig
    types = relationship("CaseStepConfig", back_populates="case_type")
    case_step = relationship("CaseStep", back_populates="case_type")

    cases = relationship(
        "Case", foreign_keys="Case.case_type_id", back_populates="case_type"
    )


class CaseStep(Base, AuditMixin):
    """
    CaseStep model
    """

    __tablename__ = "case_steps"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True)
    case_type_id = Column(
        Integer, ForeignKey("case_types.id"), nullable=False
    )  # Links to CaseType
    weight = Column(Integer, nullable=False)

    # Relationship to CaseStepConfig
    configs = relationship("CaseStepConfig", back_populates="case_step")
    case_type = relationship("CaseType", back_populates="case_step")


# Association table for many-to-many relationship between CaseStepConfig and Role
case_step_config_role_table = Table(
    "case_step_config_role",
    Base.metadata,
    Column(
        "case_step_config_id",
        Integer,
        ForeignKey("case_step_configs.id"),
        primary_key=True,
    ),
    Column("role_id", Integer, ForeignKey("roles.id"), primary_key=True),
)


# CaseStepConfig model
class CaseStepConfig(Base, AuditMixin):
    """
    CaseStepConfig model
    """

    __tablename__ = "case_step_configs"

    id = Column(Integer, primary_key=True, index=True)
    # Unique random string identifier
    step_id = Column(String(255), unique=True, index=True)
    step_name = Column(String(255), unique=True, index=True)
    case_step_id = Column(
        Integer, ForeignKey("case_steps.id"), nullable=False
    )  # Links to CaseStep
    case_type_id = Column(
        Integer, ForeignKey("case_types.id"), nullable=False
    )  # Links to CaseType
    current_assignee_id = Column(
        Integer, ForeignKey("users.id"), nullable=True
    )  # Nullable foreign key to User
    next_assignee_id = Column(
        Integer, ForeignKey("users.id"), nullable=True
    )  # Nullable foreign key to User
    # Nullable string for next step identifier
    next_step_id = Column(String(255), nullable=True)

    # Relationships
    case_step = relationship("CaseStep", back_populates="configs")
    current_assignee = relationship(
        "User", back_populates="case_step_configs", foreign_keys=[current_assignee_id]
    )
    next_assignee = relationship(
        "User", back_populates="case_step_configs", foreign_keys=[next_assignee_id]
    )
    roles = relationship(
        "Role",
        secondary=case_step_config_role_table,
        back_populates="case_step_configs",
    )
    paths = relationship("CaseStepConfigPath", back_populates="case_step_config")
    case_type = relationship("CaseType", back_populates="types")
    slas = relationship(
        "SLA",
        back_populates="case_step_configs",
        foreign_keys="SLA.case_step_config_id",
    )


Role.case_step_configs = relationship(
    "CaseStepConfig", secondary=case_step_config_role_table, back_populates="roles"
)


class CaseTypeFirstStep(Base, AuditMixin):
    """
    CaseTypeFirstStep model
    """

    __tablename__ = "case_type_first_steps"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key to the CaseType table
    case_type_id = Column(Integer, ForeignKey("case_types.id"), nullable=False)
    # Foreign key to the CaseStepConfig table
    first_step_id = Column(Integer, ForeignKey("case_step_configs.id"), nullable=True)

    # Relationships
    case_type = relationship("CaseType", back_populates="first_steps")
    first_step = relationship("CaseStepConfig")


class Case(Base, AuditMixin):
    """
    Case model
    """

    __tablename__ = "cases"

    id = Column(Integer, primary_key=True, index=True)
    case_no = Column(String(255), nullable=False)

    # Foreign keys for relationships to other tables
    case_type_id = Column(Integer, ForeignKey("case_types.id"), nullable=False)
    case_status_id = Column(Integer, ForeignKey("case_statuses.id"), nullable=False)
    sla_id = Column(Integer, ForeignKey("slas.id"), nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    role_id = Column(Integer, ForeignKey("roles.id"), nullable=True)

    case_step_config_id = Column(
        Integer, ForeignKey("case_step_configs.id"), nullable=True
    )  # New field

    # Relationships to other tables
    case_type = relationship(
        "CaseType", foreign_keys=[case_type_id], back_populates="cases"
    )
    case_status = relationship(
        "CaseStatus", foreign_keys=[case_status_id], back_populates="cases"
    )
    sla = relationship("SLA")
    users = relationship("User", back_populates="case_users", foreign_keys=[user_id])

    # Relationship to CaseStepConfig
    case_step_config = relationship("CaseStepConfig")
    role = relationship("Role")

    # Relationship to AuditTrail
    audit_trail = relationship("AuditTrail", back_populates="case")

    # Text representation
    def __str__(self):
        return json.dumps(
            {
                "case_no": self.case_no,
                "case_status": self.case_status.name,
                "case_step": self.case_step_config.step_name,
                "case_type": self.case_type.name,
                "created_on": self.created_on.strftime("%Y-%m-%d %H:%M:%S"),
                "created_by": getattr(self.creator, "first_name", ""),
            }
        )


class CaseStepConfigPath(Base, AuditMixin):
    """
    CaseStepConfigPath model
    """

    __tablename__ = "case_step_config_paths"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key to CaseStepConfig
    case_step_config_id = Column(
        Integer, ForeignKey("case_step_configs.id"), nullable=False
    )

    # Path to the JSON schema file
    path = Column(String(255), nullable=True, comment="File path to JSON schema")

    # Relationship to CaseStepConfig
    case_step_config = relationship("CaseStepConfig", back_populates="paths")


class CaseEntity(Base, AuditMixin):
    """
    CaseEntity model
    """

    __tablename__ = "case_entities"

    id = Column(Integer, primary_key=True, index=True)

    case_no = Column(String(255), nullable=False)

    # Entity-specific fields
    entity_name = Column(String(255), nullable=False)
    identifier = Column(String(255), nullable=False)
    identifier_value = Column(String(255), nullable=False)


# Association table for many-to-many relationship between CaseReassignment and Role (for role_id_at_assignment)
case_reassignment_roles_at_assignment_table = Table(
    "case_reassignment_roles_at_assignment",
    Base.metadata,
    Column(
        "case_reassignment_id",
        Integer,
        ForeignKey("case_reassignments.id"),
        primary_key=True,
    ),
    Column("role_id", Integer, ForeignKey("roles.id"), primary_key=True),
)

# Association table for many-to-many relationship between CaseReassignment and Role (for assigned_by_role)
case_reassignment_assigned_by_roles_table = Table(
    "case_reassignment_assigned_by_roles",
    Base.metadata,
    Column(
        "case_reassignment_id",
        Integer,
        ForeignKey("case_reassignments.id"),
        primary_key=True,
    ),
    Column("role_id", Integer, ForeignKey("roles.id"), primary_key=True),
)


class CaseReassignment(Base, AuditMixin):
    (
        """"
    CaseReassignment Model
    """
        ""
    )

    __tablename__ = "case_reassignments"

    id = Column(Integer, primary_key=True, index=True)
    case_no = Column(String(255), nullable=False)
    step_id = Column(String(255), index=True, nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    role_id = Column(Integer, ForeignKey("roles.id"), nullable=False)
    user_id_at_assignment = Column(Integer, ForeignKey("users.id"), nullable=True)

    users = relationship(
        "User", back_populates="case_reassignments", foreign_keys=[user_id]
    )
    roles = relationship(
        "Role", back_populates="case_reassignments", foreign_keys=[role_id]
    )

    assigned_by_roles = relationship(
        "Role",
        secondary=case_reassignment_assigned_by_roles_table,
        back_populates="case_reassignments_assigned_by"
    )

    user_at_assignment = relationship(
        "User",
        back_populates="case_reassignments",
        foreign_keys=[user_id_at_assignment],
    )
    roles_at_assignment = relationship(
        "Role",
        secondary=case_reassignment_roles_at_assignment_table,
        back_populates="case_reassignments_at_assignment"
    )


# Add back_populates for roles_at_assignment in Role model
Role.case_reassignments_at_assignment = relationship(
    "CaseReassignment",
    secondary=case_reassignment_roles_at_assignment_table,
    back_populates="roles_at_assignment"
)

# Add back_populates for assigned_by_roles in Role model
Role.case_reassignments_assigned_by = relationship(
    "CaseReassignment",
    secondary=case_reassignment_assigned_by_roles_table,
    back_populates="assigned_by_roles"
)
