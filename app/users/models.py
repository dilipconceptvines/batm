# app/users/models.py

from typing import List, Optional

from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Integer,
    String, Table, func,
)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship, Mapped, mapped_column

from app.core.db import Base

# --- Mixins ---
class AuditMixin:
    """Mixin for auditing fields."""

    @declared_attr
    def created_by(cls):
        """
        Column for the user who created this record
        """
        return Column(
            Integer,
            ForeignKey("users.id", ondelete="SET NULL", onupdate="CASCADE"),
            nullable=True,
            comment="User who created this record",
        )

    @declared_attr
    def modified_by(cls):
        """
        Column for the user who last modified this record
        """
        return Column(
            Integer,
            ForeignKey("users.id", ondelete="SET NULL", onupdate="CASCADE"),
            nullable=True,
            comment="User who last modified this record",
        )

    @declared_attr
    def created_on(cls):
        """
        Column for the timestamp when this record was created
        """
        return Column(
            DateTime(timezone=True),
            server_default=func.now(),
            comment="Timestamp when this record was created",
        )

    @declared_attr
    def updated_on(cls):
        """
        Column for the timestamp when this record was last updated
        """
        return Column(
            DateTime(timezone=True),
            onupdate=func.now(),
            server_default=None,
            comment="Timestamp when this record was last updated",
        )

    @declared_attr
    def creator(cls):
        """
        Relationship to the user who created this record
        """
        if cls.__name__.lower() == "user":
            return None
        return relationship(
            "User",
            foreign_keys=[cls.created_by],
            backref="created_%s" % cls.__tablename__,
        )

    @declared_attr
    def updater(cls):
        """
        Relationship to the user who last modified this record
        """
        if cls.__name__.lower() == "user":
            return None
        return relationship(
            "User",
            foreign_keys=[cls.modified_by],
            backref="updated_%s" % cls.__tablename__,
        )

    is_archived = Column(
        Boolean,
        nullable=True,
        default=False,
        comment="Flag indicating if the record is archived",
    )
    is_active = Column(
        Boolean, default=True, comment="Flag to keep track of record is active or not"
    )
# --- End of Mixins ---


# --- Association table for many-to-many relationship between users and roles ---
user_role_association = Table(
    "users_and_roles",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id"), primary_key=True),
    Column("role_id", Integer, ForeignKey("roles.id"), primary_key=True),
    Column("is_active", Boolean, default=True),
    Column("created_at", DateTime(timezone=True), server_default=func.now()),
    Column("updated_on", DateTime(timezone=True), onupdate=func.now()),
)


class User(Base, AuditMixin):
    """User model"""
    __tablename__ = "users"

    # --- Columns ---
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    middle_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    email_address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    password: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    last_login: Mapped[Optional[DateTime]] = mapped_column(DateTime, nullable=True)

    # --- Relationships ---
    roles: Mapped[List["Role"]] = relationship(
        back_populates="users", secondary=user_role_association
    )
    slas: Mapped[List["SLA"]] = relationship(back_populates="users", foreign_keys="SLA.user_id")
    case_step_configs: Mapped[List["CaseStepConfig"]] = relationship(
        back_populates="next_assignee",
        foreign_keys="CaseStepConfig.next_assignee_id",
    )
    case_users: Mapped[List["Case"]] = relationship(
        back_populates="users", foreign_keys="Case.user_id"
    )
    created_audit_trails: Mapped[List["AuditTrail"]] = relationship(
        "AuditTrail",
        back_populates="creator",
        foreign_keys="[AuditTrail.created_by]",
        overlaps="user"
    )
    case_reassignments: Mapped[List["CaseReassignment"]] = relationship(
        back_populates="users",
        foreign_keys="CaseReassignment.user_id",
    )
    case_reassignments_at_assignment: Mapped[List["CaseReassignment"]] = relationship(
        back_populates="user_at_assignment",
        foreign_keys="CaseReassignment.user_id_at_assignment",
    )

    @property
    def name(self):
        """
        Returns concatenated name
        """
        return f"{self.first_name} {self.last_name}".strip()

    def __repr__(self):
        """
        String representation of the User model
        """
        return f"<User(id={self.id}, email='{self.email_address}', name='{self.first_name} {self.last_name}', is_active={self.is_active}) , roles={self.roles}>"


class Role(Base, AuditMixin):
    """Role model"""
    __tablename__ = "roles"

    # --- Columns ---
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # --- Relationships ---
    users: Mapped[List["User"]] = relationship(
        back_populates="roles", secondary=user_role_association
    )
    slas: Mapped[List["SLA"]] = relationship(back_populates="roles", foreign_keys="SLA.role_id")
    case_reassignments: Mapped[List["CaseReassignment"]] = relationship(
        back_populates="roles",
        foreign_keys="CaseReassignment.role_id",
    )

    def __repr__(self):
        """
        String representation of the Role model
        """
        return f"<Role(id={self.id}, name='{self.name}', is_active={self.is_active})>"
