import enum
from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    JSON,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Gender(str, enum.Enum):
    male = "male"
    female = "female"
    other = "other"


# ---------------------------------------------------------------------------
# Lookup tables — all share {id, code, name}, seeded but fully CRUD-able via
# the generic lookup router (app/routers/lookup.py).
# ---------------------------------------------------------------------------


class HouseholdRole(Base):
    __tablename__ = "household_roles"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)


class MembershipCategory(Base):
    __tablename__ = "membership_categories"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)


class MemberStatus(Base):
    __tablename__ = "member_statuses"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)


class Nationality(Base):
    __tablename__ = "nationalities"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(10), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    dial_code: Mapped[str | None] = mapped_column(String(10), nullable=True)


class ContactType(Base):
    __tablename__ = "contact_types"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)


# ---------------------------------------------------------------------------
# Core organizational / membership data
# ---------------------------------------------------------------------------


class Church(Base):
    __tablename__ = "churches"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(30), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    app_name: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    families: Mapped[list["Family"]] = relationship(back_populates="church", foreign_keys="Family.church_id")
    members: Mapped[list["Member"]] = relationship(back_populates="church", foreign_keys="Member.church_id")


class Family(Base):
    __tablename__ = "families"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    alias: Mapped[str | None] = mapped_column(String(100), nullable=True)
    leader_id: Mapped[int | None] = mapped_column(ForeignKey("members.id", use_alter=True), nullable=True)
    church_id: Mapped[int] = mapped_column(ForeignKey("churches.id"), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, onupdate=func.now())

    church: Mapped["Church"] = relationship(back_populates="families", foreign_keys=[church_id])
    leader: Mapped["Member | None"] = relationship(foreign_keys=[leader_id], post_update=True)
    members: Mapped[list["Member"]] = relationship(back_populates="family", foreign_keys="Member.family_id")

    @property
    def leader_name(self) -> str | None:
        if self.leader is None:
            return None
        return f"{self.leader.first_name} {self.leader.last_name}"

    @property
    def member_count(self) -> int:
        return len(self.members)


class RegistrationStatus(str, enum.Enum):
    pending = "pending"
    approved = "approved"
    rejected = "rejected"


class CommunicationChannel(str, enum.Enum):
    email = "email"
    sms = "sms"
    whatsapp = "whatsapp"
    physical = "physical"


class CommunicationCategory(str, enum.Enum):
    """What the communication is about — lets a member pick a different
    channel per kind of message (e.g. SMS for offering receipts, email for
    church announcements) instead of one blanket setting for everything.
    'general' is the catch-all bucket and the one the admin MemberForm still
    edits as a flat "preferred communication channels" checkbox group."""
    general = "general"
    offering_receipts = "offering_receipts"
    church_announcements = "church_announcements"


class LookupStatus(str, enum.Enum):
    """Shared active/inactive status for the church-scoped lookups below
    (Organisation, SabbathSchoolGroup, ChurchRole) — each gets its own
    distinctly-named Postgres enum type so they don't collide with each
    other or with CollectionTypeStatus, even though the values are the same."""
    active = "active"
    inactive = "inactive"


class Organisation(Base):
    """A ministry/organisation a member can belong to (AMO, Dorcas, Choir,
    PF, Jumuiya, ...) — many-to-many via MemberOrganisation. NULL church_id
    = a global default available to every church (same convention as
    CollectionType.church_id)."""
    __tablename__ = "organisations"
    __table_args__ = (UniqueConstraint("code", "church_id", name="uq_organisation_code_church"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    church_id: Mapped[int | None] = mapped_column(ForeignKey("churches.id"), nullable=True, index=True)
    status: Mapped[LookupStatus] = mapped_column(Enum(LookupStatus, name="organisation_status"), nullable=False, default=LookupStatus.active)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    church: Mapped["Church | None"] = relationship()


class SabbathSchoolGroup(Base):
    """A single Sabbath School group a member belongs to (one-to-many from
    the group's side, so it's a plain FK on Member rather than a join table)."""
    __tablename__ = "sabbath_school_groups"
    __table_args__ = (UniqueConstraint("code", "church_id", name="uq_ss_group_code_church"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    church_id: Mapped[int | None] = mapped_column(ForeignKey("churches.id"), nullable=True, index=True)
    status: Mapped[LookupStatus] = mapped_column(Enum(LookupStatus, name="sabbath_school_group_status"), nullable=False, default=LookupStatus.active)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    church: Mapped["Church | None"] = relationship()


class ChurchRole(Base):
    """A ministry/functional role a member can hold (Elder, Deacon, Choir
    Leader, ...) — distinct from the RBAC `Role` (system login/permissions),
    many-to-many via MemberChurchRole."""
    __tablename__ = "church_roles"
    __table_args__ = (UniqueConstraint("code", "church_id", name="uq_church_role_code_church"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    church_id: Mapped[int | None] = mapped_column(ForeignKey("churches.id"), nullable=True, index=True)
    status: Mapped[LookupStatus] = mapped_column(Enum(LookupStatus, name="church_role_status"), nullable=False, default=LookupStatus.active)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    church: Mapped["Church | None"] = relationship()


class Member(Base):
    __tablename__ = "members"
    id: Mapped[int] = mapped_column(primary_key=True)
    first_name: Mapped[str] = mapped_column(String(120), nullable=False)
    middle_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    last_name: Mapped[str] = mapped_column(String(120), nullable=False)
    gender: Mapped[Gender | None] = mapped_column(Enum(Gender, name="gender"), nullable=True)
    date_of_birth: Mapped[date | None] = mapped_column(Date, nullable=True)

    household_role_id: Mapped[int] = mapped_column(ForeignKey("household_roles.id"), nullable=False)
    membership_category_id: Mapped[int] = mapped_column(ForeignKey("membership_categories.id"), nullable=False)
    status_id: Mapped[int] = mapped_column(ForeignKey("member_statuses.id"), nullable=False)

    family_id: Mapped[int | None] = mapped_column(ForeignKey("families.id"), nullable=True, index=True)
    church_id: Mapped[int] = mapped_column(ForeignKey("churches.id"), nullable=False, index=True)

    national_id: Mapped[str | None] = mapped_column(String(60), nullable=True)
    nationality_id: Mapped[int | None] = mapped_column(ForeignKey("nationalities.id"), nullable=True)
    address: Mapped[str | None] = mapped_column(String(400), nullable=True)
    occupation: Mapped[str | None] = mapped_column(String(200), nullable=True)
    join_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    notes: Mapped[str | None] = mapped_column(String(1000), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, onupdate=func.now())

    # Registration review workflow (ToR "Registration review and approval") —
    # every existing creation path (privileged staff via MemberForm) defaults
    # to 'approved' so current behavior is unchanged; a creator can instead
    # opt into 'pending' (MemberCreate.submit_for_review) for assisted
    # registration that a separate `review_members` holder must decide on.
    registration_status: Mapped["RegistrationStatus"] = mapped_column(
        Enum(RegistrationStatus, name="member_registration_status"), nullable=False, default=RegistrationStatus.approved, index=True
    )

    # Transfer-in / transfer-out (ToR Registration). A transfer-out doesn't
    # remove the member from their church's historical roster — it flags
    # them via `status` (the 'transferred' MemberStatus) and records where/
    # when/why. Transfer-in is just a normal create with `transferred_from_church_id`
    # set for provenance — there's no separate "transfer-in" row shape.
    transferred_from_church_id: Mapped[int | None] = mapped_column(ForeignKey("churches.id"), nullable=True)
    transfer_out_church_id: Mapped[int | None] = mapped_column(ForeignKey("churches.id"), nullable=True)
    transfer_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    transfer_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)

    sabbath_school_group_id: Mapped[int | None] = mapped_column(ForeignKey("sabbath_school_groups.id"), nullable=True)

    church: Mapped["Church"] = relationship(back_populates="members", foreign_keys=[church_id])
    family: Mapped["Family | None"] = relationship(back_populates="members", foreign_keys=[family_id])
    household_role: Mapped["HouseholdRole"] = relationship()
    membership_category: Mapped["MembershipCategory"] = relationship()
    status: Mapped["MemberStatus"] = relationship()
    nationality: Mapped["Nationality | None"] = relationship()
    contacts: Mapped[list["MemberContact"]] = relationship(back_populates="member", cascade="all, delete-orphan")
    sabbath_school_group: Mapped["SabbathSchoolGroup | None"] = relationship()
    transferred_from_church: Mapped["Church | None"] = relationship(foreign_keys=[transferred_from_church_id])
    transfer_out_church: Mapped["Church | None"] = relationship(foreign_keys=[transfer_out_church_id])
    organisation_links: Mapped[list["MemberOrganisation"]] = relationship(cascade="all, delete-orphan")
    church_role_links: Mapped[list["MemberChurchRole"]] = relationship(cascade="all, delete-orphan")
    communication_preferences: Mapped[list["MemberCommunicationPreference"]] = relationship(cascade="all, delete-orphan")

    @property
    def family_name(self) -> str | None:
        return self.family.name if self.family is not None else None

    @property
    def member_number(self) -> str:
        """The ToR's "unique member identifier" — derived, not stored, so it's
        always unique (piggybacks on the primary key) with zero migration risk."""
        church_code = self.church.code if self.church is not None else "MEM"
        return f"{church_code}-{self.id:05d}"

    @property
    def sabbath_school_group_name(self) -> str | None:
        return self.sabbath_school_group.name if self.sabbath_school_group is not None else None

    @property
    def organisation_ids(self) -> list[int]:
        return [link.organisation_id for link in self.organisation_links]

    @property
    def organisation_names(self) -> list[str]:
        return [link.organisation.name for link in self.organisation_links]

    @property
    def church_role_ids(self) -> list[int]:
        return [link.church_role_id for link in self.church_role_links]

    @property
    def church_role_names(self) -> list[str]:
        return [link.church_role.name for link in self.church_role_links]

    @property
    def preferred_channels(self) -> list[str]:
        """Backward-compatible flat view — the 'general' category only, which
        is all the admin MemberForm's checkbox group ever writes to."""
        return [p.channel.value for p in self.communication_preferences if p.category == CommunicationCategory.general]

    @property
    def communication_preferences_by_category(self) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {c.value: [] for c in CommunicationCategory}
        for p in self.communication_preferences:
            result[p.category.value].append(p.channel.value)
        return result


class MemberContact(Base):
    __tablename__ = "member_contacts"
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int] = mapped_column(ForeignKey("members.id"), nullable=False, index=True)
    contact_type_id: Mapped[int] = mapped_column(ForeignKey("contact_types.id"), nullable=False)
    contact_init: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    contact: Mapped[str] = mapped_column(String(320), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    member: Mapped["Member"] = relationship(back_populates="contacts")
    contact_type: Mapped["ContactType"] = relationship()


class MemberOrganisation(Base):
    __tablename__ = "member_organisations"
    __table_args__ = (UniqueConstraint("member_id", "organisation_id", name="uq_member_organisation"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int] = mapped_column(ForeignKey("members.id"), nullable=False, index=True)
    organisation_id: Mapped[int] = mapped_column(ForeignKey("organisations.id"), nullable=False, index=True)
    joined_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    organisation: Mapped["Organisation"] = relationship()


class MemberChurchRole(Base):
    __tablename__ = "member_church_roles"
    __table_args__ = (UniqueConstraint("member_id", "church_role_id", name="uq_member_church_role"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int] = mapped_column(ForeignKey("members.id"), nullable=False, index=True)
    church_role_id: Mapped[int] = mapped_column(ForeignKey("church_roles.id"), nullable=False, index=True)
    assigned_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    church_role: Mapped["ChurchRole"] = relationship()


class MemberCommunicationPreference(Base):
    """Which channel(s) a member wants to be reached on, per kind of
    communication (ToR Table 6's "Preferred communication methods",
    extended so a member can pick e.g. SMS for offering receipts but email
    for church announcements) — independent of the actual contact values in
    MemberContact, since 'physical correspondence' isn't a contact value at all."""
    __tablename__ = "member_communication_preferences"
    __table_args__ = (UniqueConstraint("member_id", "category", "channel", name="uq_member_comm_category_channel"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int] = mapped_column(ForeignKey("members.id"), nullable=False, index=True)
    category: Mapped[CommunicationCategory] = mapped_column(
        Enum(CommunicationCategory, name="communication_category"), nullable=False, default=CommunicationCategory.general
    )
    channel: Mapped[CommunicationChannel] = mapped_column(Enum(CommunicationChannel, name="communication_channel"), nullable=False)


class MemberAuditLog(Base):
    """Append-only trail for "View record history where authorised" — one
    row per create/update/delete/transfer/registration-decision."""
    __tablename__ = "member_audit_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int] = mapped_column(ForeignKey("members.id"), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(60), nullable=False)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    details: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    actor: Mapped["User | None"] = relationship()

    @property
    def actor_username(self) -> str | None:
        return self.actor.username if self.actor is not None else None


class CorrectionRequestStatus(str, enum.Enum):
    pending = "pending"
    approved = "approved"
    rejected = "rejected"


class MemberCorrectionRequest(Base):
    """A member's self-service "request corrections" submission — only the
    allow-listed personal-detail fields (see member_service.CORRECTABLE_FIELDS)
    are ever applied; membership/status/church/family changes stay staff-only
    via the admin MemberForm."""
    __tablename__ = "member_correction_requests"
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int] = mapped_column(ForeignKey("members.id"), nullable=False, index=True)
    requested_changes: Mapped[dict] = mapped_column(JSON, nullable=False)
    reason: Mapped[str | None] = mapped_column(String(500), nullable=True)
    status: Mapped[CorrectionRequestStatus] = mapped_column(
        Enum(CorrectionRequestStatus, name="correction_request_status"), nullable=False, default=CorrectionRequestStatus.pending, index=True
    )
    reviewed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    review_note: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    decided_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    member: Mapped["Member"] = relationship()
    reviewed_by: Mapped["User | None"] = relationship()

    @property
    def member_name(self) -> str | None:
        return f"{self.member.first_name} {self.member.last_name}" if self.member is not None else None

    @property
    def reviewed_by_username(self) -> str | None:
        return self.reviewed_by.username if self.reviewed_by is not None else None


class FeedbackStatus(str, enum.Enum):
    open = "open"
    reviewed = "reviewed"


class MemberFeedback(Base):
    __tablename__ = "member_feedback"
    id: Mapped[int] = mapped_column(primary_key=True)
    member_id: Mapped[int] = mapped_column(ForeignKey("members.id"), nullable=False, index=True)
    subject: Mapped[str] = mapped_column(String(200), nullable=False)
    message: Mapped[str] = mapped_column(String(2000), nullable=False)
    status: Mapped[FeedbackStatus] = mapped_column(Enum(FeedbackStatus, name="feedback_status"), nullable=False, default=FeedbackStatus.open, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    member: Mapped["Member"] = relationship()

    @property
    def member_name(self) -> str | None:
        return f"{self.member.first_name} {self.member.last_name}" if self.member is not None else None


# ---------------------------------------------------------------------------
# Collections — a member's contribution, split across one or more collection
# types. Bulk-imported or manually entered rows are saved as `pending` inside
# a `CollectionGroup` (the batch/submission record); a separate reviewer
# approves or rejects each collection, and approval issues a `Receipt`.
# ---------------------------------------------------------------------------


class CollectionTypeStatus(str, enum.Enum):
    active = "active"
    inactive = "inactive"


class CollectionSource(str, enum.Enum):
    cash = "cash"
    mobile_money = "mobile_money"
    bank_transfer = "bank_transfer"
    cheque = "cheque"
    other = "other"


class CollectionEntryMethod(str, enum.Enum):
    manual = "manual"
    import_ = "import"
    # A "Digital Treasurer" agent parsed a submitted receipt (photo/slip/bank
    # or mobile-money notification) into structured data on the caller's
    # behalf — see collection_service.submit_agent_collection. Still lands as
    # an ordinary pending Collection; nothing here bypasses the existing
    # review/approve workflow.
    agent = "agent"


class CollectionStatus(str, enum.Enum):
    pending = "pending"
    approved = "approved"
    rejected = "rejected"


class CollectionGroupStatus(str, enum.Enum):
    pending = "pending"
    reviewed = "reviewed"


class CollectionType(Base):
    __tablename__ = "collection_types"
    __table_args__ = (UniqueConstraint("code", "church_id", name="uq_collection_type_code_church"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    # NULL = a global default type available to every church (same convention
    # as the old system's collection_codes.church); non-null = church-specific.
    church_id: Mapped[int | None] = mapped_column(ForeignKey("churches.id"), nullable=True, index=True)
    status: Mapped[CollectionTypeStatus] = mapped_column(
        Enum(CollectionTypeStatus, name="collection_type_status"), nullable=False, default=CollectionTypeStatus.active
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    church: Mapped["Church | None"] = relationship()


class CollectionGroup(Base):
    __tablename__ = "collection_groups"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(40), unique=True, nullable=False)
    name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    church_id: Mapped[int] = mapped_column(ForeignKey("churches.id"), nullable=False, index=True)
    # Derived from its collections' statuses (pending while any collection is
    # still pending, reviewed once every collection has been decided) —
    # written whenever a collection's status changes, see collection_service.
    status: Mapped[CollectionGroupStatus] = mapped_column(
        Enum(CollectionGroupStatus, name="collection_group_status"), nullable=False, default=CollectionGroupStatus.pending, index=True
    )
    initiator_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    initiated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    approver_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    church: Mapped["Church"] = relationship()
    initiator: Mapped["User"] = relationship(foreign_keys=[initiator_user_id])
    approver: Mapped["User | None"] = relationship(foreign_keys=[approver_user_id])
    collections: Mapped[list["Collection"]] = relationship(back_populates="collection_group")

    @property
    def collection_count(self) -> int:
        return len(self.collections)

    @property
    def total_amount(self) -> float:
        return sum(float(c.total_amount) for c in self.collections)

    @property
    def initiator_username(self) -> str | None:
        return self.initiator.username if self.initiator is not None else None

    @property
    def approver_username(self) -> str | None:
        return self.approver.username if self.approver is not None else None


class Collection(Base):
    __tablename__ = "collections"
    id: Mapped[int] = mapped_column(primary_key=True)
    collection_group_id: Mapped[int] = mapped_column(ForeignKey("collection_groups.id"), nullable=False, index=True)
    # Nullable to support true anonymous contributions (see is_anonymous) —
    # a service-level rule keeps the two mutually consistent (member_id set
    # <=> is_anonymous False), enforced in collection_service, not the DB.
    member_id: Mapped[int | None] = mapped_column(ForeignKey("members.id"), nullable=True, index=True)
    church_id: Mapped[int] = mapped_column(ForeignKey("churches.id"), nullable=False, index=True)
    total_amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False, default=0)
    collection_source: Mapped[CollectionSource] = mapped_column(
        Enum(CollectionSource, name="collection_source"), nullable=False, default=CollectionSource.cash
    )
    entry_method: Mapped[CollectionEntryMethod] = mapped_column(
        Enum(CollectionEntryMethod, name="collection_entry_method"), nullable=False, default=CollectionEntryMethod.manual
    )
    collection_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[CollectionStatus] = mapped_column(
        Enum(CollectionStatus, name="collection_status"), nullable=False, default=CollectionStatus.pending, index=True
    )
    rejection_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)
    # Assigned on approval only (see collection_service.approve_collection).
    receipt_number: Mapped[str | None] = mapped_column(String(60), unique=True, nullable=True)
    notes: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    is_anonymous: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # Bank/mobile-money transaction reference, entered by whoever records the
    # contribution — matched against uploaded statement rows during
    # reconciliation (see StatementTransaction).
    payment_reference: Mapped[str | None] = mapped_column(String(120), nullable=True)
    # Path to an uploaded payment slip/evidence file, if any (see
    # collection_service.save_evidence_file) — never a public static mount,
    # streamed back only to callers with read access to this collection.
    evidence_file_path: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, onupdate=func.now())

    # Populated only for agent-submitted rows (entry_method=agent) — the full
    # raw JSON from the Digital Treasurer receipt-reading agent, kept as-is
    # for the reviewer to inspect (it already carries the agent's own
    # valid/validation_issues/summary/etc.) and for audit traceability.
    agent_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    collection_group: Mapped["CollectionGroup"] = relationship(back_populates="collections")
    member: Mapped["Member | None"] = relationship()
    church: Mapped["Church"] = relationship()
    items: Mapped[list["CollectionItem"]] = relationship(back_populates="collection", cascade="all, delete-orphan")
    # One-to-many, not one-to-one: a correction that reissues a receipt (see
    # receipt_service.correct_receipt) adds a new row rather than replacing
    # the old one, preserving history. `receipt_number` above always points
    # at whichever one is current — see `current_receipt`.
    receipts: Mapped[list["Receipt"]] = relationship(back_populates="collection", cascade="all, delete-orphan")

    @property
    def member_name(self) -> str | None:
        if self.is_anonymous:
            return "Anonymous"
        return f"{self.member.first_name} {self.member.last_name}" if self.member is not None else None

    @property
    def current_receipt(self) -> "Receipt | None":
        if self.receipt_number is None:
            return None
        return next((r for r in self.receipts if r.receipt_number == self.receipt_number), None)


class CollectionItem(Base):
    __tablename__ = "collection_items"
    id: Mapped[int] = mapped_column(primary_key=True)
    collection_id: Mapped[int] = mapped_column(ForeignKey("collections.id"), nullable=False, index=True)
    collection_type_id: Mapped[int] = mapped_column(ForeignKey("collection_types.id"), nullable=False, index=True)
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    # The value as first entered/imported, preserved even if `amount` is
    # later edited during the Step-3 preview or during review — an audit
    # trail distinct from the (possibly corrected) effective `amount`.
    original_amount: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    notes: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    collection: Mapped["Collection"] = relationship(back_populates="items")
    collection_type: Mapped["CollectionType"] = relationship()


class ReceiptStatus(str, enum.Enum):
    issued = "issued"
    reprinted = "reprinted"
    cancelled = "cancelled"
    corrected = "corrected"


class Receipt(Base):
    __tablename__ = "receipts"
    id: Mapped[int] = mapped_column(primary_key=True)
    # Not unique — a correction that reissues a receipt adds a new row for
    # the same collection rather than replacing the old one (see
    # Collection.receipts / Collection.current_receipt).
    collection_id: Mapped[int] = mapped_column(ForeignKey("collections.id"), nullable=False, index=True)
    receipt_number: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    # Nullable — an anonymous collection has no member to attribute the
    # receipt to; the record still exists for accounting purposes.
    member_id: Mapped[int | None] = mapped_column(ForeignKey("members.id"), nullable=True)
    church_id: Mapped[int] = mapped_column(ForeignKey("churches.id"), nullable=False)
    total_amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    issued_by_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    issued_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    status: Mapped[ReceiptStatus] = mapped_column(Enum(ReceiptStatus, name="receipt_status"), nullable=False, default=ReceiptStatus.issued)
    # A persisted snapshot of the member's preferred channel at issuance time
    # — no real send happens (no outbound email/SMS/WhatsApp integration
    # exists anywhere in this app); delivery_status stays a manually-set
    # marker, never an automated delivery confirmation.
    delivery_channel: Mapped[str | None] = mapped_column(String(40), nullable=True)
    delivery_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    reprint_count: Mapped[int] = mapped_column(default=0, nullable=False)
    cancelled_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)
    cancelled_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # Set on the OLD receipt once a correction issues a replacement — points
    # forward to the new one. use_alter=True: same self-referential-FK
    # workaround already used by Family.leader_id.
    superseded_by_receipt_id: Mapped[int | None] = mapped_column(ForeignKey("receipts.id", use_alter=True), nullable=True)

    collection: Mapped["Collection"] = relationship(back_populates="receipts")
    member: Mapped["Member | None"] = relationship()
    church: Mapped["Church"] = relationship()
    issued_by: Mapped["User"] = relationship(foreign_keys=[issued_by_user_id])
    cancelled_by: Mapped["User | None"] = relationship(foreign_keys=[cancelled_by_user_id])
    superseded_by: Mapped["Receipt | None"] = relationship(remote_side=[id], foreign_keys=[superseded_by_receipt_id], post_update=True)

    @property
    def member_name(self) -> str | None:
        if self.collection is not None and self.collection.is_anonymous:
            return "Anonymous"
        return f"{self.member.first_name} {self.member.last_name}" if self.member is not None else None

    @property
    def issued_by_username(self) -> str | None:
        return self.issued_by.username if self.issued_by is not None else None

    @property
    def cancelled_by_username(self) -> str | None:
        return self.cancelled_by.username if self.cancelled_by is not None else None


# ---------------------------------------------------------------------------
# Collections — audit trail, corrections, and receipt-series (ToR Module 2:
# Treasury Management upgrade, Phase A). Tamper-evidence for the audit log is
# achieved structurally: no update/delete endpoint is ever exposed for it.
# ---------------------------------------------------------------------------


class CollectionAuditAction(str, enum.Enum):
    collection_created = "collection_created"
    collection_approved = "collection_approved"
    collection_rejected = "collection_rejected"
    correction_requested = "correction_requested"
    correction_approved = "correction_approved"
    correction_rejected = "correction_rejected"
    group_approved_all = "group_approved_all"
    receipt_issued = "receipt_issued"
    receipt_reprinted = "receipt_reprinted"
    receipt_cancelled = "receipt_cancelled"
    receipt_corrected = "receipt_corrected"
    collection_verified = "collection_verified"
    collection_flagged = "collection_flagged"
    reconciliation_matched = "reconciliation_matched"
    period_closed = "period_closed"
    period_reopened = "period_reopened"


class CollectionAuditLog(Base):
    """Append-only trail for every treasury mutation — mirrors MemberAuditLog's
    shape, extended with before/after value capture (ToR: "original and
    revised values") which MemberAuditLog doesn't need."""
    __tablename__ = "collection_audit_logs"
    id: Mapped[int] = mapped_column(primary_key=True)
    collection_id: Mapped[int | None] = mapped_column(ForeignKey("collections.id"), nullable=True, index=True)
    collection_group_id: Mapped[int | None] = mapped_column(ForeignKey("collection_groups.id"), nullable=True, index=True)
    receipt_id: Mapped[int | None] = mapped_column(ForeignKey("receipts.id"), nullable=True, index=True)
    church_id: Mapped[int] = mapped_column(ForeignKey("churches.id"), nullable=False, index=True)
    action: Mapped[CollectionAuditAction] = mapped_column(Enum(CollectionAuditAction, name="collection_audit_action"), nullable=False, index=True)
    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    reason: Mapped[str | None] = mapped_column(String(500), nullable=True)
    before_value: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    after_value: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # manual/import/bank_statement/mobile_money_statement/member_portal
    source: Mapped[str | None] = mapped_column(String(40), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), index=True)

    actor: Mapped["User | None"] = relationship()

    @property
    def actor_username(self) -> str | None:
        return self.actor.username if self.actor is not None else None


class CollectionCorrectionKind(str, enum.Enum):
    amount_adjustment = "amount_adjustment"
    reversal = "reversal"


class CollectionCorrectionStatus(str, enum.Enum):
    pending = "pending"
    approved = "approved"
    rejected = "rejected"


class CollectionCorrection(Base):
    """A requested change to an already-approved Collection — preserves
    history (via before/after values on the resulting audit-log row) rather
    than silently overwriting, and is maker-checker enforced: the decider
    may never be the same user as the requester (see collection_service)."""
    __tablename__ = "collection_corrections"
    id: Mapped[int] = mapped_column(primary_key=True)
    collection_id: Mapped[int] = mapped_column(ForeignKey("collections.id"), nullable=False, index=True)
    kind: Mapped[CollectionCorrectionKind] = mapped_column(Enum(CollectionCorrectionKind, name="collection_correction_kind"), nullable=False)
    requested_by_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    reason: Mapped[str] = mapped_column(String(500), nullable=False)
    # [{collection_type_id, amount}] — proposed post-correction item set; a
    # reversal proposes an empty/zeroed set.
    proposed_items: Mapped[list] = mapped_column(JSON, nullable=False)
    status: Mapped[CollectionCorrectionStatus] = mapped_column(
        Enum(CollectionCorrectionStatus, name="collection_correction_status"), nullable=False, default=CollectionCorrectionStatus.pending, index=True
    )
    decided_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    decision_notes: Mapped[str | None] = mapped_column(String(500), nullable=True)

    collection: Mapped["Collection"] = relationship()
    requested_by: Mapped["User"] = relationship(foreign_keys=[requested_by_user_id])
    decided_by: Mapped["User | None"] = relationship(foreign_keys=[decided_by_user_id])

    @property
    def requested_by_username(self) -> str | None:
        return self.requested_by.username if self.requested_by is not None else None

    @property
    def decided_by_username(self) -> str | None:
        return self.decided_by.username if self.decided_by is not None else None


class ReceiptSeries(Base):
    """A real, persisted, row-locked numbering register per church — replaces
    the previous derived-COUNT scheme (see receipt_service.generate_receipt_number),
    fixing its documented concurrency gap as a byproduct.

    `range_start`/`range_end` mirror a real pre-numbered physical receipt
    booklet — `next_sequence` starts at `range_start` and is never allowed
    past `range_end`; once it is, the series is exhausted and a new one
    must be created. `range_end` is nullable only for series that predate
    this feature (auto-created, open-ended) — every series created through
    the admin UI going forward always declares both ends of its range."""
    __tablename__ = "receipt_series"
    id: Mapped[int] = mapped_column(primary_key=True)
    church_id: Mapped[int] = mapped_column(ForeignKey("churches.id"), nullable=False, index=True)
    prefix: Mapped[str] = mapped_column(String(30), nullable=False)
    range_start: Mapped[int] = mapped_column(default=1, nullable=False)
    range_end: Mapped[int | None] = mapped_column(nullable=True)
    next_sequence: Mapped[int] = mapped_column(default=1, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    church: Mapped["Church"] = relationship()

    @property
    def remaining_count(self) -> int | None:
        """None means unbounded (a legacy, range-less series)."""
        if self.range_end is None:
            return None
        return max(0, self.range_end - self.next_sequence + 1)

    @property
    def is_exhausted(self) -> bool:
        return self.range_end is not None and self.next_sequence > self.range_end


# ---------------------------------------------------------------------------
# RBAC
# ---------------------------------------------------------------------------


class Role(Base):
    __tablename__ = "roles"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(60), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)


class Permission(Base):
    __tablename__ = "permissions"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    description: Mapped[str | None] = mapped_column(String(500), nullable=True)


class RolePermission(Base):
    __tablename__ = "role_permissions"
    __table_args__ = (UniqueConstraint("role_id", "permission_id", name="uq_role_permission"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    role_id: Mapped[int] = mapped_column(ForeignKey("roles.id"), nullable=False, index=True)
    permission_id: Mapped[int] = mapped_column(ForeignKey("permissions.id"), nullable=False, index=True)
    is_allowed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    role: Mapped["Role"] = relationship()
    permission: Mapped["Permission"] = relationship()


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(150), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    member_id: Mapped[int | None] = mapped_column(ForeignKey("members.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    member: Mapped["Member | None"] = relationship()
    user_roles: Mapped[list["UserRole"]] = relationship(back_populates="user", cascade="all, delete-orphan")


class UserRole(Base):
    __tablename__ = "user_roles"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)
    role_id: Mapped[int] = mapped_column(ForeignKey("roles.id"), nullable=False, index=True)
    church_id: Mapped[int | None] = mapped_column(ForeignKey("churches.id"), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    user: Mapped["User"] = relationship(back_populates="user_roles")
    role: Mapped["Role"] = relationship()
    church: Mapped["Church | None"] = relationship()
