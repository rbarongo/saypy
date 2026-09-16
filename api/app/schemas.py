from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from app.models import (
    CollectionAuditAction,
    CollectionCorrectionKind,
    CollectionCorrectionStatus,
    CollectionEntryMethod,
    CollectionGroupStatus,
    CollectionSource,
    CollectionStatus,
    CollectionTypeStatus,
    CommunicationCategory,
    CommunicationChannel,
    CorrectionRequestStatus,
    FeedbackStatus,
    Gender,
    LookupStatus,
    ReceiptStatus,
    RegistrationStatus,
)

# ---------------------------------------------------------------------------
# Generic lookup {code, name} resources — household roles, membership
# categories, member statuses, nationalities, contact types.
# ---------------------------------------------------------------------------


class LookupCreate(BaseModel):
    code: str = Field(min_length=1, max_length=60)
    name: str = Field(min_length=1, max_length=150)


class LookupUpdate(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None


class LookupRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    code: str
    name: str


class NationalityCreate(LookupCreate):
    dial_code: Optional[str] = None


class NationalityUpdate(LookupUpdate):
    dial_code: Optional[str] = None


class NationalityRead(LookupRead):
    dial_code: Optional[str] = None


# ---------------------------------------------------------------------------
# Church
# ---------------------------------------------------------------------------


class ChurchCreate(BaseModel):
    code: str
    name: str
    app_name: Optional[str] = None


class ChurchUpdate(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    app_name: Optional[str] = None


class ChurchRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    code: str
    name: str
    app_name: Optional[str] = None
    created_at: datetime


# ---------------------------------------------------------------------------
# Family
# ---------------------------------------------------------------------------


class FamilyCreate(BaseModel):
    name: str
    alias: Optional[str] = None
    leader_id: Optional[int] = None
    church_id: int


class FamilyUpdate(BaseModel):
    name: Optional[str] = None
    alias: Optional[str] = None
    leader_id: Optional[int] = None


class FamilyRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    alias: Optional[str] = None
    leader_id: Optional[int] = None
    leader_name: Optional[str] = None
    member_count: int = 0
    church_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None


class FamilyMemberSummary(BaseModel):
    id: int
    first_name: str
    last_name: str
    household_role: LookupRead


class FamilyRoster(BaseModel):
    family: FamilyRead
    leader: Optional[FamilyMemberSummary] = None
    members: list[FamilyMemberSummary]


# ---------------------------------------------------------------------------
# Church-scoped lookups shared shape — Organisations, Sabbath School Groups,
# Church Roles all share {id, code, name, church_id, status}, same as
# CollectionType; reused via app/routers/church_scoped_lookup.py.
# ---------------------------------------------------------------------------


class ChurchScopedLookupCreate(BaseModel):
    code: str = Field(min_length=1, max_length=60)
    name: str = Field(min_length=1, max_length=150)
    church_id: Optional[int] = None
    status: LookupStatus = LookupStatus.active


class ChurchScopedLookupUpdate(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    status: Optional[LookupStatus] = None


class ChurchScopedLookupRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    code: str
    name: str
    church_id: Optional[int] = None
    status: LookupStatus
    created_at: datetime


# ---------------------------------------------------------------------------
# Member + contacts
# ---------------------------------------------------------------------------


class MemberContactCreate(BaseModel):
    contact_type_id: int
    contact: str
    contact_init: bool = False


class MemberContactUpdate(BaseModel):
    contact_type_id: Optional[int] = None
    contact: Optional[str] = None
    contact_init: Optional[bool] = None


class MemberContactRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    member_id: int
    contact_type_id: int
    contact_type: LookupRead
    contact: str
    contact_init: bool
    created_at: datetime


class MemberBase(BaseModel):
    first_name: str
    middle_name: Optional[str] = None
    last_name: str
    gender: Optional[Gender] = None
    date_of_birth: Optional[date] = None
    household_role_id: int
    membership_category_id: int
    status_id: int
    family_id: Optional[int] = None
    church_id: int
    national_id: Optional[str] = None
    nationality_id: Optional[int] = None
    address: Optional[str] = None
    occupation: Optional[str] = None
    join_date: Optional[date] = None
    notes: Optional[str] = None
    sabbath_school_group_id: Optional[int] = None
    organisation_ids: list[int] = []
    church_role_ids: list[int] = []
    preferred_channels: list[CommunicationChannel] = []


class MemberCreate(MemberBase):
    contacts: list[MemberContactCreate] = []
    # Assisted registration by a lower-trust encoder: when true, the member
    # is saved as `pending` instead of immediately `approved` and must go
    # through /members/{id}/approve-registration or reject-registration.
    submit_for_review: bool = False
    # Transfer-in provenance — who they're arriving from, if applicable.
    transferred_from_church_id: Optional[int] = None
    transfer_date: Optional[date] = None
    # Set by the frontend once the operator has seen and dismissed a
    # duplicate warning from POST /members/check-duplicates.
    force_despite_duplicates: bool = False


class MemberUpdate(BaseModel):
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    gender: Optional[Gender] = None
    date_of_birth: Optional[date] = None
    household_role_id: Optional[int] = None
    membership_category_id: Optional[int] = None
    status_id: Optional[int] = None
    family_id: Optional[int] = None
    church_id: Optional[int] = None
    national_id: Optional[str] = None
    nationality_id: Optional[int] = None
    address: Optional[str] = None
    occupation: Optional[str] = None
    join_date: Optional[date] = None
    notes: Optional[str] = None
    sabbath_school_group_id: Optional[int] = None
    organisation_ids: Optional[list[int]] = None
    church_role_ids: Optional[list[int]] = None
    preferred_channels: Optional[list[CommunicationChannel]] = None


class MemberRead(MemberBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    household_role: LookupRead
    membership_category: LookupRead
    status: LookupRead
    nationality: Optional[NationalityRead] = None
    family_name: Optional[str] = None
    contacts: list[MemberContactRead] = []
    created_at: datetime
    updated_at: Optional[datetime] = None
    # Only ever populated in the POST /members response, right after the
    # member's login account is provisioned — never persisted or re-shown.
    created_username: Optional[str] = None
    created_password: Optional[str] = None
    member_number: str
    registration_status: RegistrationStatus
    sabbath_school_group_name: Optional[str] = None
    organisation_names: list[str] = []
    church_role_names: list[str] = []
    transferred_from_church_id: Optional[int] = None
    transfer_out_church_id: Optional[int] = None
    transfer_date: Optional[date] = None
    transfer_reason: Optional[str] = None
    # Full per-category breakdown (see CommunicationCategory) — `preferred_channels`
    # above (inherited from MemberBase) stays the flat 'general'-only view for
    # backward compatibility with the admin MemberForm's checkbox group.
    communication_preferences: dict[str, list[CommunicationChannel]] = Field(default_factory=dict, validation_alias="communication_preferences_by_category")


class MemberListResponse(BaseModel):
    items: list[MemberRead]
    total: int
    page: int
    page_size: int


class MemberDuplicateCheckRequest(BaseModel):
    church_id: int
    first_name: str
    last_name: str
    date_of_birth: Optional[date] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    exclude_member_id: Optional[int] = None


class MemberDuplicateCandidate(BaseModel):
    member_id: int
    member_name: str
    match_reason: str  # 'phone' | 'email' | 'name_and_dob'


class MemberDuplicateCheckResponse(BaseModel):
    candidates: list[MemberDuplicateCandidate] = []


class MemberTransferOutRequest(BaseModel):
    to_church_id: int
    transfer_date: date
    reason: Optional[str] = None


class MemberRegistrationRejectRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=500)


class MemberRegistrationApproveRequest(BaseModel):
    # Optional — when omitted, a random temporary password is generated as
    # before (see member_service.provision_member_user_account).
    password: Optional[str] = Field(None, min_length=6, max_length=100)


class MemberAuditLogRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    action: str
    actor_user_id: Optional[int] = None
    actor_username: Optional[str] = None
    details: Optional[str] = None
    created_at: datetime


# ---------------------------------------------------------------------------
# RBAC
# ---------------------------------------------------------------------------


class RoleCreate(BaseModel):
    code: str
    name: str


class RoleUpdate(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None


class RoleRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    code: str
    name: str


class PermissionCreate(BaseModel):
    code: str
    name: str
    description: Optional[str] = None


class PermissionUpdate(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None


class PermissionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    code: str
    name: str
    description: Optional[str] = None


class RolePermissionRead(BaseModel):
    permission_id: int
    code: str
    name: str
    description: Optional[str] = None
    is_allowed: bool


class RolePermissionSetRequest(BaseModel):
    is_allowed: bool


class UserRoleCreate(BaseModel):
    role_id: int
    church_id: Optional[int] = None


class UserRoleUpdate(BaseModel):
    is_active: Optional[bool] = None
    church_id: Optional[int] = None


class UserRoleRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    user_id: int
    role_id: int
    role: RoleRead
    church_id: Optional[int] = None
    is_active: bool


class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=150)
    password: str = Field(min_length=6)
    email: Optional[EmailStr] = None
    member_id: Optional[int] = None


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    is_active: Optional[bool] = None
    member_id: Optional[int] = None


class PasswordResetRequest(BaseModel):
    new_password: str = Field(min_length=6)


class UserRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    username: str
    email: Optional[str] = None
    is_active: bool
    member_id: Optional[int] = None
    created_at: datetime
    last_login_at: Optional[datetime] = None
    # The ORM relationship is named `user_roles` (matches the table); `validation_alias`
    # reads from that attribute while the public API field stays the nicer `roles`.
    roles: list[UserRoleRead] = Field(default_factory=list, validation_alias="user_roles")


class MyPermissionsRead(BaseModel):
    # permission code -> list of granted church ids; `null` in the list means
    # a global grant (every church) for that permission — mirrors
    # rbac_service.PermissionScopes, JSON-safe (sets aren't).
    scopes: dict[str, list[Optional[int]]]


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserRead


# ---------------------------------------------------------------------------
# Dashboard stats
# ---------------------------------------------------------------------------


class StatBucket(BaseModel):
    code: Optional[str] = None
    name: str
    count: int


class TrendPoint(BaseModel):
    day: date
    count: int


class ContactCompleteness(BaseModel):
    total: int
    with_phone: int
    with_email: int
    without_any_contact: int


class MonthPoint(BaseModel):
    month: str  # 'YYYY-MM'
    count: int
    cumulative: int


class StatsOverview(BaseModel):
    members_total: int
    members_by_status: list[StatBucket]
    members_by_household_role: list[StatBucket]
    members_by_membership_category: list[StatBucket]
    members_by_church: list[StatBucket]
    members_recent_trend: list[TrendPoint]
    families_total: int
    churches_total: int
    users_total: int
    roles_total: int
    members_by_registration_status: list[StatBucket] = []
    members_transferred_count: int = 0
    contact_completeness: Optional[ContactCompleteness] = None
    members_growth_monthly: list[MonthPoint] = []


# ---------------------------------------------------------------------------
# Member management reports — one shared envelope for all of them (see
# routers/reports.py). `rows` are flat, display-ready dicts keyed by each
# column's `key`, so the frontend can render/export any of the 7 reports
# through one generic table + export path with no per-report logic.
# ---------------------------------------------------------------------------


class ReportColumn(BaseModel):
    key: str
    label: str
    numeric: bool = False


class ReportResult(BaseModel):
    columns: list[ReportColumn]
    rows: list[dict[str, Any]]
    summary: dict[str, Any] = {}
    total: int = 0
    generated_at: datetime


# ---------------------------------------------------------------------------
# Collections
# ---------------------------------------------------------------------------


class CollectionTypeCreate(BaseModel):
    code: str = Field(min_length=1, max_length=60)
    name: str = Field(min_length=1, max_length=150)
    church_id: Optional[int] = None
    status: CollectionTypeStatus = CollectionTypeStatus.active


class CollectionTypeUpdate(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    status: Optional[CollectionTypeStatus] = None


class CollectionTypeRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    code: str
    name: str
    church_id: Optional[int] = None
    status: CollectionTypeStatus
    created_at: datetime


class CollectionItemCreate(BaseModel):
    collection_type_id: int
    amount: float = Field(gt=0)
    original_amount: Optional[float] = None
    notes: Optional[str] = None


class CollectionItemRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    collection_type_id: int
    collection_type: CollectionTypeRead
    amount: float
    original_amount: Optional[float] = None
    notes: Optional[str] = None


class CollectionCreate(BaseModel):
    # Optional — a truly anonymous contribution has neither. Validated
    # server-side: exactly one of (member_id set) / (is_anonymous=True) must
    # hold (see the collections router).
    member_id: Optional[int] = None
    is_anonymous: bool = False
    church_id: Optional[int] = None
    collection_date: date
    collection_source: CollectionSource = CollectionSource.cash
    # Bank/mobile-money transaction reference — matched during reconciliation.
    payment_reference: Optional[str] = Field(None, max_length=120)
    notes: Optional[str] = None
    items: list[CollectionItemCreate] = Field(min_length=1)


class CollectionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    collection_group_id: int
    member_id: Optional[int] = None
    member_name: Optional[str] = None
    is_anonymous: bool = False
    payment_reference: Optional[str] = None
    evidence_file_path: Optional[str] = None
    church_id: int
    total_amount: float
    collection_source: CollectionSource
    entry_method: CollectionEntryMethod
    collection_date: date
    status: CollectionStatus
    rejection_reason: Optional[str] = None
    receipt_number: Optional[str] = None
    notes: Optional[str] = None
    items: list[CollectionItemRead] = []
    created_at: datetime
    updated_at: Optional[datetime] = None
    # Digital Treasurer agent submissions only (entry_method=agent) — the
    # full raw JSON the agent produced, for the reviewer to inspect.
    agent_payload: Optional[dict[str, Any]] = None


class AgentReceiptItem(BaseModel):
    name: str
    amount: float
    currency: Optional[str] = None
    category: Optional[str] = None


class AgentCollectionSubmitRequest(BaseModel):
    """Matches the JSON produced by the Digital Treasurer receipt-reading
    agent (photo/slip/bank or mobile-money notification -> structured data).
    `church_id` isn't part of the agent's own output — it only sees a
    free-text `payee` church name, unreliable to resolve automatically — so
    the integration calling this endpoint supplies it directly, since it
    already knows which church's channel the receipt came in on."""
    church_id: int
    valid: bool = True
    receipt_type: Optional[str] = None
    items: list[AgentReceiptItem] = Field(default_factory=list)
    total_amount: Optional[float] = None
    currency: Optional[str] = None
    payer: Optional[str] = None
    name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    phone_or_email: Optional[str] = None
    payee: Optional[str] = None
    reference_number: Optional[str] = None
    transaction_date: Optional[str] = None
    payment_channel: Optional[str] = None
    description_matches_receipt: Optional[bool] = None
    validation_issues: list[str] = Field(default_factory=list)
    summary: Optional[str] = None
    reconciliation: Optional[bool] = None
    receipt_number: Optional[str] = None
    submission_channel: Optional[str] = None
    user_feedback: Optional[bool] = None
    sender: Optional[str] = None
    account_type: Optional[str] = None


class CollectionListResponse(BaseModel):
    items: list[CollectionRead]
    total: int
    page: int
    page_size: int


class CollectionRejectRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=500)


class CollectionGroupRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    code: str
    name: Optional[str] = None
    church_id: int
    status: CollectionGroupStatus
    initiator_user_id: int
    initiator_username: Optional[str] = None
    initiated_at: datetime
    approver_user_id: Optional[int] = None
    approver_username: Optional[str] = None
    approved_at: Optional[datetime] = None
    collection_count: int = 0
    total_amount: float = 0


class CollectionGroupDetail(CollectionGroupRead):
    collections: list[CollectionRead] = []


class ReceiptRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    collection_id: int
    receipt_number: str
    member_id: Optional[int] = None
    member_name: Optional[str] = None
    church_id: int
    total_amount: float
    issued_by_user_id: int
    issued_by_username: Optional[str] = None
    issued_at: datetime
    status: ReceiptStatus = ReceiptStatus.issued
    delivery_channel: Optional[str] = None
    delivery_status: str = "pending"
    reprint_count: int = 0
    cancelled_reason: Optional[str] = None
    cancelled_by_user_id: Optional[int] = None
    cancelled_by_username: Optional[str] = None
    cancelled_at: Optional[datetime] = None
    superseded_by_receipt_id: Optional[int] = None
    collection: CollectionRead


class ReceiptCancelRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=500)


# ---- Collection audit trail ------------------------------------------------


class CollectionAuditLogRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    collection_id: Optional[int] = None
    collection_group_id: Optional[int] = None
    receipt_id: Optional[int] = None
    church_id: int
    action: CollectionAuditAction
    actor_user_id: Optional[int] = None
    actor_username: Optional[str] = None
    reason: Optional[str] = None
    before_value: Optional[dict[str, Any]] = None
    after_value: Optional[dict[str, Any]] = None
    source: Optional[str] = None
    created_at: datetime


class CollectionAuditLogListResponse(BaseModel):
    items: list[CollectionAuditLogRead]
    total: int
    page: int
    page_size: int


# ---- Correction / adjustment workflow --------------------------------------


class CollectionCorrectionItem(BaseModel):
    collection_type_id: int
    amount: float = Field(ge=0)


class CollectionCorrectionCreate(BaseModel):
    kind: CollectionCorrectionKind
    reason: str = Field(min_length=1, max_length=500)
    # A reversal proposes an empty/zeroed list; an amount_adjustment proposes
    # the full corrected item set (not a delta).
    proposed_items: list[CollectionCorrectionItem] = []


class CollectionCorrectionDecision(BaseModel):
    approve: bool
    decision_notes: Optional[str] = Field(None, max_length=500)


class CollectionCorrectionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    collection_id: int
    kind: CollectionCorrectionKind
    requested_by_user_id: int
    requested_by_username: Optional[str] = None
    requested_at: datetime
    reason: str
    proposed_items: list[CollectionCorrectionItem]
    status: CollectionCorrectionStatus
    decided_by_user_id: Optional[int] = None
    decided_by_username: Optional[str] = None
    decided_at: Optional[datetime] = None
    decision_notes: Optional[str] = None


# ---- Receipt series (numbering register) -----------------------------------


class ReceiptSeriesCreate(BaseModel):
    church_id: int
    prefix: str = Field(min_length=1, max_length=30)
    # Mirrors a real pre-numbered physical receipt booklet — numbering
    # begins at range_start and may never exceed range_end.
    range_start: int = Field(ge=1)
    range_end: int = Field(ge=1)


class ReceiptSeriesUpdate(BaseModel):
    prefix: Optional[str] = Field(None, min_length=1, max_length=30)
    is_active: Optional[bool] = None
    # Extends the range only — must be >= the current range_end and >=
    # whatever has already been issued; range_start/next_sequence are never
    # directly editable (that would let numbers be skipped or reused).
    range_end: Optional[int] = Field(None, ge=1)


class ReceiptSeriesRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    church_id: int
    prefix: str
    range_start: int
    range_end: Optional[int] = None
    next_sequence: int
    remaining_count: Optional[int] = None
    is_exhausted: bool = False
    is_active: bool
    created_by_user_id: Optional[int] = None
    created_at: datetime


# ---- Bulk import wizard ---------------------------------------------------


class ImportInspectResponse(BaseModel):
    headers: list[str]
    rows: list[dict[str, Any]]


class ImportResolveRequest(BaseModel):
    church_id: int
    rows: list[dict[str, Any]]
    # header -> 'name' | 'phone' | 'email'
    identification_mapping: dict[str, str]
    # header -> collection_type_id
    collection_mapping: dict[str, int]


class ResolvedImportItem(BaseModel):
    collection_type_id: int
    amount: float


class ResolvedImportRow(BaseModel):
    row_index: int
    name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    member_id: Optional[int] = None
    member_name: Optional[str] = None
    match_confidence: str = "unmatched"  # 'phone' | 'email' | 'name' | 'unmatched'
    items: list[ResolvedImportItem] = []
    total_amount: float = 0


class ImportResolveResponse(BaseModel):
    rows: list[ResolvedImportRow]


class ImportCommitItem(BaseModel):
    collection_type_id: int
    amount: float = Field(gt=0)
    original_amount: Optional[float] = None


class ImportCommitRow(BaseModel):
    member_id: int
    total_amount: float
    items: list[ImportCommitItem] = Field(min_length=1)
    notes: Optional[str] = None


class ImportCommitRequest(BaseModel):
    church_id: int
    group_name: Optional[str] = None
    collection_date: date
    collection_source: CollectionSource = CollectionSource.cash
    rows: list[ImportCommitRow] = Field(min_length=1)


# ---------------------------------------------------------------------------
# Member self-service portal — public self-registration, and the
# authenticated /me endpoints (profile, preferences, corrections, feedback).
# ---------------------------------------------------------------------------


class SelfRegisterRequest(BaseModel):
    church_id: int
    first_name: str
    middle_name: Optional[str] = None
    last_name: str
    gender: Optional[Gender] = None
    date_of_birth: Optional[date] = None
    household_role_id: int
    nationality_id: Optional[int] = None
    address: Optional[str] = None
    occupation: Optional[str] = None
    contacts: list[MemberContactCreate] = []
    # A visitor doesn't get to declare their own membership standing —
    # membership_category_id/status_id are resolved server-side, not
    # accepted here (see member_service.self_register).
    force_despite_duplicates: bool = False


class SelfRegisterOptionsResponse(BaseModel):
    churches: list[ChurchRead]
    household_roles: list[LookupRead]
    nationalities: list[NationalityRead]
    contact_types: list[LookupRead]


class MemberPreferencesUpdate(BaseModel):
    # Per-category channel selection, e.g. {"offering_receipts": ["sms"],
    # "church_announcements": ["email", "whatsapp"]}. A category omitted from
    # the payload is cleared — the caller should send its full current state,
    # same wipe-and-replace convention as organisation/church-role links.
    preferences: dict[CommunicationCategory, list[CommunicationChannel]] = {}


class CorrectionRequestCreate(BaseModel):
    requested_changes: dict[str, Any]
    reason: Optional[str] = None


class CorrectionRequestRejectRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=500)


class CorrectionRequestRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    member_id: int
    member_name: Optional[str] = None
    requested_changes: dict[str, Any]
    reason: Optional[str] = None
    status: CorrectionRequestStatus
    reviewed_by_user_id: Optional[int] = None
    reviewed_by_username: Optional[str] = None
    review_note: Optional[str] = None
    created_at: datetime
    decided_at: Optional[datetime] = None


class FeedbackCreate(BaseModel):
    subject: str = Field(min_length=1, max_length=200)
    message: str = Field(min_length=1, max_length=2000)


class FeedbackRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    member_id: int
    member_name: Optional[str] = None
    subject: str
    message: str
    status: FeedbackStatus
    created_at: datetime
    reviewed_at: Optional[datetime] = None
