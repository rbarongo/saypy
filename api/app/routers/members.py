from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import church_scope_for, enforce_church_scope, get_current_member, get_current_user, get_permission_scopes, require_permission
from app.models import Church, ContactType, HouseholdRole, Member, MemberCorrectionRequest, MemberFeedback, Nationality, RegistrationStatus, User
from app.schemas import (
    ChurchRead,
    CorrectionRequestCreate,
    CorrectionRequestRead,
    CorrectionRequestRejectRequest,
    FeedbackCreate,
    FeedbackRead,
    LookupRead,
    MemberAuditLogRead,
    MemberContactCreate,
    MemberContactRead,
    MemberContactUpdate,
    MemberCreate,
    MemberDuplicateCandidate,
    MemberDuplicateCheckRequest,
    MemberDuplicateCheckResponse,
    MemberListResponse,
    MemberPreferencesUpdate,
    MemberRead,
    MemberRegistrationApproveRequest,
    MemberRegistrationRejectRequest,
    MemberTransferOutRequest,
    MemberUpdate,
    NationalityRead,
    SelfRegisterOptionsResponse,
    SelfRegisterRequest,
)
from app.services import member_service
from app.services.rbac_service import PermissionScopes, accessible_churches, has_permission

router = APIRouter(prefix="/members", tags=["members"])

WRITE_SCOPE = church_scope_for("manage_members")
REVIEW_SCOPE = church_scope_for("review_members")


READ_PERMISSIONS = ("manage_members", "view_members", "review_members")


def _read_scope(scopes: PermissionScopes = Depends(get_permission_scopes)) -> Optional[set[int]]:
    """A member is readable if the caller has manage_members, view_members,
    or review_members for its church; the readable church set is the union."""
    per_permission = [accessible_churches(scopes, code) for code in READ_PERMISSIONS]
    if any(s is None for s in per_permission):
        return None
    union: set[int] = set()
    for s in per_permission:
        union |= s
    return union


def _require_any_read_permission(scopes: PermissionScopes = Depends(get_permission_scopes)) -> PermissionScopes:
    if not any(has_permission(scopes, code) for code in READ_PERMISSIONS):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Missing required permission: view_members")
    return scopes


# ---------------------------------------------------------------------------
# Public self-registration — no auth. Always lands as a pending assisted
# registration (see member_service.self_register); the applicant's own
# login account isn't provisioned until someone with `review_members`
# approves it, same as the staff form's "submit for review" path.
# ---------------------------------------------------------------------------


@router.get("/self-register/options", response_model=SelfRegisterOptionsResponse)
def self_register_options(db: Session = Depends(get_db)):
    return SelfRegisterOptionsResponse(
        churches=db.query(Church).order_by(Church.name.asc()).all(),
        household_roles=db.query(HouseholdRole).order_by(HouseholdRole.name.asc()).all(),
        nationalities=db.query(Nationality).order_by(Nationality.name.asc()).all(),
        contact_types=db.query(ContactType).order_by(ContactType.name.asc()).all(),
    )


@router.post("/self-register", response_model=MemberRead, status_code=status.HTTP_201_CREATED)
def self_register(payload: SelfRegisterRequest, db: Session = Depends(get_db)):
    return member_service.self_register(db, payload, payload.church_id)


# ---------------------------------------------------------------------------
# Self-service `/me` endpoints — a member acting on their own record. No
# RBAC permission check (see app.deps.get_current_member): a member is
# always allowed to see/act on themselves.
# ---------------------------------------------------------------------------


@router.get("/me", response_model=MemberRead)
def get_my_member(member=Depends(get_current_member), db: Session = Depends(get_db)):
    return member_service.get_member_or_404(db, member.id)


@router.put("/me/preferences", response_model=MemberRead)
def update_my_preferences(payload: MemberPreferencesUpdate, member=Depends(get_current_member), db: Session = Depends(get_db)):
    member_service.set_communication_preferences_by_category(db, member.id, payload.preferences)
    db.commit()
    return member_service.get_member_or_404(db, member.id)


@router.post("/me/correction-requests", response_model=CorrectionRequestRead, status_code=status.HTTP_201_CREATED)
def submit_my_correction_request(payload: CorrectionRequestCreate, member=Depends(get_current_member), db: Session = Depends(get_db)):
    return member_service.create_correction_request(db, member, payload)


@router.get("/me/correction-requests", response_model=list[CorrectionRequestRead])
def list_my_correction_requests(member=Depends(get_current_member), db: Session = Depends(get_db)):
    return (
        db.query(MemberCorrectionRequest)
        .filter(MemberCorrectionRequest.member_id == member.id)
        .order_by(MemberCorrectionRequest.created_at.desc())
        .all()
    )


@router.post("/me/feedback", response_model=FeedbackRead, status_code=status.HTTP_201_CREATED)
def submit_my_feedback(payload: FeedbackCreate, member=Depends(get_current_member), db: Session = Depends(get_db)):
    return member_service.create_feedback(db, member, payload)


@router.get("/me/feedback", response_model=list[FeedbackRead])
def list_my_feedback(member=Depends(get_current_member), db: Session = Depends(get_db)):
    return db.query(MemberFeedback).filter(MemberFeedback.member_id == member.id).order_by(MemberFeedback.created_at.desc()).all()


# ---------------------------------------------------------------------------
# Staff review queue for correction requests — reuses the same
# manage_members/review_members permission split as registration review.
# ---------------------------------------------------------------------------


@router.get("/correction-requests", response_model=list[CorrectionRequestRead])
def list_correction_requests(
    status_filter: Optional[str] = Query(None, alias="status"),
    db: Session = Depends(get_db),
    _perm=Depends(_require_any_read_permission),
):
    query = db.query(MemberCorrectionRequest)
    if status_filter:
        query = query.filter(MemberCorrectionRequest.status == status_filter)
    return query.order_by(MemberCorrectionRequest.created_at.desc()).all()


@router.post("/correction-requests/{request_id}/approve", response_model=CorrectionRequestRead)
def approve_correction_request(
    request_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("review_members")),
):
    request = member_service.get_correction_request_or_404(db, request_id)
    return member_service.apply_correction_request(db, request, actor_user_id=user.id)


@router.post("/correction-requests/{request_id}/reject", response_model=CorrectionRequestRead)
def reject_correction_request(
    request_id: int,
    payload: CorrectionRequestRejectRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("review_members")),
):
    request = member_service.get_correction_request_or_404(db, request_id)
    return member_service.reject_correction_request(db, request, actor_user_id=user.id, reason=payload.reason)


@router.get("", response_model=MemberListResponse)
def list_members(
    church_id: Optional[int] = None,
    family_id: Optional[int] = None,
    q: Optional[str] = None,
    household_role_id: Optional[int] = None,
    membership_category_id: Optional[int] = None,
    status_id: Optional[int] = None,
    registration_status: Optional[RegistrationStatus] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    sort: str = Query("last_name"),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    query = db.query(Member)
    if scope is not None:
        if church_id is not None:
            query = query.filter(Member.church_id == church_id if church_id in scope else False)
        else:
            query = query.filter(Member.church_id.in_(scope) if scope else False)
    elif church_id is not None:
        query = query.filter(Member.church_id == church_id)

    # A still-pending (or rejected) assisted registration isn't a real
    # member yet — keep the default roster to `approved` unless the caller
    # explicitly asks for another status (e.g. the Pending Registrations tab).
    query = query.filter(Member.registration_status == (registration_status or RegistrationStatus.approved))

    if family_id is not None:
        query = query.filter(Member.family_id == family_id)
    if household_role_id is not None:
        query = query.filter(Member.household_role_id == household_role_id)
    if membership_category_id is not None:
        query = query.filter(Member.membership_category_id == membership_category_id)
    if status_id is not None:
        query = query.filter(Member.status_id == status_id)
    if q:
        like = f"%{q}%"
        query = query.filter(or_(Member.first_name.ilike(like), Member.last_name.ilike(like), Member.national_id.ilike(like)))

    total = query.count()

    sort_map = {
        "last_name": Member.last_name.asc(),
        "-last_name": Member.last_name.desc(),
        "first_name": Member.first_name.asc(),
        "-first_name": Member.first_name.desc(),
        "created_at": Member.created_at.asc(),
        "-created_at": Member.created_at.desc(),
    }
    order = sort_map.get(sort, Member.last_name.asc())

    ids = [m.id for m in query.order_by(order).offset((page - 1) * page_size).limit(page_size).all()]
    items = [member_service.get_member_or_404(db, mid) for mid in ids]
    return MemberListResponse(items=items, total=total, page=page, page_size=page_size)


@router.post("/check-duplicates", response_model=MemberDuplicateCheckResponse)
def check_duplicates(
    payload: MemberDuplicateCheckRequest,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    _perm=Depends(require_permission("manage_members")),
):
    enforce_church_scope(payload.church_id, scope, context="members")
    duplicates = member_service.find_probable_duplicates(
        db,
        payload.church_id,
        payload.first_name,
        payload.last_name,
        payload.date_of_birth,
        phone=payload.phone,
        email=payload.email,
        exclude_member_id=payload.exclude_member_id,
    )
    return MemberDuplicateCheckResponse(
        candidates=[
            MemberDuplicateCandidate(member_id=m.id, member_name=f"{m.first_name} {m.last_name}", match_reason=reason)
            for m, reason in duplicates
        ]
    )


@router.get("/pending-registrations", response_model=MemberListResponse)
def list_pending_registrations(
    church_id: Optional[int] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    query = db.query(Member).filter(Member.registration_status == RegistrationStatus.pending)
    if scope is not None:
        if church_id is not None:
            query = query.filter(Member.church_id == church_id if church_id in scope else False)
        else:
            query = query.filter(Member.church_id.in_(scope) if scope else False)
    elif church_id is not None:
        query = query.filter(Member.church_id == church_id)

    total = query.count()
    ids = [
        m.id
        for m in query.order_by(Member.created_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
    ]
    items = [member_service.get_member_or_404(db, mid) for mid in ids]
    return MemberListResponse(items=items, total=total, page=page, page_size=page_size)


@router.post("", response_model=MemberRead, status_code=status.HTTP_201_CREATED)
def create_member(
    payload: MemberCreate,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("manage_members")),
):
    effective_church_id = enforce_church_scope(payload.church_id, scope, context="members")
    return member_service.create_member(db, payload, effective_church_id, actor_user_id=user.id)


@router.get("/{member_id}", response_model=MemberRead)
def get_member(member_id: int, db: Session = Depends(get_db), scope: Optional[set[int]] = Depends(_read_scope), _perm=Depends(_require_any_read_permission)):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    return member


@router.put("/{member_id}", response_model=MemberRead)
def update_member(
    member_id: int,
    payload: MemberUpdate,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("manage_members")),
):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    effective_church_id = None
    if payload.church_id is not None:
        effective_church_id = enforce_church_scope(payload.church_id, scope, context="members")
    return member_service.update_member(db, member, payload, effective_church_id, actor_user_id=user.id)


@router.post("/{member_id}/transfer-out", response_model=MemberRead)
def transfer_member_out(
    member_id: int,
    payload: MemberTransferOutRequest,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("manage_members")),
):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    return member_service.transfer_out(db, member, payload, actor_user_id=user.id)


@router.post("/{member_id}/approve-registration", response_model=MemberRead)
def approve_member_registration(
    member_id: int,
    payload: Optional[MemberRegistrationApproveRequest] = None,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(REVIEW_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("review_members")),
):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    password = payload.password if payload else None
    return member_service.approve_registration(db, member, actor_user_id=user.id, password=password)


@router.post("/{member_id}/reject-registration", response_model=MemberRead)
def reject_member_registration(
    member_id: int,
    payload: MemberRegistrationRejectRequest,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(REVIEW_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("review_members")),
):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    return member_service.reject_registration(db, member, actor_user_id=user.id, reason=payload.reason)


@router.get("/{member_id}/history", response_model=list[MemberAuditLogRead])
def get_member_history(
    member_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    return member_service.get_member_history(db, member_id)


@router.delete("/{member_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_member(
    member_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    _perm=Depends(require_permission("manage_members")),
):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    member_service.delete_member(db, member)


@router.get("/{member_id}/contacts", response_model=list[MemberContactRead])
def list_contacts(member_id: int, db: Session = Depends(get_db), scope: Optional[set[int]] = Depends(_read_scope), _perm=Depends(_require_any_read_permission)):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    return member.contacts


@router.post("/{member_id}/contacts", response_model=MemberContactRead, status_code=status.HTTP_201_CREATED)
def add_contact(
    member_id: int,
    payload: MemberContactCreate,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    _perm=Depends(require_permission("manage_members")),
):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    return member_service.add_contact(db, member, payload)


@router.put("/{member_id}/contacts/{contact_id}", response_model=MemberContactRead)
def update_contact(
    member_id: int,
    contact_id: int,
    payload: MemberContactUpdate,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    _perm=Depends(require_permission("manage_members")),
):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    contact = member_service.get_contact_or_404(db, member_id, contact_id)
    return member_service.update_contact(db, contact, payload)


@router.delete("/{member_id}/contacts/{contact_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_contact(
    member_id: int,
    contact_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    _perm=Depends(require_permission("manage_members")),
):
    member = member_service.get_member_or_404(db, member_id)
    enforce_church_scope(member.church_id, scope, context="this member")
    contact = member_service.get_contact_or_404(db, member_id, contact_id)
    member_service.delete_contact(db, contact)
