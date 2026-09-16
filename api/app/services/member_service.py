import json
import re
import secrets
import string
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy.orm import Session, selectinload

from app.models import (
    Collection,
    CommunicationCategory,
    CorrectionRequestStatus,
    FeedbackStatus,
    Member,
    MemberAuditLog,
    MemberChurchRole,
    MemberCommunicationPreference,
    MemberContact,
    MemberCorrectionRequest,
    MemberFeedback,
    MemberOrganisation,
    MembershipCategory,
    MemberStatus,
    RegistrationStatus,
    Role,
    User,
    UserRole,
)
from app.schemas import (
    CorrectionRequestCreate,
    FeedbackCreate,
    MemberContactCreate,
    MemberContactUpdate,
    MemberCreate,
    MemberTransferOutRequest,
    MemberUpdate,
    SelfRegisterRequest,
)
from app.security import hash_password

MEMBER_ROLE_CODE = "member"
TRANSFERRED_STATUS_CODE = "transferred"
ACTIVE_STATUS_CODE = "active"
VISITOR_CATEGORY_CODE = "visitor"

# Self-service "request corrections" only ever touches these personal-detail
# fields — membership/status/church/family/household-role stay staff-only,
# changed via the admin MemberForm's direct edit, not this request/approve path.
CORRECTABLE_FIELDS = {"middle_name", "address", "occupation", "date_of_birth", "nationality_id", "national_id", "notes"}

MEMBER_LOAD_OPTIONS = (
    selectinload(Member.household_role),
    selectinload(Member.membership_category),
    selectinload(Member.status),
    selectinload(Member.nationality),
    selectinload(Member.family),
    selectinload(Member.contacts).selectinload(MemberContact.contact_type),
    selectinload(Member.sabbath_school_group),
    selectinload(Member.organisation_links).selectinload(MemberOrganisation.organisation),
    selectinload(Member.church_role_links).selectinload(MemberChurchRole.church_role),
    selectinload(Member.communication_preferences),
)


def get_member_or_404(db: Session, member_id: int) -> Member:
    # populate_existing=True: routers fetch the member once for a permission/
    # scope check, mutate it (incl. relationship FKs like household_role_id),
    # then call this again to build the response — without it, Session.get()
    # would return the same already-loaded object with stale nested relations
    # (e.g. the old household_role) instead of reloading them.
    member = db.get(Member, member_id, options=MEMBER_LOAD_OPTIONS, populate_existing=True)
    if member is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Member not found")
    return member


def _unset_other_primary_contacts(db: Session, member_id: int, contact_type_id: int, exclude_id: int | None = None):
    """Only one contact per (member, contact_type) may be marked primary."""
    query = db.query(MemberContact).filter(
        MemberContact.member_id == member_id,
        MemberContact.contact_type_id == contact_type_id,
        MemberContact.contact_init.is_(True),
    )
    if exclude_id is not None:
        query = query.filter(MemberContact.id != exclude_id)
    query.update({"contact_init": False})


def _slugify_username(first_name: str, last_name: str) -> str:
    base = re.sub(r"[^a-z0-9.]+", "", f"{first_name}.{last_name}".strip().lower())
    base = re.sub(r"\.+", ".", base).strip(".")
    return base or "member"


def _generate_unique_username(db: Session, first_name: str, last_name: str) -> str:
    base = _slugify_username(first_name, last_name)
    username = base
    suffix = 1
    while db.query(User).filter(User.username == username).first() is not None:
        suffix += 1
        username = f"{base}{suffix}"
    return username


def _generate_temp_password(length: int = 12) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def ensure_member_role(db: Session) -> Role:
    """Gets (or, on first use, creates) the default 'Member' role that every
    self-service member account is assigned. Created bare (no permissions) —
    an admin grants more via Access Control if a member needs anything
    beyond logging in."""
    role = db.query(Role).filter(Role.code == MEMBER_ROLE_CODE).first()
    if role is None:
        role = Role(code=MEMBER_ROLE_CODE, name="Member")
        db.add(role)
        db.flush()
    return role


def provision_member_user_account(db: Session, member: Member, password: str | None = None) -> tuple[User, str]:
    """Creates a login account for a newly-created member, defaulting to the
    'member' role. Returns the User plus its plaintext password — the only
    moment it's available outside the hash, so the caller must surface it to
    the admin now (it can't be recovered later, only reset).

    `password` lets the approving staff member choose the account's password
    up front instead of relaying a random one; when omitted, a random
    temporary password is generated as before."""
    role = ensure_member_role(db)
    username = _generate_unique_username(db, member.first_name, member.last_name)
    password = password or _generate_temp_password()
    user = User(username=username, password_hash=hash_password(password), member_id=member.id, is_active=True)
    db.add(user)
    db.flush()
    db.add(UserRole(user_id=user.id, role_id=role.id, church_id=member.church_id, is_active=True))
    db.commit()
    db.refresh(user)
    return user, password


def log_action(db: Session, member_id: int, action: str, actor_user_id: int | None, details: str | None = None) -> None:
    db.add(MemberAuditLog(member_id=member_id, action=action, actor_user_id=actor_user_id, details=details))


def _sync_organisations(db: Session, member_id: int, organisation_ids: list[int]) -> None:
    db.query(MemberOrganisation).filter(MemberOrganisation.member_id == member_id).delete()
    for organisation_id in organisation_ids:
        db.add(MemberOrganisation(member_id=member_id, organisation_id=organisation_id))


def _sync_church_roles(db: Session, member_id: int, church_role_ids: list[int]) -> None:
    db.query(MemberChurchRole).filter(MemberChurchRole.member_id == member_id).delete()
    for church_role_id in church_role_ids:
        db.add(MemberChurchRole(member_id=member_id, church_role_id=church_role_id))


def set_communication_preferences(db: Session, member_id: int, channels: list) -> None:
    """Replaces the 'general' category only — what the admin MemberForm's
    flat checkbox group edits. Scoped so it never wipes a member's own
    per-category choices (offering receipts, church announcements, ...)
    made through the self-service portal; see
    `set_communication_preferences_by_category` for those."""
    db.query(MemberCommunicationPreference).filter(
        MemberCommunicationPreference.member_id == member_id,
        MemberCommunicationPreference.category == CommunicationCategory.general,
    ).delete()
    for channel in channels:
        db.add(MemberCommunicationPreference(member_id=member_id, category=CommunicationCategory.general, channel=channel))


def set_communication_preferences_by_category(db: Session, member_id: int, preferences: dict) -> None:
    """Full wipe-and-replace across every category — used by the member's
    own `/members/me/preferences`, where the member is the sole owner of
    their whole preference set (including 'general')."""
    db.query(MemberCommunicationPreference).filter(MemberCommunicationPreference.member_id == member_id).delete()
    for category, channels in preferences.items():
        for channel in channels:
            db.add(MemberCommunicationPreference(member_id=member_id, category=category, channel=channel))


def _normalize_text(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _normalize_phone(value) -> str:
    return re.sub(r"\D", "", value or "")


def find_probable_duplicates(
    db: Session,
    church_id: int,
    first_name: str,
    last_name: str,
    date_of_birth=None,
    phone: str | None = None,
    email: str | None = None,
    exclude_member_id: int | None = None,
) -> list[tuple[Member, str]]:
    """Deliberately not fuzzy, same philosophy as the collections import's
    member matching (`collection_service.resolve_member_match`) — a wrong
    duplicate *flag* just wastes a click, a wrong duplicate *merge* would be
    a data-integrity incident, so only exact signals count: an exact phone
    or email match anywhere in the church, or an exact normalized
    first+last name match with the same date of birth."""
    query = db.query(Member).options(selectinload(Member.contacts)).filter(Member.church_id == church_id)
    if exclude_member_id is not None:
        query = query.filter(Member.id != exclude_member_id)
    church_members = query.all()

    found: list[tuple[Member, str]] = []
    seen_ids: set[int] = set()

    phone_norm = _normalize_phone(phone)
    if phone_norm:
        for m in church_members:
            if any(_normalize_phone(c.contact) == phone_norm for c in m.contacts):
                found.append((m, "phone"))
                seen_ids.add(m.id)

    email_norm = _normalize_text(email)
    if email_norm:
        for m in church_members:
            if m.id in seen_ids:
                continue
            if any(_normalize_text(c.contact) == email_norm for c in m.contacts):
                found.append((m, "email"))
                seen_ids.add(m.id)

    name_norm = _normalize_text(f"{first_name} {last_name}")
    if name_norm:
        for m in church_members:
            if m.id in seen_ids:
                continue
            if _normalize_text(f"{m.first_name} {m.last_name}") == name_norm and (
                date_of_birth is None or m.date_of_birth == date_of_birth
            ):
                found.append((m, "name_and_dob"))
                seen_ids.add(m.id)

    return found


_NON_COLUMN_CREATE_FIELDS = {"contacts", "organisation_ids", "church_role_ids", "preferred_channels", "submit_for_review", "force_despite_duplicates"}


def create_member(db: Session, payload: MemberCreate, effective_church_id: int, actor_user_id: int | None = None) -> Member:
    if not payload.force_despite_duplicates:
        duplicates = find_probable_duplicates(db, effective_church_id, payload.first_name, payload.last_name, payload.date_of_birth)
        if duplicates:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "Possible duplicate member(s) found. Resubmit with force_despite_duplicates=true to save anyway.",
                    "candidates": [
                        {"member_id": m.id, "member_name": f"{m.first_name} {m.last_name}", "match_reason": reason}
                        for m, reason in duplicates
                    ],
                },
            )

    data = payload.model_dump(exclude=_NON_COLUMN_CREATE_FIELDS)
    data["church_id"] = effective_church_id
    data["registration_status"] = RegistrationStatus.pending if payload.submit_for_review else RegistrationStatus.approved
    member = Member(**data)
    db.add(member)
    db.flush()

    for contact_in in payload.contacts:
        _add_contact(db, member.id, contact_in)
    _sync_organisations(db, member.id, payload.organisation_ids)
    _sync_church_roles(db, member.id, payload.church_role_ids)
    set_communication_preferences(db, member.id, payload.preferred_channels)
    log_action(db, member.id, "registration_submitted" if payload.submit_for_review else "created", actor_user_id)

    db.commit()
    db.refresh(member, attribute_names=["contacts"])

    result = get_member_or_404(db, member.id)
    # A still-pending assisted registration isn't a real member yet — no
    # self-service login account until someone with `review_members`
    # approves it (see approve_registration).
    if member.registration_status == RegistrationStatus.approved:
        user, temp_password = provision_member_user_account(db, member)
        # Transient, response-only fields (not mapped columns) — the
        # plaintext password only ever exists here, right after creation.
        result.created_username = user.username
        result.created_password = temp_password
    return result


def update_member(db: Session, member: Member, payload: MemberUpdate, effective_church_id: int | None, actor_user_id: int | None = None) -> Member:
    data = payload.model_dump(exclude_unset=True, exclude={"organisation_ids", "church_role_ids", "preferred_channels"})
    if effective_church_id is not None:
        data["church_id"] = effective_church_id
    for field, value in data.items():
        setattr(member, field, value)

    if payload.organisation_ids is not None:
        _sync_organisations(db, member.id, payload.organisation_ids)
    if payload.church_role_ids is not None:
        _sync_church_roles(db, member.id, payload.church_role_ids)
    if payload.preferred_channels is not None:
        set_communication_preferences(db, member.id, payload.preferred_channels)

    log_action(db, member.id, "updated", actor_user_id)
    db.commit()
    return get_member_or_404(db, member.id)


def approve_registration(db: Session, member: Member, actor_user_id: int, password: str | None = None) -> Member:
    if member.registration_status != RegistrationStatus.pending:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Only a pending registration can be approved")
    member.registration_status = RegistrationStatus.approved
    log_action(db, member.id, "registration_approved", actor_user_id)
    db.commit()

    result = get_member_or_404(db, member.id)
    already_has_account = db.query(User).filter(User.member_id == member.id).first() is not None
    if not already_has_account:
        user, temp_password = provision_member_user_account(db, member, password=password)
        result.created_username = user.username
        result.created_password = temp_password
    return result


def reject_registration(db: Session, member: Member, actor_user_id: int, reason: str) -> Member:
    if member.registration_status != RegistrationStatus.pending:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Only a pending registration can be rejected")
    member.registration_status = RegistrationStatus.rejected
    log_action(db, member.id, "registration_rejected", actor_user_id, details=reason)
    db.commit()
    return get_member_or_404(db, member.id)


def transfer_out(db: Session, member: Member, payload: MemberTransferOutRequest, actor_user_id: int) -> Member:
    transferred_status = db.query(MemberStatus).filter(MemberStatus.code == TRANSFERRED_STATUS_CODE).first()
    if transferred_status is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"No '{TRANSFERRED_STATUS_CODE}' member status is configured",
        )
    member.transfer_out_church_id = payload.to_church_id
    member.transfer_date = payload.transfer_date
    member.transfer_reason = payload.reason
    member.status_id = transferred_status.id
    log_action(db, member.id, "transferred_out", actor_user_id, details=f"to church {payload.to_church_id}" + (f": {payload.reason}" if payload.reason else ""))
    db.commit()
    return get_member_or_404(db, member.id)


def get_member_history(db: Session, member_id: int) -> list[MemberAuditLog]:
    return (
        db.query(MemberAuditLog)
        .options(selectinload(MemberAuditLog.actor))
        .filter(MemberAuditLog.member_id == member_id)
        .order_by(MemberAuditLog.created_at.desc())
        .all()
    )


def delete_member(db: Session, member: Member) -> None:
    # Financial history is never cascade-deleted along with the member —
    # collections/receipts must survive for treasury accountability even if
    # the member record itself needs to go. Reject the delete outright
    # rather than either orphaning `Collection.member_id` or silently
    # erasing contribution history.
    if db.query(Collection).filter(Collection.member_id == member.id).first() is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="This member has contribution history and cannot be deleted — transfer or archive the member instead",
        )
    # A member's login account (see provision_member_user_account) only
    # exists to represent them — deleting the member without it would
    # either orphan the account or, since users.member_id is a plain FK
    # with no cascade, fail the delete outright with an FK violation.
    # ORM-level db.delete() (not a bulk Query.delete()) so User's own
    # cascade="all, delete-orphan" takes its user_roles rows with it too.
    for user in db.query(User).filter(User.member_id == member.id).all():
        db.delete(user)
    # Audit rows, correction requests, and feedback don't survive a hard
    # delete of their subject either — there's no soft-delete/archival model
    # in this system (yet) to keep them meaningfully attached to anything
    # once the member itself is gone.
    db.query(MemberAuditLog).filter(MemberAuditLog.member_id == member.id).delete()
    db.query(MemberCorrectionRequest).filter(MemberCorrectionRequest.member_id == member.id).delete()
    db.query(MemberFeedback).filter(MemberFeedback.member_id == member.id).delete()
    db.delete(member)
    db.commit()


def _add_contact(db: Session, member_id: int, payload: MemberContactCreate) -> MemberContact:
    if payload.contact_init:
        _unset_other_primary_contacts(db, member_id, payload.contact_type_id)
    contact = MemberContact(member_id=member_id, **payload.model_dump())
    db.add(contact)
    db.flush()
    return contact


def add_contact(db: Session, member: Member, payload: MemberContactCreate) -> MemberContact:
    contact = _add_contact(db, member.id, payload)
    db.commit()
    db.refresh(contact)
    return contact


def get_contact_or_404(db: Session, member_id: int, contact_id: int) -> MemberContact:
    contact = db.get(MemberContact, contact_id)
    if contact is None or contact.member_id != member_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Contact not found")
    return contact


def update_contact(db: Session, contact: MemberContact, payload: MemberContactUpdate) -> MemberContact:
    data = payload.model_dump(exclude_unset=True)
    if data.get("contact_init"):
        _unset_other_primary_contacts(
            db, contact.member_id, data.get("contact_type_id", contact.contact_type_id), exclude_id=contact.id
        )
    for field, value in data.items():
        setattr(contact, field, value)
    db.commit()
    db.refresh(contact)
    return contact


def delete_contact(db: Session, contact: MemberContact) -> None:
    db.delete(contact)
    db.commit()


# ---------------------------------------------------------------------------
# Self-service portal: public self-registration, corrections, feedback.
# ---------------------------------------------------------------------------


def self_register(db: Session, payload: SelfRegisterRequest, effective_church_id: int) -> Member:
    """Public registration — always lands as a pending assisted-registration-
    style record (no login account until someone with `review_members`
    approves it, same as the staff form's "submit for review" path). A
    visitor doesn't get to declare their own membership standing: the
    membership category and status are resolved server-side, not accepted
    from the request."""
    visitor_category = db.query(MembershipCategory).filter(MembershipCategory.code == VISITOR_CATEGORY_CODE).first()
    if visitor_category is None:
        visitor_category = db.query(MembershipCategory).order_by(MembershipCategory.id.asc()).first()
    if visitor_category is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No membership category is configured")

    active_status = db.query(MemberStatus).filter(MemberStatus.code == ACTIVE_STATUS_CODE).first()
    if active_status is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"No '{ACTIVE_STATUS_CODE}' member status is configured")

    create_payload = MemberCreate(
        first_name=payload.first_name,
        middle_name=payload.middle_name,
        last_name=payload.last_name,
        gender=payload.gender,
        date_of_birth=payload.date_of_birth,
        household_role_id=payload.household_role_id,
        membership_category_id=visitor_category.id,
        status_id=active_status.id,
        church_id=effective_church_id,
        nationality_id=payload.nationality_id,
        address=payload.address,
        occupation=payload.occupation,
        contacts=payload.contacts,
        submit_for_review=True,
        force_despite_duplicates=payload.force_despite_duplicates,
    )
    return create_member(db, create_payload, effective_church_id, actor_user_id=None)


CORRECTION_REQUEST_LOAD_OPTIONS = (
    selectinload(MemberCorrectionRequest.member),
    selectinload(MemberCorrectionRequest.reviewed_by),
)


def get_correction_request_or_404(db: Session, request_id: int) -> MemberCorrectionRequest:
    request = db.get(MemberCorrectionRequest, request_id, options=CORRECTION_REQUEST_LOAD_OPTIONS)
    if request is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Correction request not found")
    return request


def create_correction_request(db: Session, member: Member, payload: CorrectionRequestCreate) -> MemberCorrectionRequest:
    changes = {k: v for k, v in payload.requested_changes.items() if k in CORRECTABLE_FIELDS}
    if not changes:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No correctable fields were provided")
    request = MemberCorrectionRequest(member_id=member.id, requested_changes=changes, reason=payload.reason)
    db.add(request)
    db.commit()
    db.refresh(request, attribute_names=["member", "reviewed_by"])
    return request


def apply_correction_request(db: Session, request: MemberCorrectionRequest, actor_user_id: int) -> MemberCorrectionRequest:
    if request.status != CorrectionRequestStatus.pending:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Only a pending correction request can be approved")
    member = get_member_or_404(db, request.member_id)
    applied = {}
    for field, value in request.requested_changes.items():
        if field not in CORRECTABLE_FIELDS:
            continue
        setattr(member, field, value)
        applied[field] = value
    request.status = CorrectionRequestStatus.approved
    request.reviewed_by_user_id = actor_user_id
    request.decided_at = datetime.now(timezone.utc)
    log_action(db, member.id, "correction_approved", actor_user_id, details=json.dumps(applied) if applied else None)
    db.commit()
    return get_correction_request_or_404(db, request.id)


def reject_correction_request(db: Session, request: MemberCorrectionRequest, actor_user_id: int, reason: str) -> MemberCorrectionRequest:
    if request.status != CorrectionRequestStatus.pending:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Only a pending correction request can be rejected")
    request.status = CorrectionRequestStatus.rejected
    request.reviewed_by_user_id = actor_user_id
    request.review_note = reason
    request.decided_at = datetime.now(timezone.utc)
    log_action(db, request.member_id, "correction_rejected", actor_user_id, details=reason)
    db.commit()
    return get_correction_request_or_404(db, request.id)


def create_feedback(db: Session, member: Member, payload: FeedbackCreate) -> MemberFeedback:
    feedback = MemberFeedback(member_id=member.id, subject=payload.subject, message=payload.message)
    db.add(feedback)
    db.commit()
    db.refresh(feedback)
    return feedback


def mark_feedback_reviewed(db: Session, feedback: MemberFeedback) -> MemberFeedback:
    feedback.status = FeedbackStatus.reviewed
    feedback.reviewed_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(feedback)
    return feedback
