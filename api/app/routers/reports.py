from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import Date, cast, func, or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import (
    Church,
    ChurchRole,
    ContactType,
    HouseholdRole,
    Member,
    MemberAuditLog,
    MemberChurchRole,
    MemberContact,
    MemberOrganisation,
    MembershipCategory,
    MemberStatus,
    Nationality,
    Organisation,
    RegistrationStatus,
    SabbathSchoolGroup,
    User,
)
from app.routers.members import _read_scope, _require_any_read_permission
from app.routers.stats import PHONE_CONTACT_CODES
from app.schemas import ReportColumn, ReportResult

router = APIRouter(prefix="/reports", tags=["reports"])

DEFAULT_MOVEMENT_ACTIONS = ("created", "registration_submitted", "registration_approved", "registration_rejected", "transferred_out")
ALL_MOVEMENT_ACTIONS = DEFAULT_MOVEMENT_ACTIONS + ("updated", "correction_approved", "correction_rejected")


def _apply_church_scope(query, church_id: Optional[int], scope: Optional[set[int]]):
    """Same 3-branch church-scoping as list_members — kept as our own copy
    since stats.py's private `_scoped` has no `church_id` param (it's never
    scoped to one church there)."""
    if scope is not None:
        if church_id is not None:
            return query.filter(Member.church_id == church_id if church_id in scope else False)
        return query.filter(Member.church_id.in_(scope) if scope else False)
    if church_id is not None:
        return query.filter(Member.church_id == church_id)
    return query


def _member_number(church_code: str, member_id: int) -> str:
    """Mirrors Member.member_number — needed here because report queries
    select raw columns instead of loading full ORM objects."""
    return f"{church_code}-{member_id:05d}"


def _primary_contacts(db: Session, member_ids: list[int]) -> dict[int, dict[str, Optional[str]]]:
    """member_id -> {'phone': str|None, 'email': str|None}. A member may have
    several rows per contact type; prefer contact_init=True, else the most
    recently added."""
    if not member_ids:
        return {}
    rows = (
        db.query(MemberContact.member_id, ContactType.code, MemberContact.contact, MemberContact.contact_init, MemberContact.created_at)
        .join(ContactType, ContactType.id == MemberContact.contact_type_id)
        .filter(MemberContact.member_id.in_(member_ids))
        .order_by(MemberContact.member_id, MemberContact.contact_init.desc(), MemberContact.created_at.desc())
        .all()
    )
    result: dict[int, dict[str, Optional[str]]] = {}
    for member_id, code, contact, _init, _created in rows:
        bucket = result.setdefault(member_id, {"phone": None, "email": None})
        if code == "email" and bucket["email"] is None:
            bucket["email"] = contact
        elif code in PHONE_CONTACT_CODES and bucket["phone"] is None:
            bucket["phone"] = contact
    return result


def _month_seq(date_from: date, date_to: date) -> list[str]:
    months, y, m = [], date_from.year, date_from.month
    while (y, m) <= (date_to.year, date_to.month):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return months


def _add_months(d: date, months: int) -> date:
    total = d.year * 12 + (d.month - 1) + months
    y, m = divmod(total, 12)
    return date(y, m + 1, 1)


def _age_bracket(dob: Optional[date], today: date) -> str:
    if dob is None:
        return "Unknown"
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    if age <= 12:
        return "0-12"
    if age <= 17:
        return "13-17"
    if age <= 35:
        return "18-35"
    if age <= 60:
        return "36-60"
    return "60+"


@router.get("/membership-register", response_model=ReportResult)
def membership_register(
    church_id: Optional[int] = None,
    status_id: Optional[int] = None,
    membership_category_id: Optional[int] = None,
    household_role_id: Optional[int] = None,
    registration_status: Optional[RegistrationStatus] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    q: Optional[str] = None,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    query = (
        db.query(
            Member.id, Member.first_name, Member.middle_name, Member.last_name,
            Member.gender, Member.date_of_birth, Member.join_date,
            Church.code.label("church_code"), Church.name.label("church_name"),
            HouseholdRole.name.label("household_role_name"),
            MembershipCategory.name.label("membership_category_name"),
            MemberStatus.name.label("status_name"),
        )
        .join(Church, Church.id == Member.church_id)
        .join(HouseholdRole, HouseholdRole.id == Member.household_role_id)
        .join(MembershipCategory, MembershipCategory.id == Member.membership_category_id)
        .join(MemberStatus, MemberStatus.id == Member.status_id)
        .filter(Member.registration_status == (registration_status or RegistrationStatus.approved))
    )
    query = _apply_church_scope(query, church_id, scope)
    if status_id is not None:
        query = query.filter(Member.status_id == status_id)
    if membership_category_id is not None:
        query = query.filter(Member.membership_category_id == membership_category_id)
    if household_role_id is not None:
        query = query.filter(Member.household_role_id == household_role_id)
    if date_from is not None:
        query = query.filter(Member.join_date >= date_from)
    if date_to is not None:
        query = query.filter(Member.join_date <= date_to)
    if q:
        like = f"%{q}%"
        query = query.filter(or_(Member.first_name.ilike(like), Member.last_name.ilike(like), Member.national_id.ilike(like)))

    records = query.order_by(Member.last_name.asc(), Member.first_name.asc()).all()
    contacts = _primary_contacts(db, [r.id for r in records])

    rows = [
        {
            "member_number": _member_number(r.church_code, r.id),
            "name": f"{r.first_name} {r.middle_name + ' ' if r.middle_name else ''}{r.last_name}",
            "gender": r.gender.value if r.gender else None,
            "date_of_birth": r.date_of_birth.isoformat() if r.date_of_birth else None,
            "join_date": r.join_date.isoformat() if r.join_date else None,
            "church": r.church_name,
            "household_role": r.household_role_name,
            "membership_category": r.membership_category_name,
            "status": r.status_name,
            "phone": contacts.get(r.id, {}).get("phone"),
            "email": contacts.get(r.id, {}).get("email"),
        }
        for r in records
    ]

    columns = [
        ReportColumn(key="member_number", label="Member No."),
        ReportColumn(key="name", label="Name"),
        ReportColumn(key="gender", label="Gender"),
        ReportColumn(key="date_of_birth", label="Date of Birth"),
        ReportColumn(key="join_date", label="Join Date"),
        ReportColumn(key="church", label="Church"),
        ReportColumn(key="household_role", label="Household Role"),
        ReportColumn(key="membership_category", label="Membership Category"),
        ReportColumn(key="status", label="Status"),
        ReportColumn(key="phone", label="Phone"),
        ReportColumn(key="email", label="Email"),
    ]
    return ReportResult(columns=columns, rows=rows, summary={"total_members": len(rows)}, total=len(rows), generated_at=datetime.utcnow())


@router.get("/membership-movements", response_model=ReportResult)
def membership_movements(
    church_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    actions: Optional[str] = Query(None, description="Comma-separated action codes; default: created,registration_submitted,registration_approved,registration_rejected,transferred_out"),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    if actions:
        action_list = [a.strip() for a in actions.split(",") if a.strip() in ALL_MOVEMENT_ACTIONS]
    else:
        action_list = list(DEFAULT_MOVEMENT_ACTIONS)

    # MemberAuditLog carries no church_id of its own — church_id here is the
    # member's *current* Member.church_id. transfer_out() never reassigns
    # church_id (only status/transfer_* fields), so this correctly attributes
    # every action, including transferred_out, to the member's home church.
    query = (
        db.query(
            MemberAuditLog.id, MemberAuditLog.created_at, MemberAuditLog.action, MemberAuditLog.details,
            Member.id.label("member_id"), Member.first_name, Member.last_name,
            Church.code.label("church_code"), Church.name.label("church_name"),
            User.username.label("actor_username"),
        )
        .join(Member, Member.id == MemberAuditLog.member_id)
        .join(Church, Church.id == Member.church_id)
        .outerjoin(User, User.id == MemberAuditLog.actor_user_id)
        .filter(MemberAuditLog.action.in_(action_list))
    )
    query = _apply_church_scope(query, church_id, scope)
    if date_from is not None:
        query = query.filter(cast(MemberAuditLog.created_at, Date) >= date_from)
    if date_to is not None:
        query = query.filter(cast(MemberAuditLog.created_at, Date) <= date_to)

    records = query.order_by(MemberAuditLog.created_at.desc()).all()
    rows = [
        {
            "date": r.created_at.isoformat(),
            "member_number": _member_number(r.church_code, r.member_id),
            "member_name": f"{r.first_name} {r.last_name}",
            "church": r.church_name,
            "action": r.action,
            "details": r.details,
            "actor_username": r.actor_username,
        }
        for r in records
    ]

    counts: dict[str, int] = {}
    for r in records:
        counts[r.action] = counts.get(r.action, 0) + 1

    columns = [
        ReportColumn(key="date", label="Date"),
        ReportColumn(key="member_number", label="Member No."),
        ReportColumn(key="member_name", label="Member"),
        ReportColumn(key="church", label="Church"),
        ReportColumn(key="action", label="Action"),
        ReportColumn(key="details", label="Details"),
        ReportColumn(key="actor_username", label="By"),
    ]
    return ReportResult(columns=columns, rows=rows, summary={"counts_by_action": counts}, total=len(rows), generated_at=datetime.utcnow())


@router.get("/membership-growth", response_model=ReportResult)
def membership_growth(
    church_id: Optional[int] = None,
    membership_category_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    if date_to is None:
        date_to = date.today()
    if date_from is None:
        date_from = _add_months(date_to.replace(day=1), -11)

    base = db.query(Member).filter(Member.registration_status == RegistrationStatus.approved)
    base = _apply_church_scope(base, church_id, scope)
    if membership_category_id is not None:
        base = base.filter(Member.membership_category_id == membership_category_id)

    range_start = date_from.replace(day=1)
    baseline = base.filter(cast(Member.created_at, Date) < range_start).count()

    month_col = func.to_char(Member.created_at, "YYYY-MM")
    monthly_counts = dict(
        base.filter(cast(Member.created_at, Date) >= range_start, cast(Member.created_at, Date) <= date_to)
        .with_entities(month_col.label("month"), func.count(Member.id))
        .group_by(month_col)
        .all()
    )

    cumulative = baseline
    rows = []
    for month in _month_seq(range_start, date_to):
        new_count = monthly_counts.get(month, 0)
        cumulative += new_count
        rows.append({"month": month, "new_members": new_count, "cumulative_total": cumulative})

    columns = [
        ReportColumn(key="month", label="Month"),
        ReportColumn(key="new_members", label="New Members", numeric=True),
        ReportColumn(key="cumulative_total", label="Cumulative Total", numeric=True),
    ]
    summary = {
        "total_new_in_range": sum(r["new_members"] for r in rows),
        "ending_cumulative": rows[-1]["cumulative_total"] if rows else baseline,
    }
    return ReportResult(columns=columns, rows=rows, summary=summary, total=len(rows), generated_at=datetime.utcnow())


@router.get("/sabbath-school-membership", response_model=ReportResult)
def sabbath_school_membership(
    church_id: Optional[int] = None,
    sabbath_school_group_id: Optional[int] = Query(None, description="Pass 0 to mean 'no group assigned'"),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    query = (
        db.query(
            Member.id, Member.first_name, Member.last_name,
            Church.code.label("church_code"), Church.name.label("church_name"),
            SabbathSchoolGroup.name.label("group_name"),
        )
        .join(Church, Church.id == Member.church_id)
        .outerjoin(SabbathSchoolGroup, SabbathSchoolGroup.id == Member.sabbath_school_group_id)
        .filter(Member.registration_status == RegistrationStatus.approved)
    )
    query = _apply_church_scope(query, church_id, scope)
    if sabbath_school_group_id == 0:
        query = query.filter(Member.sabbath_school_group_id.is_(None))
    elif sabbath_school_group_id is not None:
        query = query.filter(Member.sabbath_school_group_id == sabbath_school_group_id)

    records = query.order_by(Member.last_name.asc()).all()
    rows = [
        {
            "member_number": _member_number(r.church_code, r.id),
            "member_name": f"{r.first_name} {r.last_name}",
            "church": r.church_name,
            "sabbath_school_group": r.group_name or "— none —",
        }
        for r in records
    ]

    counts: dict[str, int] = {}
    for r in rows:
        counts[r["sabbath_school_group"]] = counts.get(r["sabbath_school_group"], 0) + 1

    columns = [
        ReportColumn(key="member_number", label="Member No."),
        ReportColumn(key="member_name", label="Member"),
        ReportColumn(key="church", label="Church"),
        ReportColumn(key="sabbath_school_group", label="Sabbath School Group"),
    ]
    return ReportResult(columns=columns, rows=rows, summary={"counts_by_group": counts}, total=len(rows), generated_at=datetime.utcnow())


@router.get("/ministry-participation", response_model=ReportResult)
def ministry_participation(
    church_id: Optional[int] = None,
    organisation_id: Optional[int] = None,
    church_role_id: Optional[int] = None,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    query = (
        db.query(Member.id, Member.first_name, Member.last_name, Church.code.label("church_code"), Church.name.label("church_name"))
        .join(Church, Church.id == Member.church_id)
        .filter(Member.registration_status == RegistrationStatus.approved)
    )
    query = _apply_church_scope(query, church_id, scope)
    if organisation_id is not None:
        query = query.filter(Member.organisation_links.any(MemberOrganisation.organisation_id == organisation_id))
    if church_role_id is not None:
        query = query.filter(Member.church_role_links.any(MemberChurchRole.church_role_id == church_role_id))

    records = query.order_by(Member.last_name.asc()).all()
    member_ids = [r.id for r in records]

    org_names: dict[int, list[str]] = {}
    for member_id, name in (
        db.query(MemberOrganisation.member_id, Organisation.name)
        .join(Organisation, Organisation.id == MemberOrganisation.organisation_id)
        .filter(MemberOrganisation.member_id.in_(member_ids))
        .all()
    ):
        org_names.setdefault(member_id, []).append(name)

    role_names: dict[int, list[str]] = {}
    for member_id, name in (
        db.query(MemberChurchRole.member_id, ChurchRole.name)
        .join(ChurchRole, ChurchRole.id == MemberChurchRole.church_role_id)
        .filter(MemberChurchRole.member_id.in_(member_ids))
        .all()
    ):
        role_names.setdefault(member_id, []).append(name)

    rows = [
        {
            "member_number": _member_number(r.church_code, r.id),
            "member_name": f"{r.first_name} {r.last_name}",
            "church": r.church_name,
            "organisations": ", ".join(org_names.get(r.id, [])) or "—",
            "church_roles": ", ".join(role_names.get(r.id, [])) or "—",
        }
        for r in records
    ]

    org_count_query = _apply_church_scope(
        db.query(Organisation.code, Organisation.name, func.count(func.distinct(MemberOrganisation.member_id)))
        .join(MemberOrganisation, MemberOrganisation.organisation_id == Organisation.id)
        .join(Member, Member.id == MemberOrganisation.member_id)
        .filter(Member.registration_status == RegistrationStatus.approved)
        .group_by(Organisation.id),
        church_id,
        scope,
    )
    role_count_query = _apply_church_scope(
        db.query(ChurchRole.code, ChurchRole.name, func.count(func.distinct(MemberChurchRole.member_id)))
        .join(MemberChurchRole, MemberChurchRole.church_role_id == ChurchRole.id)
        .join(Member, Member.id == MemberChurchRole.member_id)
        .filter(Member.registration_status == RegistrationStatus.approved)
        .group_by(ChurchRole.id),
        church_id,
        scope,
    )

    columns = [
        ReportColumn(key="member_number", label="Member No."),
        ReportColumn(key="member_name", label="Member"),
        ReportColumn(key="church", label="Church"),
        ReportColumn(key="organisations", label="Organisations"),
        ReportColumn(key="church_roles", label="Church Roles"),
    ]
    summary = {
        "organisation_counts": [{"code": c, "name": n, "count": cnt} for c, n, cnt in org_count_query.all()],
        "church_role_counts": [{"code": c, "name": n, "count": cnt} for c, n, cnt in role_count_query.all()],
    }
    return ReportResult(columns=columns, rows=rows, summary=summary, total=len(rows), generated_at=datetime.utcnow())


@router.get("/demographic-summary", response_model=ReportResult)
def demographic_summary(
    church_id: Optional[int] = None,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    query = (
        db.query(
            Member.gender, Member.date_of_birth, Nationality.name.label("nationality_name"),
            HouseholdRole.name.label("household_role_name"), MembershipCategory.name.label("membership_category_name"),
        )
        .outerjoin(Nationality, Nationality.id == Member.nationality_id)
        .join(HouseholdRole, HouseholdRole.id == Member.household_role_id)
        .join(MembershipCategory, MembershipCategory.id == Member.membership_category_id)
        .filter(Member.registration_status == RegistrationStatus.approved)
    )
    query = _apply_church_scope(query, church_id, scope)
    records = query.all()
    today = date.today()

    def _counts(values):
        c: dict[str, int] = {}
        for v in values:
            c[v] = c.get(v, 0) + 1
        return c

    breakdowns = {
        "gender": _counts((r.gender.value if r.gender else "Unknown") for r in records),
        "age_bracket": _counts(_age_bracket(r.date_of_birth, today) for r in records),
        "nationality": _counts((r.nationality_name or "Unknown") for r in records),
        "household_role": _counts(r.household_role_name for r in records),
        "membership_category": _counts(r.membership_category_name for r in records),
    }
    rows = [{"breakdown": b, "category": cat, "count": cnt} for b, buckets in breakdowns.items() for cat, cnt in buckets.items()]

    columns = [
        ReportColumn(key="breakdown", label="Breakdown"),
        ReportColumn(key="category", label="Category"),
        ReportColumn(key="count", label="Count", numeric=True),
    ]
    return ReportResult(
        columns=columns, rows=rows, summary={"total_members": len(records), "breakdowns": breakdowns}, total=len(rows), generated_at=datetime.utcnow()
    )


@router.get("/contact-completeness", response_model=ReportResult)
def contact_completeness(
    church_id: Optional[int] = None,
    gap: Optional[str] = Query(None, description="missing_phone | missing_email | missing_both | has_all"),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    query = (
        db.query(Member.id, Member.first_name, Member.last_name, Church.code.label("church_code"), Church.name.label("church_name"))
        .join(Church, Church.id == Member.church_id)
        .filter(Member.registration_status == RegistrationStatus.approved)
    )
    query = _apply_church_scope(query, church_id, scope)
    records = query.order_by(Member.last_name.asc()).all()
    contacts = _primary_contacts(db, [r.id for r in records])

    all_rows = []
    for r in records:
        c = contacts.get(r.id, {})
        has_phone, has_email = c.get("phone") is not None, c.get("email") is not None
        all_rows.append(
            {
                "member_number": _member_number(r.church_code, r.id),
                "member_name": f"{r.first_name} {r.last_name}",
                "church": r.church_name,
                "has_phone": has_phone,
                "has_email": has_email,
                "phone": c.get("phone"),
                "email": c.get("email"),
            }
        )

    total = len(all_rows)
    with_phone = sum(1 for r in all_rows if r["has_phone"])
    with_email = sum(1 for r in all_rows if r["has_email"])
    with_both = sum(1 for r in all_rows if r["has_phone"] and r["has_email"])
    with_neither = sum(1 for r in all_rows if not r["has_phone"] and not r["has_email"])

    gap_filters = {
        "missing_phone": lambda r: not r["has_phone"],
        "missing_email": lambda r: not r["has_email"],
        "missing_both": lambda r: not r["has_phone"] and not r["has_email"],
        "has_all": lambda r: r["has_phone"] and r["has_email"],
    }
    rows = [r for r in all_rows if gap_filters[gap](r)] if gap in gap_filters else all_rows

    columns = [
        ReportColumn(key="member_number", label="Member No."),
        ReportColumn(key="member_name", label="Member"),
        ReportColumn(key="church", label="Church"),
        ReportColumn(key="phone", label="Phone"),
        ReportColumn(key="email", label="Email"),
    ]
    summary = {"total": total, "with_phone": with_phone, "with_email": with_email, "with_both": with_both, "with_neither": with_neither}
    return ReportResult(columns=columns, rows=rows, summary=summary, total=len(rows), generated_at=datetime.utcnow())
