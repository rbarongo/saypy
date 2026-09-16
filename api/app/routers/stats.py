from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy import Date, cast, func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import (
    Church,
    ContactType,
    Family,
    HouseholdRole,
    Member,
    MemberContact,
    MembershipCategory,
    MemberStatus,
    RegistrationStatus,
    Role,
    User,
)
from app.routers.members import _read_scope, _require_any_read_permission
from app.schemas import StatsOverview

PHONE_CONTACT_CODES = ("mobile", "landline", "whatsapp")

router = APIRouter(prefix="/stats", tags=["stats"])


def _scoped(query, scope: Optional[set[int]]):
    """Members-table-joined query, filtered to the caller's readable churches."""
    if scope is not None:
        query = query.filter(Member.church_id.in_(scope) if scope else False)
    return query


@router.get("/overview", response_model=StatsOverview)
def overview(
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    # Headline breakdowns describe the real, approved roster — a still-pending
    # or rejected assisted registration isn't a member yet (see list_members).
    def _approved(query):
        return query.filter(Member.registration_status == RegistrationStatus.approved)

    members_total = _approved(_scoped(db.query(Member), scope)).count()

    by_status = _approved(_scoped(
        db.query(MemberStatus.code, MemberStatus.name, func.count(Member.id))
        .join(Member, Member.status_id == MemberStatus.id)
        .group_by(MemberStatus.id),
        scope,
    )).all()

    by_household_role = _approved(_scoped(
        db.query(HouseholdRole.code, HouseholdRole.name, func.count(Member.id))
        .join(Member, Member.household_role_id == HouseholdRole.id)
        .group_by(HouseholdRole.id),
        scope,
    )).all()

    by_membership_category = _approved(_scoped(
        db.query(MembershipCategory.code, MembershipCategory.name, func.count(Member.id))
        .join(Member, Member.membership_category_id == MembershipCategory.id)
        .group_by(MembershipCategory.id),
        scope,
    )).all()

    by_church = _approved(_scoped(
        db.query(Church.code, Church.name, func.count(Member.id))
        .join(Member, Member.church_id == Church.id)
        .group_by(Church.id),
        scope,
    )).all()

    day_col = cast(Member.created_at, Date)
    recent_trend = _approved(_scoped(
        db.query(day_col.label("day"), func.count(Member.id))
        .group_by(day_col)
        .order_by(day_col),
        scope,
    )).all()

    # -- New in this pass: registration-status breakdown (across ALL
    # statuses, unlike the approved-only breakdowns above), transferred
    # count, contact completeness, and a monthly growth/cumulative series.
    by_registration_status = _scoped(
        db.query(Member.registration_status, func.count(Member.id)).group_by(Member.registration_status),
        scope,
    ).all()

    members_transferred_count = _scoped(
        db.query(Member).filter(Member.transfer_out_church_id.isnot(None)),
        scope,
    ).count()

    contact_total = _approved(_scoped(db.query(Member), scope)).count()
    with_phone = _approved(_scoped(
        db.query(Member.id)
        .join(MemberContact, MemberContact.member_id == Member.id)
        .join(ContactType, ContactType.id == MemberContact.contact_type_id)
        .filter(ContactType.code.in_(PHONE_CONTACT_CODES))
        .distinct(),
        scope,
    )).count()
    with_email = _approved(_scoped(
        db.query(Member.id)
        .join(MemberContact, MemberContact.member_id == Member.id)
        .join(ContactType, ContactType.id == MemberContact.contact_type_id)
        .filter(ContactType.code == "email")
        .distinct(),
        scope,
    )).count()
    without_any_contact = _approved(_scoped(
        db.query(Member.id).filter(~Member.id.in_(db.query(MemberContact.member_id))),
        scope,
    )).count()

    month_col = func.to_char(Member.created_at, "YYYY-MM")
    monthly_rows = _approved(_scoped(
        db.query(month_col.label("month"), func.count(Member.id)).group_by(month_col).order_by(month_col),
        scope,
    )).all()
    cumulative = 0
    members_growth_monthly = []
    for month, cnt in monthly_rows:
        cumulative += cnt
        members_growth_monthly.append({"month": month, "count": cnt, "cumulative": cumulative})

    families_query = db.query(Family)
    if scope is not None:
        families_query = families_query.filter(Family.church_id.in_(scope) if scope else False)
    families_total = families_query.count()

    return StatsOverview(
        members_total=members_total,
        members_by_status=[{"code": c, "name": n, "count": cnt} for c, n, cnt in by_status],
        members_by_household_role=[{"code": c, "name": n, "count": cnt} for c, n, cnt in by_household_role],
        members_by_membership_category=[{"code": c, "name": n, "count": cnt} for c, n, cnt in by_membership_category],
        members_by_church=[{"code": c, "name": n, "count": cnt} for c, n, cnt in by_church],
        members_recent_trend=[{"day": d, "count": cnt} for d, cnt in recent_trend],
        families_total=families_total,
        churches_total=db.query(Church).count(),
        users_total=db.query(User).count(),
        roles_total=db.query(Role).count(),
        members_by_registration_status=[{"code": s.value, "name": s.value.capitalize(), "count": cnt} for s, cnt in by_registration_status],
        members_transferred_count=members_transferred_count,
        contact_completeness={
            "total": contact_total,
            "with_phone": with_phone,
            "with_email": with_email,
            "without_any_contact": without_any_contact,
        },
        members_growth_monthly=members_growth_monthly,
    )
