from fastapi import HTTPException, status
from sqlalchemy.orm import Session, selectinload

from app.models import Family, Member


def get_family_or_404(db: Session, family_id: int) -> Family:
    family = db.get(Family, family_id)
    if family is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Family not found")
    return family


def delete_family(db: Session, family: Family) -> None:
    still_linked = db.query(Member).filter(Member.family_id == family.id).count()
    if still_linked:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot delete: {still_linked} member(s) still belong to this family",
        )
    db.delete(family)
    db.commit()


def get_roster(db: Session, family: Family) -> dict:
    members = (
        db.query(Member)
        .options(selectinload(Member.household_role))
        .filter(Member.family_id == family.id)
        .order_by(Member.household_role_id.asc(), Member.first_name.asc())
        .all()
    )
    leader = None
    if family.leader_id is not None:
        leader = db.get(Member, family.leader_id, options=[selectinload(Member.household_role)])
    return {"family": family, "leader": leader, "members": members}
