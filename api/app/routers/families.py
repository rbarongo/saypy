from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session, selectinload

from app.database import get_db
from app.deps import church_scope_for, enforce_church_scope, get_permission_scopes, require_permission
from app.models import Family
from app.schemas import FamilyCreate, FamilyRead, FamilyRoster, FamilyUpdate
from app.services import family_service, rbac_service
from app.services.rbac_service import PermissionScopes, has_permission

router = APIRouter(prefix="/families", tags=["families"])

VIEW_SCOPE = church_scope_for("manage_families")


def _read_scope(scopes: PermissionScopes = Depends(get_permission_scopes)) -> Optional[set[int]]:
    """A family is readable if the caller has manage_families, manage_members,
    or view_members for its church; the readable church set is the union."""
    manage_f = rbac_service.accessible_churches(scopes, "manage_families")
    manage_m = rbac_service.accessible_churches(scopes, "manage_members")
    view_m = rbac_service.accessible_churches(scopes, "view_members")
    if manage_f is None or manage_m is None or view_m is None:
        return None
    return manage_f | manage_m | view_m


def _require_any_read_permission(scopes: PermissionScopes = Depends(get_permission_scopes)) -> PermissionScopes:
    if not (
        has_permission(scopes, "manage_families")
        or has_permission(scopes, "manage_members")
        or has_permission(scopes, "view_members")
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Missing required permission to view families")
    return scopes


@router.get("", response_model=list[FamilyRead])
def list_families(
    church_id: Optional[int] = None,
    q: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    query = db.query(Family).options(selectinload(Family.leader), selectinload(Family.members))
    if scope is not None:
        if church_id is not None:
            query = query.filter(Family.church_id == church_id if church_id in scope else False)
        else:
            query = query.filter(Family.church_id.in_(scope) if scope else False)
    elif church_id is not None:
        query = query.filter(Family.church_id == church_id)
    if q:
        query = query.filter(Family.name.ilike(f"%{q}%"))
    return query.order_by(Family.name.asc()).offset((page - 1) * page_size).limit(page_size).all()


@router.post("", response_model=FamilyRead, status_code=status.HTTP_201_CREATED)
def create_family(
    payload: FamilyCreate,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(VIEW_SCOPE),
    _perm=Depends(require_permission("manage_families")),
):
    effective_church_id = enforce_church_scope(payload.church_id, scope, context="families")
    data = payload.model_dump()
    data["church_id"] = effective_church_id
    family = Family(**data)
    db.add(family)
    db.commit()
    db.refresh(family)
    return family


@router.get("/{family_id}", response_model=FamilyRead)
def get_family(family_id: int, db: Session = Depends(get_db), scope: Optional[set[int]] = Depends(VIEW_SCOPE)):
    family = family_service.get_family_or_404(db, family_id)
    enforce_church_scope(family.church_id, scope, context="this family")
    return family


@router.put("/{family_id}", response_model=FamilyRead)
def update_family(
    family_id: int,
    payload: FamilyUpdate,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(VIEW_SCOPE),
    _perm=Depends(require_permission("manage_families")),
):
    family = family_service.get_family_or_404(db, family_id)
    enforce_church_scope(family.church_id, scope, context="this family")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(family, field, value)
    db.commit()
    db.refresh(family)
    return family


@router.delete("/{family_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_family(
    family_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(VIEW_SCOPE),
    _perm=Depends(require_permission("manage_families")),
):
    family = family_service.get_family_or_404(db, family_id)
    enforce_church_scope(family.church_id, scope, context="this family")
    family_service.delete_family(db, family)


@router.get("/{family_id}/members", response_model=FamilyRoster)
def get_family_roster(family_id: int, db: Session = Depends(get_db), scope: Optional[set[int]] = Depends(VIEW_SCOPE)):
    family = family_service.get_family_or_404(db, family_id)
    enforce_church_scope(family.church_id, scope, context="this family")
    return family_service.get_roster(db, family)
