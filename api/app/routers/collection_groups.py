from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import church_scope_for, get_current_user, get_permission_scopes, require_permission
from app.models import CollectionGroup, CollectionGroupStatus, User
from app.schemas import CollectionGroupDetail, CollectionGroupRead
from app.services import collection_service, rbac_service
from app.services.rbac_service import PermissionScopes, has_permission

router = APIRouter(prefix="/collection-groups", tags=["collection-groups"])

REVIEW_SCOPE = church_scope_for("review_collections")
READ_PERMISSIONS = ("manage_collections", "review_collections", "view_collections")


def _read_scope(scopes: PermissionScopes = Depends(get_permission_scopes)) -> Optional[set[int]]:
    per_permission = [rbac_service.accessible_churches(scopes, code) for code in READ_PERMISSIONS]
    if any(s is None for s in per_permission):
        return None
    union: set[int] = set()
    for s in per_permission:
        union |= s
    return union


def _require_any_read_permission(scopes: PermissionScopes = Depends(get_permission_scopes)) -> PermissionScopes:
    if not any(has_permission(scopes, code) for code in READ_PERMISSIONS):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Missing required permission to view collection groups")
    return scopes


@router.get("", response_model=list[CollectionGroupRead])
def list_collection_groups(
    church_id: Optional[int] = None,
    status_filter: Optional[CollectionGroupStatus] = Query(None, alias="status"),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    query = db.query(CollectionGroup)
    if scope is not None:
        if church_id is not None:
            query = query.filter(CollectionGroup.church_id == church_id if church_id in scope else False)
        else:
            query = query.filter(CollectionGroup.church_id.in_(scope) if scope else False)
    elif church_id is not None:
        query = query.filter(CollectionGroup.church_id == church_id)
    if status_filter is not None:
        query = query.filter(CollectionGroup.status == status_filter)
    groups = query.order_by(CollectionGroup.initiated_at.desc()).all()
    return [collection_service.get_group_or_404(db, g.id) for g in groups]


@router.get("/{group_id}", response_model=CollectionGroupDetail)
def get_collection_group(
    group_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    group = collection_service.get_group_or_404(db, group_id)
    if scope is not None and group.church_id not in scope:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized for this collection group")
    return group


@router.post("/{group_id}/approve-all", response_model=CollectionGroupDetail)
def approve_all_pending(
    group_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(REVIEW_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("review_collections")),
):
    group = collection_service.get_group_or_404(db, group_id)
    if scope is not None and group.church_id not in scope:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized for this collection group")
    return collection_service.approve_all_pending(db, group, user.id)
