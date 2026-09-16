from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.database import get_db
from app.deps import church_scope_for, get_current_user, require_permission
from app.models import CollectionAuditAction, User
from app.schemas import CollectionAuditLogListResponse
from app.services import collection_audit_service
from sqlalchemy.orm import Session

router = APIRouter(prefix="/collection-audit-logs", tags=["treasury-audit"])

READ_SCOPE = church_scope_for("view_treasury_audit")


@router.get("", response_model=CollectionAuditLogListResponse)
def list_collection_audit_logs(
    church_id: Optional[int] = None,
    collection_id: Optional[int] = None,
    collection_group_id: Optional[int] = None,
    action: Optional[CollectionAuditAction] = None,
    actor_user_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(READ_SCOPE),
    _perm=Depends(require_permission("view_treasury_audit")),
    _user: User = Depends(get_current_user),
):
    """Cross-collection auditor view — an authorised reviewer's read-only
    window over every treasury mutation, church-scoped. No write endpoint of
    any kind is exposed for this table anywhere in the app: that (plus the
    fact ordinary manage/review permissions don't grant this one) is what
    makes it tamper-evident to everyone but the DB owner."""
    items, total = collection_audit_service.list_logs(
        db,
        church_id=church_id,
        collection_id=collection_id,
        collection_group_id=collection_group_id,
        action=action,
        actor_user_id=actor_user_id,
        date_from=date_from,
        date_to=date_to,
        scope=scope,
        page=page,
        page_size=page_size,
    )
    return CollectionAuditLogListResponse(items=items, total=total, page=page, page_size=page_size)
