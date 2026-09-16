from typing import Optional

from sqlalchemy.orm import Session, selectinload

from app.models import CollectionAuditAction, CollectionAuditLog

LOAD_OPTIONS = (selectinload(CollectionAuditLog.actor),)


def log_action(
    db: Session,
    *,
    church_id: int,
    action: CollectionAuditAction,
    actor_user_id: Optional[int],
    collection_id: Optional[int] = None,
    collection_group_id: Optional[int] = None,
    receipt_id: Optional[int] = None,
    reason: Optional[str] = None,
    before_value: Optional[dict] = None,
    after_value: Optional[dict] = None,
    source: Optional[str] = None,
) -> CollectionAuditLog:
    """Append-only insert — no update/delete path is ever exposed for this
    table anywhere in the app; that alone is what makes it tamper-evident
    against ordinary users. Caller commits."""
    entry = CollectionAuditLog(
        church_id=church_id,
        action=action,
        actor_user_id=actor_user_id,
        collection_id=collection_id,
        collection_group_id=collection_group_id,
        receipt_id=receipt_id,
        reason=reason,
        before_value=before_value,
        after_value=after_value,
        source=source,
    )
    db.add(entry)
    db.flush()
    return entry


def list_logs(
    db: Session,
    *,
    church_id: Optional[int] = None,
    collection_id: Optional[int] = None,
    collection_group_id: Optional[int] = None,
    action: Optional[CollectionAuditAction] = None,
    actor_user_id: Optional[int] = None,
    date_from=None,
    date_to=None,
    scope: Optional[set[int]] = None,
    page: int = 1,
    page_size: int = 50,
):
    from sqlalchemy import Date, cast

    query = db.query(CollectionAuditLog).options(*LOAD_OPTIONS)
    if scope is not None:
        if church_id is not None:
            query = query.filter(CollectionAuditLog.church_id == church_id if church_id in scope else False)
        else:
            query = query.filter(CollectionAuditLog.church_id.in_(scope) if scope else False)
    elif church_id is not None:
        query = query.filter(CollectionAuditLog.church_id == church_id)
    if collection_id is not None:
        query = query.filter(CollectionAuditLog.collection_id == collection_id)
    if collection_group_id is not None:
        query = query.filter(CollectionAuditLog.collection_group_id == collection_group_id)
    if action is not None:
        query = query.filter(CollectionAuditLog.action == action)
    if actor_user_id is not None:
        query = query.filter(CollectionAuditLog.actor_user_id == actor_user_id)
    if date_from is not None:
        query = query.filter(cast(CollectionAuditLog.created_at, Date) >= date_from)
    if date_to is not None:
        query = query.filter(cast(CollectionAuditLog.created_at, Date) <= date_to)

    total = query.count()
    items = (
        query.order_by(CollectionAuditLog.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return items, total
