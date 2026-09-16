import io
import os
from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import church_scope_for, enforce_church_scope, get_current_member, get_current_user, get_permission_scopes, require_permission
from app.models import Collection, CollectionEntryMethod, CollectionItem, CollectionStatus, Member, User
from app.schemas import (
    AgentCollectionSubmitRequest,
    CollectionAuditLogRead,
    CollectionCorrectionCreate,
    CollectionCorrectionDecision,
    CollectionCorrectionRead,
    CollectionCreate,
    CollectionGroupDetail,
    CollectionListResponse,
    CollectionRead,
    CollectionRejectRequest,
    ImportCommitRequest,
    ImportInspectResponse,
    ImportResolveRequest,
    ImportResolveResponse,
    ReceiptRead,
)
from app.services import collection_audit_service, collection_service, rbac_service
from app.services.rbac_service import PermissionScopes, has_permission

router = APIRouter(prefix="/collections", tags=["collections"])

WRITE_SCOPE = church_scope_for("manage_collections")
REVIEW_SCOPE = church_scope_for("review_collections")
CORRECT_SCOPE = church_scope_for("correct_collections")
READ_PERMISSIONS = ("manage_collections", "review_collections", "view_collections")


def _read_scope(scopes: PermissionScopes = Depends(get_permission_scopes)) -> Optional[set[int]]:
    """A collection is readable if the caller has manage_collections,
    review_collections, or view_collections for its church; the readable
    church set is the union — same pattern as families.py/members.py."""
    per_permission = [rbac_service.accessible_churches(scopes, code) for code in READ_PERMISSIONS]
    if any(s is None for s in per_permission):
        return None
    union: set[int] = set()
    for s in per_permission:
        union |= s
    return union


def _require_any_read_permission(scopes: PermissionScopes = Depends(get_permission_scopes)) -> PermissionScopes:
    if not any(has_permission(scopes, code) for code in READ_PERMISSIONS):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Missing required permission to view collections")
    return scopes


def _enforce_read(collection: Collection, scope: Optional[set[int]]) -> None:
    if scope is not None and collection.church_id not in scope:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized for this collection")


@router.get("", response_model=CollectionListResponse)
def list_collections(
    church_id: Optional[int] = None,
    status_filter: Optional[CollectionStatus] = Query(None, alias="status"),
    entry_method: Optional[CollectionEntryMethod] = None,
    collection_group_id: Optional[int] = None,
    member_id: Optional[int] = None,
    collection_type_id: Optional[int] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    q: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    query = db.query(Collection)
    if scope is not None:
        if church_id is not None:
            query = query.filter(Collection.church_id == church_id if church_id in scope else False)
        else:
            query = query.filter(Collection.church_id.in_(scope) if scope else False)
    elif church_id is not None:
        query = query.filter(Collection.church_id == church_id)
    if status_filter is not None:
        query = query.filter(Collection.status == status_filter)
    if entry_method is not None:
        query = query.filter(Collection.entry_method == entry_method)
    if collection_group_id is not None:
        query = query.filter(Collection.collection_group_id == collection_group_id)
    if member_id is not None:
        query = query.filter(Collection.member_id == member_id)
    if collection_type_id is not None:
        query = query.filter(Collection.id.in_(db.query(CollectionItem.collection_id).filter(CollectionItem.collection_type_id == collection_type_id)))
    if date_from is not None:
        query = query.filter(Collection.collection_date >= date_from)
    if date_to is not None:
        query = query.filter(Collection.collection_date <= date_to)
    if q:
        like = f"%{q}%"
        query = query.join(Member, Member.id == Collection.member_id).filter(
            or_(Member.first_name.ilike(like), Member.last_name.ilike(like), Collection.notes.ilike(like), Collection.receipt_number.ilike(like))
        )

    total = query.count()
    ids = [
        row.id
        for row in query.order_by(Collection.collection_date.desc(), Collection.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    ]
    items = [collection_service.get_collection_or_404(db, cid) for cid in ids]
    return CollectionListResponse(items=items, total=total, page=page, page_size=page_size)


@router.post("", response_model=CollectionRead, status_code=status.HTTP_201_CREATED)
def create_collection(
    payload: CollectionCreate,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("manage_collections")),
):
    effective_church_id = enforce_church_scope(payload.church_id, scope, context="collections")
    if payload.is_anonymous:
        if payload.member_id is not None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="An anonymous contribution cannot also name a member")
    else:
        if payload.member_id is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="member_id is required unless the contribution is anonymous")
        member = db.get(Member, payload.member_id)
        if member is None or member.church_id != effective_church_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="member_id must belong to the target church")
    return collection_service.create_manual_collection(db, payload, user.id, effective_church_id)


@router.post("/agent-submit", response_model=CollectionRead, status_code=status.HTTP_201_CREATED)
def submit_agent_collection(
    payload: AgentCollectionSubmitRequest,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("manage_collections")),
):
    """Ingestion point for the Digital Treasurer receipt-reading agent —
    accepts its structured JSON output (photo/slip/bank or mobile-money
    notification already parsed) and stores it as an ordinary *pending*
    Collection, landing in the exact same review queue as manual/import
    entries. Nothing here is posted as an official collection until a
    reviewer approves it (see POST /collections/{id}/approve).

    Authenticated the same way any other collection-creating integration
    would be: the caller (the agent/bot account) needs `manage_collections`
    for the target church — there is no separate, unauthenticated ingestion
    path for financial data."""
    effective_church_id = enforce_church_scope(payload.church_id, scope, context="collections")
    return collection_service.submit_agent_collection(db, payload.model_copy(update={"church_id": effective_church_id}), user.id)


@router.post("/{collection_id}/evidence", response_model=CollectionRead)
def upload_collection_evidence(
    collection_id: int,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    _perm=Depends(require_permission("manage_collections")),
):
    collection = collection_service.get_collection_or_404(db, collection_id)
    enforce_church_scope(collection.church_id, scope, context="this collection")
    collection.evidence_file_path = collection_service.save_evidence_file(collection, file)
    db.commit()
    return collection_service.get_collection_or_404(db, collection_id)


@router.get("/{collection_id}/evidence")
def get_collection_evidence(
    collection_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    collection = collection_service.get_collection_or_404(db, collection_id)
    _enforce_read(collection, scope)
    if not collection.evidence_file_path or not os.path.isfile(collection.evidence_file_path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No evidence file has been uploaded for this collection")
    return FileResponse(collection.evidence_file_path)


# ---------------------------------------------------------------------------
# Import wizard — three calls matching the frontend's three steps: upload +
# inspect, map + resolve members, then commit as a new pending group.
# ---------------------------------------------------------------------------


def _read_upload_dataframe(file: UploadFile) -> pd.DataFrame:
    content = file.file.read()
    name = (file.filename or "").lower()
    try:
        if name.endswith(".csv"):
            return pd.read_csv(io.BytesIO(content))
        return pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Could not read file: {e}")


def _sanitize_cell(value):
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


@router.post("/import/inspect", response_model=ImportInspectResponse)
def inspect_import(
    file: UploadFile = File(...),
    _perm=Depends(require_permission("manage_collections")),
):
    df = _read_upload_dataframe(file)
    df.columns = [str(c) for c in df.columns]
    rows = [{str(k): _sanitize_cell(v) for k, v in row.items()} for row in df.to_dict(orient="records")]
    return ImportInspectResponse(headers=list(df.columns), rows=rows)


@router.post("/import/resolve-members", response_model=ImportResolveResponse)
def resolve_members(
    payload: ImportResolveRequest,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    _perm=Depends(require_permission("manage_collections")),
):
    enforce_church_scope(payload.church_id, scope, context="collections")
    rows = collection_service.build_resolved_rows(
        db, payload.church_id, payload.rows, payload.identification_mapping, payload.collection_mapping
    )
    return ImportResolveResponse(rows=rows)


@router.post("/import/commit", response_model=CollectionGroupDetail, status_code=status.HTTP_201_CREATED)
def commit_import(
    payload: ImportCommitRequest,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(WRITE_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("manage_collections")),
):
    effective_church_id = enforce_church_scope(payload.church_id, scope, context="collections")
    member_ids = {row.member_id for row in payload.rows}
    valid_ids = {
        row.id for row in db.query(Member.id).filter(Member.id.in_(member_ids), Member.church_id == effective_church_id).all()
    }
    missing = member_ids - valid_ids
    if missing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"member_id(s) not found in the target church: {sorted(missing)}",
        )
    return collection_service.commit_import(db, payload, user.id, effective_church_id)


# ---------------------------------------------------------------------------
# Self-service: a member's own collections/receipts. No RBAC permission
# needed — self-access only, same reasoning as members.py's `/me` endpoints.
# ---------------------------------------------------------------------------


@router.get("/mine", response_model=CollectionListResponse)
def list_my_collections(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    member=Depends(get_current_member),
):
    query = db.query(Collection).filter(Collection.member_id == member.id)
    total = query.count()
    ids = [
        row.id
        for row in query.order_by(Collection.collection_date.desc(), Collection.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    ]
    items = [collection_service.get_collection_or_404(db, cid) for cid in ids]
    return CollectionListResponse(items=items, total=total, page=page, page_size=page_size)


# ---------------------------------------------------------------------------
# Single-collection read + approval workflow
# ---------------------------------------------------------------------------


@router.get("/{collection_id}", response_model=CollectionRead)
def get_collection(
    collection_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    collection = collection_service.get_collection_or_404(db, collection_id)
    _enforce_read(collection, scope)
    return collection


@router.get("/{collection_id}/receipt", response_model=ReceiptRead)
def get_receipt(
    collection_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    collection = collection_service.get_collection_or_404(db, collection_id)
    _enforce_read(collection, scope)
    return collection_service.get_receipt_by_collection_or_404(db, collection_id)


@router.post("/{collection_id}/approve", response_model=CollectionRead)
def approve_collection(
    collection_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(REVIEW_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("review_collections")),
):
    collection = collection_service.get_collection_or_404(db, collection_id)
    _enforce_read(collection, scope)
    return collection_service.approve_collection(db, collection, user.id)


@router.post("/{collection_id}/reject", response_model=CollectionRead)
def reject_collection(
    collection_id: int,
    payload: CollectionRejectRequest,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(REVIEW_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("review_collections")),
):
    collection = collection_service.get_collection_or_404(db, collection_id)
    _enforce_read(collection, scope)
    return collection_service.reject_collection(db, collection, user.id, payload.reason)


# ---------------------------------------------------------------------------
# Correction / adjustment workflow — the only path to change an
# already-approved collection. Maker-checker enforced in the service layer.
# ---------------------------------------------------------------------------


@router.post("/{collection_id}/corrections", response_model=CollectionCorrectionRead, status_code=status.HTTP_201_CREATED)
def request_correction(
    collection_id: int,
    payload: CollectionCorrectionCreate,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(CORRECT_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("correct_collections")),
):
    collection = collection_service.get_collection_or_404(db, collection_id)
    enforce_church_scope(collection.church_id, scope, context="this collection")
    return collection_service.request_correction(db, collection, user.id, payload)


@router.get("/{collection_id}/corrections", response_model=list[CollectionCorrectionRead])
def list_corrections(
    collection_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    collection = collection_service.get_collection_or_404(db, collection_id)
    _enforce_read(collection, scope)
    return collection_service.list_corrections_for_collection(db, collection_id)


@router.post("/corrections/{correction_id}/decide", response_model=CollectionCorrectionRead)
def decide_correction(
    correction_id: int,
    payload: CollectionCorrectionDecision,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(REVIEW_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("review_collections")),
):
    correction = collection_service.get_correction_or_404(db, correction_id)
    collection = collection_service.get_collection_or_404(db, correction.collection_id)
    enforce_church_scope(collection.church_id, scope, context="this collection")
    return collection_service.decide_correction(db, correction, user.id, payload.approve, payload.decision_notes)


@router.get("/{collection_id}/audit-log", response_model=list[CollectionAuditLogRead])
def get_collection_audit_log(
    collection_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    """Scoped to this one collection — reuses the same read permissions as
    the collection itself (no separate `view_treasury_audit` grant needed
    just to see one's own collection's history). A cross-collection auditor
    view lives at GET /collection-audit-logs, gated by that dedicated
    permission instead."""
    collection = collection_service.get_collection_or_404(db, collection_id)
    _enforce_read(collection, scope)
    items, _total = collection_audit_service.list_logs(db, collection_id=collection_id, page_size=200)
    return items
