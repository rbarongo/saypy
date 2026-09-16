from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import church_scope_for, enforce_church_scope, get_current_user, require_permission
from app.models import ReceiptSeries, User
from app.schemas import ReceiptSeriesCreate, ReceiptSeriesRead, ReceiptSeriesUpdate

router = APIRouter(prefix="/receipt-series", tags=["receipt-series"])

SCOPE = church_scope_for("manage_receipts")


@router.get("", response_model=list[ReceiptSeriesRead])
def list_receipt_series(
    church_id: Optional[int] = None,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(SCOPE),
    _perm=Depends(require_permission("manage_receipts")),
):
    query = db.query(ReceiptSeries)
    if scope is not None:
        if church_id is not None:
            query = query.filter(ReceiptSeries.church_id == church_id if church_id in scope else False)
        else:
            query = query.filter(ReceiptSeries.church_id.in_(scope) if scope else False)
    elif church_id is not None:
        query = query.filter(ReceiptSeries.church_id == church_id)
    return query.order_by(ReceiptSeries.church_id.asc()).all()


@router.post("", response_model=ReceiptSeriesRead, status_code=status.HTTP_201_CREATED)
def create_receipt_series(
    payload: ReceiptSeriesCreate,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("manage_receipts")),
):
    effective_church_id = enforce_church_scope(payload.church_id, scope, context="receipt series")
    if payload.range_end < payload.range_start:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="range_end must be greater than or equal to range_start")
    existing_active = (
        db.query(ReceiptSeries)
        .filter(ReceiptSeries.church_id == effective_church_id, ReceiptSeries.is_active.is_(True))
        .first()
    )
    if existing_active is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="This church already has an active receipt series — deactivate it first (PATCH is_active=false) before creating a new one",
        )
    series = ReceiptSeries(
        church_id=effective_church_id,
        prefix=payload.prefix,
        range_start=payload.range_start,
        range_end=payload.range_end,
        next_sequence=payload.range_start,
        is_active=True,
        created_by_user_id=user.id,
    )
    db.add(series)
    db.commit()
    db.refresh(series)
    return series


@router.patch("/{series_id}", response_model=ReceiptSeriesRead)
def update_receipt_series(
    series_id: int,
    payload: ReceiptSeriesUpdate,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(SCOPE),
    _perm=Depends(require_permission("manage_receipts")),
):
    series = db.get(ReceiptSeries, series_id)
    if series is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Receipt series not found")
    enforce_church_scope(series.church_id, scope, context="this receipt series")
    # `range_start`/`next_sequence` are deliberately never directly editable
    # here — the only way `next_sequence` moves is via
    # receipt_service.generate_receipt_number, to prevent number
    # gaming/skipping. `range_end` may only be extended, never shrunk below
    # what's already been issued or below the declared start.
    if payload.prefix is not None:
        series.prefix = payload.prefix
    if payload.is_active is not None:
        series.is_active = payload.is_active
    if payload.range_end is not None:
        floor = max(series.range_start, series.next_sequence - 1)
        if payload.range_end < floor:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"range_end can't be set below {floor} — numbers up to that point are already issued or reserved",
            )
        series.range_end = payload.range_end
    db.commit()
    db.refresh(series)
    return series
