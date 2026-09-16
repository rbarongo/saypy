from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import church_scope_for, enforce_church_scope, get_current_user, require_permission
from app.models import User
from app.routers.collections import _enforce_read, _read_scope, _require_any_read_permission
from app.schemas import ReceiptCancelRequest, ReceiptRead
from app.services import receipt_service

router = APIRouter(prefix="/receipts", tags=["receipts"])

MANAGE_SCOPE = church_scope_for("manage_receipts")


@router.get("/batch", response_model=list[ReceiptRead])
def get_receipt_batch(
    ids: str = Query(..., description="Comma-separated receipt ids"),
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    try:
        receipt_ids = [int(x) for x in ids.split(",") if x.strip()]
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ids must be a comma-separated list of integers")
    receipts = [receipt_service.get_receipt_or_404(db, rid) for rid in receipt_ids]
    for receipt in receipts:
        _enforce_read(receipt, scope)
    return receipts


@router.get("/{receipt_id}", response_model=ReceiptRead)
def get_receipt(
    receipt_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(_read_scope),
    _perm=Depends(_require_any_read_permission),
):
    receipt = receipt_service.get_receipt_or_404(db, receipt_id)
    _enforce_read(receipt, scope)
    return receipt


@router.post("/{receipt_id}/reprint", response_model=ReceiptRead)
def reprint_receipt(
    receipt_id: int,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(MANAGE_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("manage_receipts")),
):
    receipt = receipt_service.get_receipt_or_404(db, receipt_id)
    enforce_church_scope(receipt.church_id, scope, context="this receipt")
    return receipt_service.reprint_receipt(db, receipt, user.id)


@router.post("/{receipt_id}/cancel", response_model=ReceiptRead)
def cancel_receipt(
    receipt_id: int,
    payload: ReceiptCancelRequest,
    db: Session = Depends(get_db),
    scope: Optional[set[int]] = Depends(MANAGE_SCOPE),
    user: User = Depends(get_current_user),
    _perm=Depends(require_permission("manage_receipts")),
):
    receipt = receipt_service.get_receipt_or_404(db, receipt_id)
    enforce_church_scope(receipt.church_id, scope, context="this receipt")
    return receipt_service.cancel_receipt(db, receipt, user.id, payload.reason)
