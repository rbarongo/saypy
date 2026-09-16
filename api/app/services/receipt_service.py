from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy.orm import Session, selectinload

from app.models import Church, Collection, CollectionAuditAction, CollectionItem, Receipt, ReceiptSeries, ReceiptStatus
from app.services import collection_audit_service

RECEIPT_LOAD_OPTIONS = (
    selectinload(Receipt.member),
    selectinload(Receipt.issued_by),
    selectinload(Receipt.cancelled_by),
    selectinload(Receipt.collection).selectinload(Collection.items).selectinload(CollectionItem.collection_type),
    selectinload(Receipt.collection).selectinload(Collection.member),
)


def get_receipt_or_404(db: Session, receipt_id: int) -> Receipt:
    receipt = db.get(Receipt, receipt_id, options=RECEIPT_LOAD_OPTIONS, populate_existing=True)
    if receipt is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Receipt not found")
    return receipt


def _get_or_create_series(db: Session, church: Church, actor_user_id: int | None) -> ReceiptSeries:
    series = (
        db.query(ReceiptSeries)
        .filter(ReceiptSeries.church_id == church.id, ReceiptSeries.is_active.is_(True))
        .with_for_update()
        .first()
    )
    if series is not None:
        return series
    # Lazily create on first use, backfilling from any receipts already
    # issued for this church under the old derived-count scheme. Left
    # open-ended (range_end=None) — a real declared range is something an
    # admin sets up explicitly via POST /receipt-series, matching a real
    # pre-numbered physical receipt booklet.
    existing_count = db.query(Receipt).filter(Receipt.church_id == church.id).count()
    start = existing_count + 1
    series = ReceiptSeries(
        church_id=church.id,
        prefix=f"RCT-{church.code}",
        range_start=start,
        range_end=None,
        next_sequence=start,
        is_active=True,
        created_by_user_id=actor_user_id,
    )
    db.add(series)
    db.flush()
    return series


def generate_receipt_number(db: Session, church: Church, actor_user_id: int | None = None) -> str:
    """Persisted, row-locked continuous per-church sequence — replaces the
    previous non-locked `COUNT(...)`-derived numbering, fixing its documented
    concurrency gap as a byproduct. Format: `{prefix}-{seq:06d}`. Refuses to
    issue past the series' declared range — the caller (an approval action)
    sees a clear 409 rather than silently overflowing a pre-printed booklet."""
    series = _get_or_create_series(db, church, actor_user_id)
    if series.is_exhausted:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Receipt series '{series.prefix}' has issued all numbers in its range "
                f"({series.range_start}-{series.range_end}) — configure a new range in Receipt Series before approving more collections for this church."
            ),
        )
    number = f"{series.prefix}-{series.next_sequence:06d}"
    series.next_sequence += 1
    return number


def reprint_receipt(db: Session, receipt: Receipt, actor_user_id: int) -> Receipt:
    if receipt.status == ReceiptStatus.cancelled:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A cancelled receipt cannot be reprinted")
    receipt.reprint_count += 1
    if receipt.status == ReceiptStatus.issued:
        receipt.status = ReceiptStatus.reprinted
    collection_audit_service.log_action(
        db,
        church_id=receipt.church_id,
        action=CollectionAuditAction.receipt_reprinted,
        actor_user_id=actor_user_id,
        collection_id=receipt.collection_id,
        receipt_id=receipt.id,
        after_value={"reprint_count": receipt.reprint_count},
    )
    db.commit()
    return get_receipt_or_404(db, receipt.id)


def cancel_receipt(db: Session, receipt: Receipt, actor_user_id: int, reason: str) -> Receipt:
    if receipt.status == ReceiptStatus.cancelled:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="This receipt is already cancelled")
    before = {"status": receipt.status.value}
    receipt.status = ReceiptStatus.cancelled
    receipt.cancelled_reason = reason
    receipt.cancelled_by_user_id = actor_user_id
    receipt.cancelled_at = datetime.now(timezone.utc)
    collection_audit_service.log_action(
        db,
        church_id=receipt.church_id,
        action=CollectionAuditAction.receipt_cancelled,
        actor_user_id=actor_user_id,
        collection_id=receipt.collection_id,
        receipt_id=receipt.id,
        reason=reason,
        before_value=before,
        after_value={"status": receipt.status.value},
    )
    db.commit()
    return get_receipt_or_404(db, receipt.id)


def correct_receipt(db: Session, old_receipt: Receipt, actor_user_id: int, reason: str, corrected_total: float) -> Receipt:
    """Never edits a receipt in place — marks the old one `corrected` and
    issues a brand-new receipt (own number, from the same series), linked
    back via `superseded_by_receipt_id`. Called automatically from
    collection_service.decide_correction when an already-receipted
    collection's correction is approved."""
    old_receipt.status = ReceiptStatus.corrected
    collection = old_receipt.collection
    new_number = generate_receipt_number(db, collection.church, actor_user_id)
    collection.receipt_number = new_number
    new_receipt = Receipt(
        collection_id=collection.id,
        receipt_number=new_number,
        member_id=collection.member_id,
        church_id=collection.church_id,
        total_amount=corrected_total,
        issued_by_user_id=actor_user_id,
    )
    db.add(new_receipt)
    db.flush()
    old_receipt.superseded_by_receipt_id = new_receipt.id
    collection_audit_service.log_action(
        db,
        church_id=collection.church_id,
        action=CollectionAuditAction.receipt_corrected,
        actor_user_id=actor_user_id,
        collection_id=collection.id,
        receipt_id=old_receipt.id,
        reason=reason,
        before_value={"receipt_number": old_receipt.receipt_number, "total_amount": float(old_receipt.total_amount)},
        after_value={"receipt_number": new_number, "total_amount": float(corrected_total)},
    )
    return new_receipt
