import os
import re
import uuid
from datetime import date, datetime, timezone

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import or_
from sqlalchemy.orm import Session, selectinload

from app.models import (
    Church,
    Collection,
    CollectionAuditAction,
    CollectionCorrection,
    CollectionCorrectionKind,
    CollectionCorrectionStatus,
    CollectionEntryMethod,
    CollectionGroup,
    CollectionGroupStatus,
    CollectionItem,
    CollectionSource,
    CollectionStatus,
    CollectionType,
    Member,
    Receipt,
    ReceiptStatus,
)
from app.schemas import AgentCollectionSubmitRequest, CollectionCorrectionCreate, CollectionCreate, ImportCommitRequest
from app.services import collection_audit_service, receipt_service
from app.settings import settings

COLLECTION_LOAD_OPTIONS = (
    selectinload(Collection.items).selectinload(CollectionItem.collection_type),
    selectinload(Collection.member),
    selectinload(Collection.receipts),
)

GROUP_LOAD_OPTIONS = (
    selectinload(CollectionGroup.initiator),
    selectinload(CollectionGroup.approver),
    selectinload(CollectionGroup.collections).selectinload(Collection.items).selectinload(CollectionItem.collection_type),
    selectinload(CollectionGroup.collections).selectinload(Collection.member),
)


def get_collection_or_404(db: Session, collection_id: int) -> Collection:
    collection = db.get(Collection, collection_id, options=COLLECTION_LOAD_OPTIONS, populate_existing=True)
    if collection is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Collection not found")
    return collection


def get_group_or_404(db: Session, group_id: int) -> CollectionGroup:
    group = db.get(CollectionGroup, group_id, options=GROUP_LOAD_OPTIONS, populate_existing=True)
    if group is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Collection group not found")
    return group


def get_receipt_by_collection_or_404(db: Session, collection_id: int) -> Receipt:
    """A collection can have more than one Receipt row over its lifetime
    (a correction reissues rather than replaces — see
    receipt_service.correct_receipt), so this deliberately looks up the
    *current* one by matching Collection.receipt_number (globally unique),
    not by collection_id alone."""
    collection = get_collection_or_404(db, collection_id)
    if collection.receipt_number is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No receipt has been issued for this collection")
    receipt = (
        db.query(Receipt)
        .options(
            selectinload(Receipt.member),
            selectinload(Receipt.issued_by),
            selectinload(Receipt.cancelled_by),
            selectinload(Receipt.collection).selectinload(Collection.items).selectinload(CollectionItem.collection_type),
            selectinload(Receipt.collection).selectinload(Collection.member),
        )
        .filter(Receipt.receipt_number == collection.receipt_number)
        .first()
    )
    if receipt is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No receipt has been issued for this collection")
    return receipt


def _next_daily_code(db: Session, model, code_column, prefix: str) -> str:
    """`{prefix}-{YYYYMMDD}-{seq:04d}`, sequence scoped to today's prefix. Not
    concurrency-safe against a simultaneous insert (no row lock) — acceptable
    at this app's scale, same simplicity level as the rest of the codebase."""
    today_prefix = f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d')}-"
    count = db.query(model).filter(code_column.like(f"{today_prefix}%")).count()
    return f"{today_prefix}{count + 1:04d}"


def next_group_code(db: Session) -> str:
    return _next_daily_code(db, CollectionGroup, CollectionGroup.code, "CG")


def _normalize_phone(value: str | None) -> str:
    return re.sub(r"\D", "", value or "")


def _normalize_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def resolve_member_match(db: Session, church_id: int, name: str | None, phone: str | None, email: str | None):
    """Best-effort match against this church's members: exact phone, then
    exact email, then exact normalized full name — in that order of
    confidence. Deliberately not fuzzy: a wrong auto-match on financial data
    is worse than leaving a row for manual review, so anything short of an
    exact match falls through to 'unmatched' rather than guessing."""
    church_members = (
        db.query(Member)
        .options(selectinload(Member.contacts))
        .filter(Member.church_id == church_id)
        .all()
    )

    phone_norm = _normalize_phone(phone)
    if phone_norm:
        for member in church_members:
            for contact in member.contacts:
                if _normalize_phone(contact.contact) == phone_norm:
                    return member, "phone"

    email_norm = _normalize_text(email)
    if email_norm:
        for member in church_members:
            for contact in member.contacts:
                if _normalize_text(contact.contact) == email_norm:
                    return member, "email"

    name_norm = _normalize_text(name)
    if name_norm:
        for member in church_members:
            full = _normalize_text(f"{member.first_name} {member.last_name}")
            full_mid = _normalize_text(f"{member.first_name} {member.middle_name or ''} {member.last_name}")
            if name_norm in (full, full_mid):
                return member, "name"

    return None, "unmatched"


def build_resolved_rows(
    db: Session,
    church_id: int,
    rows: list[dict],
    identification_mapping: dict[str, str],
    collection_mapping: dict[str, int],
) -> list[dict]:
    id_headers = {kind: header for header, kind in identification_mapping.items() if kind in ("name", "phone", "email")}
    resolved = []
    for idx, row in enumerate(rows):
        name = row.get(id_headers.get("name", ""), None)
        phone = row.get(id_headers.get("phone", ""), None)
        email = row.get(id_headers.get("email", ""), None)

        items = []
        for header, type_id in collection_mapping.items():
            raw = row.get(header)
            if raw is None or raw == "":
                continue
            try:
                amount = float(str(raw).replace(",", "").strip())
            except (TypeError, ValueError):
                continue
            if amount <= 0:
                continue
            items.append({"collection_type_id": type_id, "amount": amount})

        member, confidence = resolve_member_match(db, church_id, name, phone, email)
        resolved.append({
            "row_index": idx,
            "name": str(name).strip() if name not in (None, "") else None,
            "phone": str(phone).strip() if phone not in (None, "") else None,
            "email": str(email).strip() if email not in (None, "") else None,
            "member_id": member.id if member else None,
            "member_name": f"{member.first_name} {member.last_name}" if member else None,
            "match_confidence": confidence,
            "items": items,
            "total_amount": sum(i["amount"] for i in items),
        })
    return resolved


def _add_collection(
    db: Session,
    *,
    group_id: int,
    member_id: int | None,
    church_id: int,
    total_amount: float,
    collection_source,
    entry_method: CollectionEntryMethod,
    collection_date,
    notes: str | None,
    items: list[dict],
    is_anonymous: bool = False,
    payment_reference: str | None = None,
    actor_user_id: int | None = None,
    source: str = "manual",
) -> Collection:
    collection = Collection(
        collection_group_id=group_id,
        member_id=member_id,
        church_id=church_id,
        total_amount=total_amount,
        collection_source=collection_source,
        entry_method=entry_method,
        collection_date=collection_date,
        status=CollectionStatus.pending,
        notes=notes,
        is_anonymous=is_anonymous,
        payment_reference=payment_reference,
    )
    db.add(collection)
    db.flush()
    for item in items:
        db.add(CollectionItem(
            collection_id=collection.id,
            collection_type_id=item["collection_type_id"],
            amount=item["amount"],
            original_amount=item["amount"] if item.get("original_amount") is None else item["original_amount"],
            notes=item.get("notes"),
        ))
    collection_audit_service.log_action(
        db,
        church_id=church_id,
        action=CollectionAuditAction.collection_created,
        actor_user_id=actor_user_id,
        collection_id=collection.id,
        collection_group_id=group_id,
        after_value={"total_amount": float(total_amount), "is_anonymous": is_anonymous},
        source=source,
    )
    return collection


def create_manual_collection(db: Session, payload: CollectionCreate, initiator_user_id: int, effective_church_id: int) -> Collection:
    group = CollectionGroup(
        code=next_group_code(db),
        name=None,
        church_id=effective_church_id,
        status=CollectionGroupStatus.pending,
        initiator_user_id=initiator_user_id,
    )
    db.add(group)
    db.flush()

    total_amount = sum(item.amount for item in payload.items)
    collection = _add_collection(
        db,
        group_id=group.id,
        member_id=payload.member_id,
        church_id=effective_church_id,
        total_amount=total_amount,
        collection_source=payload.collection_source,
        entry_method=CollectionEntryMethod.manual,
        collection_date=payload.collection_date,
        notes=payload.notes,
        items=[item.model_dump() for item in payload.items],
        is_anonymous=payload.is_anonymous,
        payment_reference=payload.payment_reference,
        actor_user_id=initiator_user_id,
        source="manual",
    )
    db.commit()
    return get_collection_or_404(db, collection.id)


def commit_import(db: Session, payload: ImportCommitRequest, initiator_user_id: int, effective_church_id: int) -> CollectionGroup:
    group = CollectionGroup(
        code=next_group_code(db),
        name=payload.group_name,
        church_id=effective_church_id,
        status=CollectionGroupStatus.pending,
        initiator_user_id=initiator_user_id,
    )
    db.add(group)
    db.flush()

    for row in payload.rows:
        _add_collection(
            db,
            group_id=group.id,
            member_id=row.member_id,
            church_id=effective_church_id,
            total_amount=row.total_amount,
            collection_source=payload.collection_source,
            entry_method=CollectionEntryMethod.import_,
            collection_date=payload.collection_date,
            notes=row.notes,
            items=[item.model_dump() for item in row.items],
            actor_user_id=initiator_user_id,
            source="import",
        )
    db.commit()
    return get_group_or_404(db, group.id)


# ---------------------------------------------------------------------------
# Digital Treasurer agent submissions — a receipt-reading agent (photo/slip/
# bank or mobile-money notification -> structured JSON) submits on the
# giver's behalf. Still lands as an ordinary pending Collection in the same
# review queue as manual/import entries; nothing here bypasses approval,
# receipt issuance, or the audit trail.
# ---------------------------------------------------------------------------

_AGENT_PAYMENT_CHANNEL_MAP = {
    "bank": CollectionSource.bank_transfer,
    "bank_transfer": CollectionSource.bank_transfer,
    "mobile": CollectionSource.mobile_money,
    "mobile_money": CollectionSource.mobile_money,
    "momo": CollectionSource.mobile_money,
    "mpesa": CollectionSource.mobile_money,
    "tigo_pesa": CollectionSource.mobile_money,
    "airtel_money": CollectionSource.mobile_money,
    "cash": CollectionSource.cash,
    "cheque": CollectionSource.cheque,
    "check": CollectionSource.cheque,
}


def _map_agent_payment_channel(value: str | None) -> CollectionSource:
    if not value:
        return CollectionSource.other
    return _AGENT_PAYMENT_CHANNEL_MAP.get(_normalize_text(value).replace(" ", "_"), CollectionSource.other)


def _parse_agent_transaction_date(value: str | None) -> date:
    """Agent dates look like '14/08/2026, 08:56 Am' (DD/MM/YYYY, HH:MM AM/PM)
    — best-effort parse, falling back to today if the format doesn't match.
    Never blocks a submission over an unparsable date; the full raw payload
    is kept either way for a reviewer to check."""
    if value:
        for fmt in ("%d/%m/%Y, %I:%M %p", "%d/%m/%Y %I:%M %p", "%d/%m/%Y"):
            try:
                return datetime.strptime(value.strip(), fmt).date()
            except ValueError:
                continue
    return datetime.now(timezone.utc).date()


def _resolve_agent_item_type(db: Session, church_id: int, item) -> int | None:
    """Match an item's `category` (preferred) or `name` against this
    church's (or a global) CollectionType code/name — case-insensitive,
    exact match only, same "don't guess on financial data" philosophy as
    resolve_member_match. None if nothing matches."""
    candidates = [c for c in (item.category, item.name) if c]
    if not candidates:
        return None
    types = db.query(CollectionType).filter(or_(CollectionType.church_id == church_id, CollectionType.church_id.is_(None))).all()
    for candidate in candidates:
        norm = _normalize_text(candidate)
        for t in types:
            if _normalize_text(t.code) == norm or _normalize_text(t.name) == norm:
                return t.id
    return None


def submit_agent_collection(db: Session, payload: AgentCollectionSubmitRequest, initiator_user_id: int) -> Collection:
    church = db.get(Church, payload.church_id)
    if church is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Church not found")

    member, _match_confidence = resolve_member_match(
        db, payload.church_id,
        name=payload.payer or payload.name,
        phone=payload.phone,
        email=payload.email,
    )

    items: list[dict] = []
    unresolved = []
    for raw_item in payload.items:
        type_id = _resolve_agent_item_type(db, payload.church_id, raw_item)
        if type_id is None:
            unresolved.append(raw_item.category or raw_item.name)
            continue
        items.append({"collection_type_id": type_id, "amount": raw_item.amount})

    if not items:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"None of the submitted items' categories match a configured collection type for this church: {unresolved or 'no items provided'}",
        )

    # The true total is always the sum of the (matched) items — never
    # blindly trusted from the agent's own `total_amount`, which can
    # disagree with its own extracted items (as this exact agent already
    # flags via `validation_issues` in some cases).
    total_amount = sum(i["amount"] for i in items)

    validation_issues = list(payload.validation_issues)
    if unresolved:
        validation_issues.append(f"Could not match a collection type for: {', '.join(str(u) for u in unresolved)}")
    if payload.total_amount is not None and abs(float(payload.total_amount) - total_amount) > 0.01:
        validation_issues.append(
            f"Agent-reported total ({payload.total_amount}) differs from the sum of matched items ({total_amount}) — the item sum is used as the recorded amount."
        )

    payer_label = payload.payer or payload.name or "unknown payer"
    group = CollectionGroup(
        code=next_group_code(db),
        name=f"Agent submission — {payer_label}",
        church_id=payload.church_id,
        status=CollectionGroupStatus.pending,
        initiator_user_id=initiator_user_id,
    )
    db.add(group)
    db.flush()

    collection = _add_collection(
        db,
        group_id=group.id,
        member_id=member.id if member else None,
        church_id=payload.church_id,
        total_amount=total_amount,
        collection_source=_map_agent_payment_channel(payload.payment_channel),
        entry_method=CollectionEntryMethod.agent,
        collection_date=_parse_agent_transaction_date(payload.transaction_date),
        notes=payload.summary,
        items=items,
        # Never auto-marked anonymous here: an unmatched payer is a *named*
        # giver the system just couldn't link to an existing Member record,
        # not someone who chose to withhold their identity — the reviewer
        # sees the extracted name (agent_payload) either way.
        is_anonymous=False,
        payment_reference=payload.reference_number,
        actor_user_id=initiator_user_id,
        source="agent_submission",
    )

    # Store the full raw submission, with the server-side checks (unresolved
    # item categories, total-vs-items mismatch) folded into its
    # validation_issues list alongside whatever the agent itself flagged —
    # one JSON column carries everything a reviewer needs to see.
    stored_payload = payload.model_dump(mode="json")
    stored_payload["validation_issues"] = validation_issues
    collection.agent_payload = stored_payload

    db.commit()
    return get_collection_or_404(db, collection.id)


def _refresh_group_status(group: CollectionGroup, decider_user_id: int) -> None:
    still_pending = any(c.status == CollectionStatus.pending for c in group.collections)
    if not still_pending:
        group.status = CollectionGroupStatus.reviewed
        group.approver_user_id = decider_user_id
        group.approved_at = datetime.now(timezone.utc)
    else:
        group.status = CollectionGroupStatus.pending


def _approve_one(db: Session, collection: Collection, approver_user_id: int) -> None:
    if collection.status != CollectionStatus.pending:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Only pending collections can be approved")
    collection.status = CollectionStatus.approved
    collection.receipt_number = receipt_service.generate_receipt_number(db, collection.church, approver_user_id)
    receipt = Receipt(
        collection_id=collection.id,
        receipt_number=collection.receipt_number,
        member_id=collection.member_id,
        church_id=collection.church_id,
        total_amount=collection.total_amount,
        issued_by_user_id=approver_user_id,
    )
    db.add(receipt)
    db.flush()
    collection_audit_service.log_action(
        db,
        church_id=collection.church_id,
        action=CollectionAuditAction.collection_approved,
        actor_user_id=approver_user_id,
        collection_id=collection.id,
        after_value={"status": CollectionStatus.approved.value},
    )
    collection_audit_service.log_action(
        db,
        church_id=collection.church_id,
        action=CollectionAuditAction.receipt_issued,
        actor_user_id=approver_user_id,
        collection_id=collection.id,
        receipt_id=receipt.id,
        after_value={"receipt_number": receipt.receipt_number},
    )


def _reject_one(db: Session, collection: Collection, approver_user_id: int, reason: str) -> None:
    if collection.status != CollectionStatus.pending:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Only pending collections can be rejected")
    collection.status = CollectionStatus.rejected
    collection.rejection_reason = reason
    collection_audit_service.log_action(
        db,
        church_id=collection.church_id,
        action=CollectionAuditAction.collection_rejected,
        actor_user_id=approver_user_id,
        collection_id=collection.id,
        reason=reason,
        after_value={"status": CollectionStatus.rejected.value},
    )


def approve_collection(db: Session, collection: Collection, approver_user_id: int) -> Collection:
    _approve_one(db, collection, approver_user_id)
    _refresh_group_status(collection.collection_group, approver_user_id)
    db.commit()
    return get_collection_or_404(db, collection.id)


def reject_collection(db: Session, collection: Collection, approver_user_id: int, reason: str) -> Collection:
    _reject_one(db, collection, approver_user_id, reason)
    _refresh_group_status(collection.collection_group, approver_user_id)
    db.commit()
    return get_collection_or_404(db, collection.id)


def approve_all_pending(db: Session, group: CollectionGroup, approver_user_id: int) -> CollectionGroup:
    approved_ids = [c.id for c in group.collections if c.status == CollectionStatus.pending]
    for collection in group.collections:
        if collection.status == CollectionStatus.pending:
            _approve_one(db, collection, approver_user_id)
    _refresh_group_status(group, approver_user_id)
    if approved_ids:
        collection_audit_service.log_action(
            db,
            church_id=group.church_id,
            action=CollectionAuditAction.group_approved_all,
            actor_user_id=approver_user_id,
            collection_group_id=group.id,
            after_value={"approved_collection_ids": approved_ids},
        )
    db.commit()
    return get_group_or_404(db, group.id)


# ---------------------------------------------------------------------------
# Evidence upload — a payment slip/screenshot attached to a collection.
# Never served from a public static mount; streamed back only to callers
# with read access to the collection itself (see the router).
# ---------------------------------------------------------------------------


def save_evidence_file(collection: Collection, upload: UploadFile) -> str:
    original_name = os.path.basename(upload.filename or "evidence")
    target_dir = os.path.join(settings.upload_dir, "collections", str(collection.id))
    os.makedirs(target_dir, exist_ok=True)
    stored_name = f"{uuid.uuid4().hex}_{original_name}"
    target_path = os.path.join(target_dir, stored_name)
    with open(target_path, "wb") as f:
        f.write(upload.file.read())
    return target_path


# ---------------------------------------------------------------------------
# Correction / adjustment workflow — the only path to change an already-
# approved collection. Maker-checker enforced: the decider may never be the
# same user as the requester. `collection_date` is deliberately never
# editable here, keeping backdating risk contained (see also Phase D's
# financial-period controls).
# ---------------------------------------------------------------------------

CORRECTION_LOAD_OPTIONS = (
    selectinload(CollectionCorrection.requested_by),
    selectinload(CollectionCorrection.decided_by),
)


def get_correction_or_404(db: Session, correction_id: int) -> CollectionCorrection:
    correction = db.get(CollectionCorrection, correction_id, options=CORRECTION_LOAD_OPTIONS, populate_existing=True)
    if correction is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Correction not found")
    return correction


def list_corrections_for_collection(db: Session, collection_id: int) -> list[CollectionCorrection]:
    return (
        db.query(CollectionCorrection)
        .options(*CORRECTION_LOAD_OPTIONS)
        .filter(CollectionCorrection.collection_id == collection_id)
        .order_by(CollectionCorrection.requested_at.desc())
        .all()
    )


def request_correction(db: Session, collection: Collection, requester_user_id: int, payload: CollectionCorrectionCreate) -> CollectionCorrection:
    if collection.status != CollectionStatus.approved:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Only an approved collection can have a correction requested")
    existing_pending = (
        db.query(CollectionCorrection)
        .filter(CollectionCorrection.collection_id == collection.id, CollectionCorrection.status == CollectionCorrectionStatus.pending)
        .first()
    )
    if existing_pending is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="This collection already has a pending correction request")

    proposed_items = [] if payload.kind == CollectionCorrectionKind.reversal else [item.model_dump() for item in payload.proposed_items]
    correction = CollectionCorrection(
        collection_id=collection.id,
        kind=payload.kind,
        requested_by_user_id=requester_user_id,
        reason=payload.reason,
        proposed_items=proposed_items,
        status=CollectionCorrectionStatus.pending,
    )
    db.add(correction)
    db.flush()
    collection_audit_service.log_action(
        db,
        church_id=collection.church_id,
        action=CollectionAuditAction.correction_requested,
        actor_user_id=requester_user_id,
        collection_id=collection.id,
        reason=payload.reason,
        after_value={"kind": payload.kind.value, "proposed_items": proposed_items},
    )
    db.commit()
    return get_correction_or_404(db, correction.id)


def decide_correction(db: Session, correction: CollectionCorrection, decider_user_id: int, approve: bool, decision_notes: str | None) -> CollectionCorrection:
    if correction.status != CollectionCorrectionStatus.pending:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="This correction has already been decided")
    if decider_user_id == correction.requested_by_user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="The requester of a correction cannot also approve or reject it")

    collection = get_collection_or_404(db, correction.collection_id)
    correction.decided_by_user_id = decider_user_id
    correction.decided_at = datetime.now(timezone.utc)
    correction.decision_notes = decision_notes

    if not approve:
        correction.status = CollectionCorrectionStatus.rejected
        collection_audit_service.log_action(
            db,
            church_id=collection.church_id,
            action=CollectionAuditAction.correction_rejected,
            actor_user_id=decider_user_id,
            collection_id=collection.id,
            reason=decision_notes,
        )
        db.commit()
        return get_correction_or_404(db, correction.id)

    before_items = [{"collection_type_id": i.collection_type_id, "amount": float(i.amount)} for i in collection.items]
    before_total = float(collection.total_amount)

    # Apply the proposed item set in place (replace, not append) — a
    # reversal's empty set zeroes every existing item.
    for item in list(collection.items):
        db.delete(item)
    db.flush()
    new_total = 0.0
    for proposed in correction.proposed_items:
        amount = float(proposed["amount"])
        new_total += amount
        db.add(CollectionItem(
            collection_id=collection.id,
            collection_type_id=proposed["collection_type_id"],
            amount=amount,
            original_amount=amount,
        ))
    collection.total_amount = new_total
    correction.status = CollectionCorrectionStatus.approved
    db.flush()

    collection_audit_service.log_action(
        db,
        church_id=collection.church_id,
        action=CollectionAuditAction.correction_approved,
        actor_user_id=decider_user_id,
        collection_id=collection.id,
        reason=decision_notes,
        before_value={"items": before_items, "total_amount": before_total},
        after_value={"items": correction.proposed_items, "total_amount": new_total},
    )

    current_receipt = collection.current_receipt
    if current_receipt is not None and current_receipt.status != ReceiptStatus.cancelled:
        receipt_service.correct_receipt(
            db, current_receipt, decider_user_id,
            reason=f"Correction #{correction.id} approved: {correction.reason}",
            corrected_total=new_total,
        )

    db.commit()
    return get_correction_or_404(db, correction.id)
