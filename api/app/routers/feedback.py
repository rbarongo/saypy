from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session, selectinload

from app.database import get_db
from app.deps import require_permission
from app.models import FeedbackStatus, MemberFeedback
from app.schemas import FeedbackRead
from app.services import member_service

router = APIRouter(prefix="/feedback", tags=["feedback"])


@router.get("", response_model=list[FeedbackRead])
def list_feedback(
    status_filter: FeedbackStatus | None = None,
    db: Session = Depends(get_db),
    _perm=Depends(require_permission("manage_members")),
):
    query = db.query(MemberFeedback).options(selectinload(MemberFeedback.member))
    if status_filter is not None:
        query = query.filter(MemberFeedback.status == status_filter)
    return query.order_by(MemberFeedback.created_at.desc()).all()


@router.post("/{feedback_id}/mark-reviewed", response_model=FeedbackRead)
def mark_feedback_reviewed(
    feedback_id: int,
    db: Session = Depends(get_db),
    _perm=Depends(require_permission("manage_members")),
):
    feedback = db.get(MemberFeedback, feedback_id)
    if feedback is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Feedback not found")
    return member_service.mark_feedback_reviewed(db, feedback)
