from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session, selectinload

from app.database import get_db
from app.deps import get_current_user
from app.models import User, UserRole
from app.schemas import LoginRequest, MyPermissionsRead, TokenResponse, UserRead
from app.security import create_access_token, verify_password
from app.services import rbac_service

router = APIRouter(prefix="/auth", tags=["auth"])

USER_LOAD_OPTIONS = (selectinload(User.user_roles).selectinload(UserRole.role),)


@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).options(*USER_LOAD_OPTIONS).filter(User.username == payload.username).first()
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="This account is disabled")

    user.last_login_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(user, attribute_names=["user_roles"])

    token = create_access_token(subject=user.id)
    return TokenResponse(access_token=token, user=UserRead.model_validate(user))


@router.get("/me", response_model=UserRead)
def me(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    return db.get(User, current_user.id, options=USER_LOAD_OPTIONS)


@router.get("/me/permissions", response_model=MyPermissionsRead)
def my_permissions(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Flattened, JSON-safe view of everything the caller is granted right
    now — lets the frontend gate UI (buttons, nav entries) without guessing
    at role names. Always computed fresh, same as the RBAC dependencies
    themselves (see rbac_service.get_user_permission_scopes)."""
    scopes = rbac_service.get_user_permission_scopes(db, current_user)
    return MyPermissionsRead(scopes={code: sorted(churches, key=lambda c: (c is None, c)) for code, churches in scopes.items()})
