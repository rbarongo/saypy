from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Member, User
from app.security import decode_access_token
from app.services import rbac_service
from app.services.rbac_service import PermissionScopes

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)


def get_current_user(
    token: Optional[str] = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> User:
    credentials_error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if not token:
        raise credentials_error
    try:
        payload = decode_access_token(token)
        user_id = int(payload.get("sub"))
    except (ValueError, TypeError):
        raise credentials_error

    user = db.get(User, user_id)
    if user is None or not user.is_active:
        raise credentials_error
    return user


def get_current_member(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Member:
    """For the self-service `/me`-style endpoints: resolves the calling
    account's own member record. No RBAC permission check — a member is
    always allowed to read/act on their own record; 403s only if this
    account isn't linked to one at all (a staff/admin account, typically)."""
    if user.member_id is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="This account isn't linked to a member record")
    member = db.get(Member, user.member_id)
    if member is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Linked member record not found")
    return member


def get_permission_scopes(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> PermissionScopes:
    return rbac_service.get_user_permission_scopes(db, user)


def require_permission(code: str):
    def _checker(scopes: PermissionScopes = Depends(get_permission_scopes)) -> PermissionScopes:
        if not rbac_service.has_permission(scopes, code):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing required permission: {code}",
            )
        return scopes

    return _checker


def church_scope_for(code: str):
    """Dependency returning the set of church ids `code` is granted for, or
    None if unrestricted. Does NOT itself require the permission — combine
    with `require_permission(code)` on the route when a permission is
    mandatory for the whole endpoint."""

    def _resolver(scopes: PermissionScopes = Depends(get_permission_scopes)) -> Optional[set[int]]:
        return rbac_service.accessible_churches(scopes, code)

    return _resolver


def enforce_church_scope(
    target_church_id: Optional[int],
    accessible: Optional[set[int]],
    context: str = "this record",
) -> int:
    """Resolve the effective church id for a write, or raise 403/400."""
    if accessible is None:
        if target_church_id is None:
            raise HTTPException(status_code=400, detail="church_id is required")
        return int(target_church_id)
    if not accessible:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Not authorized for {context}")
    if target_church_id is None:
        if len(accessible) == 1:
            return next(iter(accessible))
        raise HTTPException(status_code=400, detail="church_id is required")
    if int(target_church_id) not in accessible:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Restricted: your account cannot access {context} for another church",
        )
    return int(target_church_id)
