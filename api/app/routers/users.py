from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session, selectinload

from app.database import get_db
from app.deps import require_permission
from app.models import User, UserRole
from app.schemas import PasswordResetRequest, UserCreate, UserRead, UserRoleCreate, UserRoleRead, UserRoleUpdate, UserUpdate
from app.security import hash_password

router = APIRouter(prefix="/users", tags=["users"])

USER_LOAD_OPTIONS = (selectinload(User.user_roles).selectinload(UserRole.role),)


def _get_user_or_404(db: Session, user_id: int) -> User:
    # populate_existing=True: several handlers below look up the user once,
    # mutate its user_roles via a separate query, then look it up again to
    # build the response — without this, Session.get() would just hand back
    # the same already-loaded (and now stale) object from the identity map
    # instead of reloading the relationship.
    user = db.get(User, user_id, options=USER_LOAD_OPTIONS, populate_existing=True)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user


@router.get("", response_model=list[UserRead])
def list_users(db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_users"))):
    return db.query(User).options(*USER_LOAD_OPTIONS).order_by(User.username.asc()).all()


@router.post("", response_model=UserRead, status_code=status.HTTP_201_CREATED)
def create_user(payload: UserCreate, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_users"))):
    if db.query(User).filter(User.username == payload.username).first():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already taken")
    user = User(
        username=payload.username,
        password_hash=hash_password(payload.password),
        email=payload.email,
        member_id=payload.member_id,
    )
    db.add(user)
    db.commit()
    return _get_user_or_404(db, user.id)


@router.get("/{user_id}", response_model=UserRead)
def get_user(user_id: int, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_users"))):
    return _get_user_or_404(db, user_id)


@router.put("/{user_id}", response_model=UserRead)
def update_user(user_id: int, payload: UserUpdate, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_users"))):
    user = _get_user_or_404(db, user_id)
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(user, field, value)
    db.commit()
    return _get_user_or_404(db, user_id)


@router.post("/{user_id}/reset-password", response_model=UserRead)
def reset_password(user_id: int, payload: PasswordResetRequest, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_users"))):
    user = _get_user_or_404(db, user_id)
    user.password_hash = hash_password(payload.new_password)
    db.commit()
    return _get_user_or_404(db, user_id)


@router.get("/{user_id}/roles", response_model=list[UserRoleRead])
def list_user_roles(user_id: int, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_user_roles"))):
    user = _get_user_or_404(db, user_id)
    return user.user_roles


@router.post("/{user_id}/roles", status_code=status.HTTP_201_CREATED, response_model=UserRead)
def add_user_role(
    user_id: int,
    payload: UserRoleCreate,
    db: Session = Depends(get_db),
    _scopes=Depends(require_permission("manage_user_roles")),
):
    user = _get_user_or_404(db, user_id)
    user_role = UserRole(user_id=user.id, role_id=payload.role_id, church_id=payload.church_id, is_active=True)
    db.add(user_role)
    db.commit()
    return _get_user_or_404(db, user_id)


@router.put("/{user_id}/roles/{user_role_id}", response_model=UserRead)
def update_user_role(
    user_id: int,
    user_role_id: int,
    payload: UserRoleUpdate,
    db: Session = Depends(get_db),
    _scopes=Depends(require_permission("manage_user_roles")),
):
    user_role = db.get(UserRole, user_role_id)
    if user_role is None or user_role.user_id != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User role not found")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(user_role, field, value)
    db.commit()
    return _get_user_or_404(db, user_id)


@router.delete("/{user_id}/roles/{user_role_id}", response_model=UserRead)
def remove_user_role(
    user_id: int,
    user_role_id: int,
    db: Session = Depends(get_db),
    _scopes=Depends(require_permission("manage_user_roles")),
):
    user_role = db.get(UserRole, user_role_id)
    if user_role is None or user_role.user_id != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User role not found")
    db.delete(user_role)
    db.commit()
    return _get_user_or_404(db, user_id)
