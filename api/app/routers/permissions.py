from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_user, require_permission
from app.models import Permission
from app.schemas import PermissionCreate, PermissionRead, PermissionUpdate

router = APIRouter(prefix="/permissions", tags=["permissions"])


@router.get("", response_model=list[PermissionRead])
def list_permissions(db: Session = Depends(get_db), _user=Depends(get_current_user)):
    return db.query(Permission).order_by(Permission.code.asc()).all()


@router.post("", response_model=PermissionRead, status_code=status.HTTP_201_CREATED)
def create_permission(payload: PermissionCreate, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_permissions"))):
    if db.query(Permission).filter(Permission.code == payload.code).first():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A permission with this code already exists")
    permission = Permission(**payload.model_dump())
    db.add(permission)
    db.commit()
    db.refresh(permission)
    return permission


@router.get("/{permission_id}", response_model=PermissionRead)
def get_permission(permission_id: int, db: Session = Depends(get_db), _user=Depends(get_current_user)):
    permission = db.get(Permission, permission_id)
    if permission is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Permission not found")
    return permission


@router.put("/{permission_id}", response_model=PermissionRead)
def update_permission(permission_id: int, payload: PermissionUpdate, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_permissions"))):
    permission = db.get(Permission, permission_id)
    if permission is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Permission not found")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(permission, field, value)
    db.commit()
    db.refresh(permission)
    return permission


@router.delete("/{permission_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_permission(permission_id: int, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_permissions"))):
    permission = db.get(Permission, permission_id)
    if permission is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Permission not found")
    db.delete(permission)
    db.commit()
