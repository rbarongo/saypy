from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_user, require_permission
from app.models import Permission, Role, RolePermission
from app.schemas import RoleCreate, RoleRead, RolePermissionRead, RolePermissionSetRequest, RoleUpdate

router = APIRouter(prefix="/roles", tags=["roles"])


@router.get("", response_model=list[RoleRead])
def list_roles(db: Session = Depends(get_db), _user=Depends(get_current_user)):
    return db.query(Role).order_by(Role.name.asc()).all()


@router.post("", response_model=RoleRead, status_code=status.HTTP_201_CREATED)
def create_role(payload: RoleCreate, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_roles"))):
    if db.query(Role).filter(Role.code == payload.code).first():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A role with this code already exists")
    role = Role(**payload.model_dump())
    db.add(role)
    db.commit()
    db.refresh(role)
    return role


@router.get("/{role_id}", response_model=RoleRead)
def get_role(role_id: int, db: Session = Depends(get_db), _user=Depends(get_current_user)):
    role = db.get(Role, role_id)
    if role is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Role not found")
    return role


@router.put("/{role_id}", response_model=RoleRead)
def update_role(role_id: int, payload: RoleUpdate, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_roles"))):
    role = db.get(Role, role_id)
    if role is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Role not found")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(role, field, value)
    db.commit()
    db.refresh(role)
    return role


@router.delete("/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_role(role_id: int, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_roles"))):
    role = db.get(Role, role_id)
    if role is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Role not found")
    db.delete(role)
    db.commit()


@router.get("/{role_id}/permissions", response_model=list[RolePermissionRead])
def get_role_permissions(role_id: int, db: Session = Depends(get_db), _user=Depends(get_current_user)):
    role = db.get(Role, role_id)
    if role is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Role not found")

    granted = {rp.permission_id: rp.is_allowed for rp in db.query(RolePermission).filter(RolePermission.role_id == role_id).all()}
    permissions = db.query(Permission).order_by(Permission.code.asc()).all()
    return [
        RolePermissionRead(
            permission_id=p.id, code=p.code, name=p.name, description=p.description,
            is_allowed=granted.get(p.id, False),
        )
        for p in permissions
    ]


@router.put("/{role_id}/permissions/{permission_id}", response_model=RolePermissionRead)
def set_role_permission(
    role_id: int,
    permission_id: int,
    payload: RolePermissionSetRequest,
    db: Session = Depends(get_db),
    _scopes=Depends(require_permission("manage_permissions")),
):
    role = db.get(Role, role_id)
    if role is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Role not found")
    permission = db.get(Permission, permission_id)
    if permission is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Permission not found")

    row = db.query(RolePermission).filter(RolePermission.role_id == role_id, RolePermission.permission_id == permission_id).first()
    if row is None:
        row = RolePermission(role_id=role_id, permission_id=permission_id, is_allowed=payload.is_allowed)
        db.add(row)
    else:
        row.is_allowed = payload.is_allowed
    db.commit()

    return RolePermissionRead(
        permission_id=permission.id, code=permission.code, name=permission.name,
        description=permission.description, is_allowed=payload.is_allowed,
    )
