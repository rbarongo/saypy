from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_user, require_permission
from app.models import Church
from app.schemas import ChurchCreate, ChurchRead, ChurchUpdate

router = APIRouter(prefix="/churches", tags=["churches"])


@router.get("", response_model=list[ChurchRead])
def list_churches(db: Session = Depends(get_db), _user=Depends(get_current_user)):
    return db.query(Church).order_by(Church.name.asc()).all()


@router.post("", response_model=ChurchRead, status_code=status.HTTP_201_CREATED)
def create_church(payload: ChurchCreate, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_churches"))):
    if db.query(Church).filter((Church.name == payload.name) | (Church.code == payload.code)).first():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A church with this name or code already exists")
    church = Church(**payload.model_dump())
    db.add(church)
    db.commit()
    db.refresh(church)
    return church


@router.get("/{church_id}", response_model=ChurchRead)
def get_church(church_id: int, db: Session = Depends(get_db), _user=Depends(get_current_user)):
    church = db.get(Church, church_id)
    if church is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Church not found")
    return church


@router.put("/{church_id}", response_model=ChurchRead)
def update_church(church_id: int, payload: ChurchUpdate, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_churches"))):
    church = db.get(Church, church_id)
    if church is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Church not found")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(church, field, value)
    db.commit()
    db.refresh(church)
    return church


@router.delete("/{church_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_church(church_id: int, db: Session = Depends(get_db), _scopes=Depends(require_permission("manage_churches"))):
    church = db.get(Church, church_id)
    if church is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Church not found")
    db.delete(church)
    db.commit()
