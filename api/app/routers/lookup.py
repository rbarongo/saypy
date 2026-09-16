from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import get_current_user, require_permission
from app.models import ContactType, HouseholdRole, MemberStatus, MembershipCategory, Nationality
from app.schemas import (
    LookupCreate,
    LookupRead,
    LookupUpdate,
    NationalityCreate,
    NationalityRead,
    NationalityUpdate,
)

REFERENCE_DATA_PERMISSION = "manage_reference_data"


def build_lookup_router(*, model, prefix: str, tag: str, create_schema=LookupCreate, update_schema=LookupUpdate, read_schema=LookupRead):
    """A {code, name, ...} lookup table only needs this one shape of CRUD —
    used for household roles, membership categories, member statuses,
    nationalities, and contact types instead of duplicating 5 near-identical
    routers. Reads are open to any authenticated user (dropdowns everywhere
    need them); writes require `manage_reference_data`."""
    router = APIRouter(prefix=prefix, tags=[tag])

    @router.get("", response_model=list[read_schema])
    def list_items(db: Session = Depends(get_db), _user=Depends(get_current_user)):
        return db.query(model).order_by(model.name.asc()).all()

    @router.post("", response_model=read_schema, status_code=status.HTTP_201_CREATED)
    def create_item(payload: create_schema, db: Session = Depends(get_db), _scopes=Depends(require_permission(REFERENCE_DATA_PERMISSION))):
        if db.query(model).filter(model.code == payload.code).first():
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Code already exists")
        item = model(**payload.model_dump())
        db.add(item)
        db.commit()
        db.refresh(item)
        return item

    @router.get("/{item_id}", response_model=read_schema)
    def get_item(item_id: int, db: Session = Depends(get_db), _user=Depends(get_current_user)):
        item = db.get(model, item_id)
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
        return item

    @router.put("/{item_id}", response_model=read_schema)
    def update_item(item_id: int, payload: update_schema, db: Session = Depends(get_db), _scopes=Depends(require_permission(REFERENCE_DATA_PERMISSION))):
        item = db.get(model, item_id)
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
        for field, value in payload.model_dump(exclude_unset=True).items():
            setattr(item, field, value)
        db.commit()
        db.refresh(item)
        return item

    @router.delete("/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
    def delete_item(item_id: int, db: Session = Depends(get_db), _scopes=Depends(require_permission(REFERENCE_DATA_PERMISSION))):
        item = db.get(model, item_id)
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
        db.delete(item)
        db.commit()

    return router


household_roles_router = build_lookup_router(model=HouseholdRole, prefix="/household-roles", tag="household-roles")
membership_categories_router = build_lookup_router(model=MembershipCategory, prefix="/membership-categories", tag="membership-categories")
member_statuses_router = build_lookup_router(model=MemberStatus, prefix="/member-statuses", tag="member-statuses")
contact_types_router = build_lookup_router(model=ContactType, prefix="/contact-types", tag="contact-types")
nationalities_router = build_lookup_router(
    model=Nationality,
    prefix="/nationalities",
    tag="nationalities",
    create_schema=NationalityCreate,
    update_schema=NationalityUpdate,
    read_schema=NationalityRead,
)

all_lookup_routers = [
    household_roles_router,
    membership_categories_router,
    member_statuses_router,
    contact_types_router,
    nationalities_router,
]
