from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps import church_scope_for, enforce_church_scope, get_permission_scopes, require_permission
from app.schemas import ChurchScopedLookupCreate, ChurchScopedLookupRead, ChurchScopedLookupUpdate
from app.services import rbac_service
from app.services.rbac_service import PermissionScopes, has_permission


def build_church_scoped_lookup_router(
    *,
    model,
    prefix: str,
    tag: str,
    write_permission: str,
    read_permissions: tuple[str, ...],
    entity_label: str,
    create_schema=ChurchScopedLookupCreate,
    update_schema=ChurchScopedLookupUpdate,
    read_schema=ChurchScopedLookupRead,
) -> APIRouter:
    """A `{id, code, name, church_id, status}` lookup, church-scoped (NULL
    church_id = a global default available to every church) — the same
    shape first written out for `collection_types.py`, generalized here so
    Organisations/Sabbath School Groups/Church Roles don't each duplicate it,
    matching the dedup already done for pure global lookups via
    `app/routers/lookup.py`'s `build_lookup_router`."""
    router = APIRouter(prefix=prefix, tags=[tag])
    write_scope = church_scope_for(write_permission)

    def _read_scope(scopes: PermissionScopes = Depends(get_permission_scopes)) -> Optional[set[int]]:
        per_permission = [rbac_service.accessible_churches(scopes, code) for code in read_permissions]
        if any(s is None for s in per_permission):
            return None
        union: set[int] = set()
        for s in per_permission:
            union |= s
        return union

    def _require_any_read_permission(scopes: PermissionScopes = Depends(get_permission_scopes)) -> PermissionScopes:
        if not any(has_permission(scopes, code) for code in read_permissions):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Missing required permission to view {entity_label}")
        return scopes

    def _get_or_404(db: Session, item_id: int):
        item = db.get(model, item_id)
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"{entity_label.capitalize()} not found")
        return item

    def _enforce_write_scope(item_church_id: Optional[int], scope: Optional[set[int]]) -> None:
        if item_church_id is not None:
            enforce_church_scope(item_church_id, scope, context=f"this {entity_label}")
        elif scope is not None:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Only unrestricted roles can manage a global {entity_label}")

    @router.get("", response_model=list[read_schema])
    def list_items(
        church_id: Optional[int] = None,
        db: Session = Depends(get_db),
        scope: Optional[set[int]] = Depends(_read_scope),
        _perm=Depends(_require_any_read_permission),
    ):
        query = db.query(model)
        if scope is not None:
            if church_id is not None and church_id not in scope:
                query = query.filter(False)
            elif church_id is not None:
                query = query.filter((model.church_id == church_id) | (model.church_id.is_(None)))
            else:
                query = query.filter(model.church_id.in_(scope) | model.church_id.is_(None) if scope else model.church_id.is_(None))
        elif church_id is not None:
            query = query.filter((model.church_id == church_id) | (model.church_id.is_(None)))
        return query.order_by(model.name.asc()).all()

    @router.post("", response_model=read_schema, status_code=status.HTTP_201_CREATED)
    def create_item(
        payload: create_schema,
        db: Session = Depends(get_db),
        scope: Optional[set[int]] = Depends(write_scope),
        _perm=Depends(require_permission(write_permission)),
    ):
        if payload.church_id is None:
            if scope is not None:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Only unrestricted roles can create a global {entity_label}")
            effective_church_id = None
        else:
            effective_church_id = enforce_church_scope(payload.church_id, scope, context=f"{entity_label}s")

        exists = db.query(model).filter(model.code == payload.code, model.church_id == effective_church_id).first()
        if exists:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"A {entity_label} with this code already exists for this church")

        item = model(code=payload.code, name=payload.name, church_id=effective_church_id, status=payload.status)
        db.add(item)
        db.commit()
        db.refresh(item)
        return item

    @router.put("/{item_id}", response_model=read_schema)
    def update_item(
        item_id: int,
        payload: update_schema,
        db: Session = Depends(get_db),
        scope: Optional[set[int]] = Depends(write_scope),
        _perm=Depends(require_permission(write_permission)),
    ):
        item = _get_or_404(db, item_id)
        _enforce_write_scope(item.church_id, scope)
        for field, value in payload.model_dump(exclude_unset=True).items():
            setattr(item, field, value)
        db.commit()
        db.refresh(item)
        return item

    @router.delete("/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
    def delete_item(
        item_id: int,
        db: Session = Depends(get_db),
        scope: Optional[set[int]] = Depends(write_scope),
        _perm=Depends(require_permission(write_permission)),
    ):
        item = _get_or_404(db, item_id)
        _enforce_write_scope(item.church_id, scope)
        db.delete(item)
        db.commit()

    return router
