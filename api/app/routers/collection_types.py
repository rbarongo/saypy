from app.models import CollectionType
from app.routers.church_scoped_lookup import build_church_scoped_lookup_router
from app.schemas import CollectionTypeCreate, CollectionTypeRead, CollectionTypeUpdate

router = build_church_scoped_lookup_router(
    model=CollectionType,
    prefix="/collection-types",
    tag="collection-types",
    write_permission="manage_collection_types",
    read_permissions=("manage_collection_types", "manage_collections", "review_collections", "view_collections"),
    entity_label="collection type",
    create_schema=CollectionTypeCreate,
    update_schema=CollectionTypeUpdate,
    read_schema=CollectionTypeRead,
)
