from app.models import ChurchRole, Organisation, SabbathSchoolGroup
from app.routers.church_scoped_lookup import build_church_scoped_lookup_router

READ_PERMISSIONS = ("manage_reference_data", "manage_members", "view_members")

organisations_router = build_church_scoped_lookup_router(
    model=Organisation,
    prefix="/organisations",
    tag="organisations",
    write_permission="manage_reference_data",
    read_permissions=READ_PERMISSIONS,
    entity_label="organisation",
)

sabbath_school_groups_router = build_church_scoped_lookup_router(
    model=SabbathSchoolGroup,
    prefix="/sabbath-school-groups",
    tag="sabbath-school-groups",
    write_permission="manage_reference_data",
    read_permissions=READ_PERMISSIONS,
    entity_label="Sabbath School group",
)

church_roles_router = build_church_scoped_lookup_router(
    model=ChurchRole,
    prefix="/church-roles",
    tag="church-roles",
    write_permission="manage_reference_data",
    read_permissions=READ_PERMISSIONS,
    entity_label="church role",
)

all_member_lookup_routers = [organisations_router, sabbath_school_groups_router, church_roles_router]
