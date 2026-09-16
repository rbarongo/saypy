from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Permission, Role, RolePermission, User, UserRole

PermissionScopes = dict[str, set[int | None]]


def get_user_permission_scopes(db: Session, user: User) -> PermissionScopes:
    """Resolve everything `user` is allowed to do right now.

    Returns {permission_code: set(church_id | None)}. `None` in the set means
    a global grant (applies to every church) for that permission. Always
    queried fresh from the DB — nothing is cached in the JWT — so revoking a
    role takes effect on the user's very next request.
    """
    rows = db.execute(
        select(RolePermission.permission_id, Permission.code, UserRole.church_id)
        .join(Permission, Permission.id == RolePermission.permission_id)
        .join(UserRole, UserRole.role_id == RolePermission.role_id)
        .join(Role, Role.id == UserRole.role_id)
        .where(
            UserRole.user_id == user.id,
            UserRole.is_active.is_(True),
            RolePermission.is_allowed.is_(True),
        )
    ).all()

    scopes: PermissionScopes = {}
    for _permission_id, code, church_id in rows:
        scopes.setdefault(code, set()).add(church_id)
    return scopes


def has_permission(scopes: PermissionScopes, code: str) -> bool:
    return code in scopes and len(scopes[code]) > 0


def accessible_churches(scopes: PermissionScopes, code: str) -> set[int] | None:
    """None = unrestricted (at least one role grants this permission globally)."""
    granted = scopes.get(code) or set()
    if not granted:
        return set()
    if None in granted:
        return None
    return granted
