from sqlalchemy.orm import Session

from app.models import (
    Church,
    ContactType,
    HouseholdRole,
    MembershipCategory,
    MemberStatus,
    Nationality,
    Permission,
    Role,
    RolePermission,
    User,
    UserRole,
)
from app.security import hash_password
from app.settings import settings

HOUSEHOLD_ROLES = [
    ("head_of_family", "Head of Family"),
    ("spouse", "Spouse"),
    ("child", "Child"),
    ("dependent", "Dependent"),
    ("single_adult", "Single Adult"),
]

MEMBERSHIP_CATEGORIES = [
    ("full_member", "Full Member"),
    ("associate_member", "Associate Member"),
    ("baptized", "Baptized"),
    ("non_baptized", "Non-Baptized"),
    ("clergy", "Clergy"),
    ("visitor", "Visitor"),
]

MEMBER_STATUSES = [
    ("active", "Active"),
    ("inactive", "Inactive"),
    ("transferred", "Transferred"),
    ("deceased", "Deceased"),
]

CONTACT_TYPES = [
    ("mobile", "Mobile Phone"),
    ("email", "Email"),
    ("landline", "Landline"),
    ("whatsapp", "WhatsApp"),
]

NATIONALITIES = [
    ("TZ", "Tanzania", "+255"),
    ("KE", "Kenya", "+254"),
    ("UG", "Uganda", "+256"),
    ("OTHER", "Other", None),
]

# code -> (name, description)
PERMISSIONS = {
    "manage_churches": ("Manage Churches", "Create, edit, and delete churches"),
    "manage_reference_data": ("Manage Reference Data", "Manage household roles, membership categories, statuses, nationalities, contact types"),
    "manage_families": ("Manage Families", "Create, edit, and delete families"),
    "manage_members": ("Manage Members", "Create, edit, and delete members and their contacts"),
    "view_members": ("View Members", "Read-only access to member records"),
    "manage_roles": ("Manage Roles", "Create, edit, and delete roles"),
    "manage_permissions": ("Manage Permissions", "Create, edit, and delete permissions, and assign them to roles"),
    "manage_user_roles": ("Manage User Roles", "Assign and revoke roles for users"),
    "manage_users": ("Manage Users", "Create, edit, and manage login accounts"),
    "manage_collection_types": ("Manage Collection Types", "Create, edit, and delete collection types"),
    "manage_collections": ("Manage Collections", "Create and import collections (saved as pending)"),
    "review_collections": ("Review Collections", "Approve or reject pending collections and issue receipts"),
    "view_collections": ("View Collections", "Read-only access to collections and collection groups"),
    "review_members": ("Review Members", "Approve or reject a member's pending assisted registration"),
    "correct_collections": ("Correct Collections", "Request a correction, adjustment, or reversal on an approved collection"),
    "manage_receipts": ("Manage Receipts", "Reprint, cancel, or correct receipts; administer receipt-numbering series"),
    "view_treasury_audit": ("View Treasury Audit Log", "Read-only access to the treasury/collections audit trail"),
}

CHURCHES = [
    ("KIB", "Kibada"),
    ("KCC", "KCC"),
    ("MWE", "Mwera"),
    ("GOR", "Goroka"),
]


def _seed_lookup(db: Session, model, rows: list[tuple[str, str]]) -> None:
    if db.query(model).count() > 0:
        return
    for code, name in rows:
        db.add(model(code=code, name=name))
    db.commit()


def run(db: Session) -> None:
    if db.query(Church).count() == 0:
        for code, name in CHURCHES:
            db.add(Church(code=code, name=name, app_name="Church Offerings"))
        db.commit()

    _seed_lookup(db, HouseholdRole, HOUSEHOLD_ROLES)
    _seed_lookup(db, MembershipCategory, MEMBERSHIP_CATEGORIES)
    _seed_lookup(db, MemberStatus, MEMBER_STATUSES)
    _seed_lookup(db, ContactType, CONTACT_TYPES)

    if db.query(Nationality).count() == 0:
        for code, name, dial_code in NATIONALITIES:
            db.add(Nationality(code=code, name=name, dial_code=dial_code))
        db.commit()

    # Additive rather than empty-table-only: new permission codes introduced
    # in later releases (e.g. the collections module) must still get seeded
    # into an already-populated deployment, not just a brand-new database.
    existing_permission_codes = {p.code for p in db.query(Permission).all()}
    for code, (name, description) in PERMISSIONS.items():
        if code not in existing_permission_codes:
            db.add(Permission(code=code, name=name, description=description))
    db.commit()

    system_admin_role = db.query(Role).filter(Role.code == "system_admin").first()
    if system_admin_role is None:
        system_admin_role = Role(code="system_admin", name="System Administrator")
        db.add(system_admin_role)
        db.commit()
        db.refresh(system_admin_role)

    granted_ids = {
        rp.permission_id
        for rp in db.query(RolePermission).filter(RolePermission.role_id == system_admin_role.id).all()
    }
    for permission in db.query(Permission).all():
        if permission.id not in granted_ids:
            db.add(RolePermission(role_id=system_admin_role.id, permission_id=permission.id, is_allowed=True))
    db.commit()

    bootstrap_user = db.query(User).filter(User.username == settings.seed_admin_username).first()
    if bootstrap_user is None:
        bootstrap_user = User(
            username=settings.seed_admin_username,
            password_hash=hash_password(settings.seed_admin_password),
            email=None,
            is_active=True,
        )
        db.add(bootstrap_user)
        db.commit()
        db.refresh(bootstrap_user)

    has_admin_role = (
        db.query(UserRole)
        .filter(UserRole.user_id == bootstrap_user.id, UserRole.role_id == system_admin_role.id)
        .first()
    )
    if has_admin_role is None:
        db.add(UserRole(user_id=bootstrap_user.id, role_id=system_admin_role.id, church_id=None, is_active=True))
        db.commit()
