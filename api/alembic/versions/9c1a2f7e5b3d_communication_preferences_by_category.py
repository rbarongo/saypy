"""communication preferences by category

Revision ID: 9c1a2f7e5b3d
Revises: fd20e012b6b3
Create Date: 2026-08-27 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9c1a2f7e5b3d'
down_revision: Union[str, Sequence[str], None] = 'fd20e012b6b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Unlike create_table (which creates the enum type as part of the DDL),
    # add_column needs the Postgres enum type created explicitly first.
    category_enum = sa.Enum('general', 'offering_receipts', 'church_announcements', name='communication_category')
    category_enum.create(op.get_bind(), checkfirst=True)
    # server_default so this NOT NULL column can be added to a table that
    # already has rows — every existing preference row predates categories,
    # so it's treated as 'general' (matches the model's Python-side default
    # and Member.preferred_channels' own definition of that bucket).
    op.add_column('member_communication_preferences', sa.Column(
        'category', category_enum, nullable=False, server_default='general',
    ))
    op.drop_constraint('uq_member_comm_channel', 'member_communication_preferences', type_='unique')
    op.create_unique_constraint(
        'uq_member_comm_category_channel', 'member_communication_preferences', ['member_id', 'category', 'channel']
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('uq_member_comm_category_channel', 'member_communication_preferences', type_='unique')
    op.create_unique_constraint('uq_member_comm_channel', 'member_communication_preferences', ['member_id', 'channel'])
    op.drop_column('member_communication_preferences', 'category')
    sa.Enum(name='communication_category').drop(op.get_bind(), checkfirst=True)
