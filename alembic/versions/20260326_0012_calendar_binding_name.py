"""add calendar_name to calendar_bindings

Revision ID: 20260326_0012
Revises: 20260318_0011
Create Date: 2026-03-26 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260326_0012"
down_revision: Union[str, Sequence[str], None] = "20260318_0011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("calendar_bindings", sa.Column("calendar_name", sa.String(length=255), nullable=True))


def downgrade() -> None:
    op.drop_column("calendar_bindings", "calendar_name")
