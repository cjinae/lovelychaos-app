"""add teacher contacts

Revision ID: 20260318_0010
Revises: 20260318_0009
Create Date: 2026-03-18 00:30:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260318_0010"
down_revision: Union[str, Sequence[str], None] = "20260318_0009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_contacts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("child_id", sa.Integer(), nullable=False),
        sa.Column("teacher_name", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("teacher_email", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="active"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["child_id"], ["children.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("child_id", "teacher_email", name="uq_teacher_contact_child_email"),
    )
    op.create_index(op.f("ix_teacher_contacts_child_id"), "teacher_contacts", ["child_id"], unique=False)
    op.create_index(op.f("ix_teacher_contacts_teacher_email"), "teacher_contacts", ["teacher_email"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_teacher_contacts_teacher_email"), table_name="teacher_contacts")
    op.drop_index(op.f("ix_teacher_contacts_child_id"), table_name="teacher_contacts")
    op.drop_table("teacher_contacts")
