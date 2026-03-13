"""add google calendar integration tables

Revision ID: 20260304_0002
Revises: 20260303_0001
Create Date: 2026-03-04 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260304_0002"
down_revision: Union[str, Sequence[str], None] = "20260303_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "google_credentials",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("provider_user_email", sa.String(length=255), nullable=False),
        sa.Column("token_subject", sa.String(length=255), nullable=False),
        sa.Column("access_token", sa.Text(), nullable=False),
        sa.Column("refresh_token", sa.Text(), nullable=True),
        sa.Column("token_expiry", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_google_credentials_household_id"), "google_credentials", ["household_id"], unique=False)

    op.create_table(
        "calendar_bindings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("household_id", sa.Integer(), nullable=False),
        sa.Column("google_credential_id", sa.Integer(), nullable=False),
        sa.Column("calendar_id", sa.String(length=255), nullable=False),
        sa.Column("calendar_owner_email", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["google_credential_id"], ["google_credentials.id"]),
        sa.ForeignKeyConstraint(["household_id"], ["households.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("household_id", name="uq_calendar_binding_household"),
    )
    op.create_index(op.f("ix_calendar_bindings_google_credential_id"), "calendar_bindings", ["google_credential_id"], unique=False)
    op.create_index(op.f("ix_calendar_bindings_household_id"), "calendar_bindings", ["household_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_calendar_bindings_household_id"), table_name="calendar_bindings")
    op.drop_index(op.f("ix_calendar_bindings_google_credential_id"), table_name="calendar_bindings")
    op.drop_table("calendar_bindings")
    op.drop_index(op.f("ix_google_credentials_household_id"), table_name="google_credentials")
    op.drop_table("google_credentials")
