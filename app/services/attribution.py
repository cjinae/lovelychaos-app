from __future__ import annotations

from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import User


class AttributionResult:
    def __init__(self, kind: str, user: Optional[User] = None):
        self.kind = kind
        self.user = user


def resolve_admin_sender(db: Session, sender: str) -> AttributionResult:
    normalized = sender.strip().lower()
    users = db.scalars(
        select(User).where(User.email == normalized, User.is_admin.is_(True), User.verified.is_(True))
    ).all()
    if len(users) == 0:
        return AttributionResult("unverified")
    if len(users) > 1:
        return AttributionResult("ambiguous")
    return AttributionResult("ok", users[0])


def resolve_admin_phone(db: Session, sender_phone: str) -> AttributionResult:
    normalized = sender_phone.strip()
    users = db.scalars(
        select(User).where(User.phone == normalized, User.is_admin.is_(True), User.verified.is_(True))
    ).all()
    if len(users) == 0:
        return AttributionResult("unverified")
    if len(users) > 1:
        return AttributionResult("ambiguous")
    return AttributionResult("ok", users[0])
