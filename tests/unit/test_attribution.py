from sqlalchemy import select

from app.models import User
from app.services.attribution import resolve_admin_sender


def test_attribution_zero_match(db_session):
    result = resolve_admin_sender(db_session, "nobody@example.com")
    assert result.kind == "unverified"


def test_attribution_single_match(db_session):
    result = resolve_admin_sender(db_session, "admin@example.com")
    assert result.kind == "ok"
    assert result.user is not None


def test_attribution_second_verified_admin_same_household(db_session):
    db_session.add(User(household_id=1, email="parent2@example.com", is_admin=True, verified=True))
    db_session.commit()
    result = resolve_admin_sender(db_session, "parent2@example.com")
    assert result.kind == "ok"
    assert result.user is not None
    assert result.user.household_id == 1


def test_attribution_ambiguous_match(db_session):
    db_session.add(User(household_id=1, email="admin@example.com", is_admin=True, verified=True))
    db_session.commit()
    result = resolve_admin_sender(db_session, "admin@example.com")
    assert result.kind == "ambiguous"


def test_tenant_scope_query_example(db_session):
    users = db_session.scalars(select(User).where(User.household_id == 1)).all()
    assert all(u.household_id == 1 for u in users)
