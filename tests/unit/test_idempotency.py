import pytest
from sqlalchemy.exc import IntegrityError

from app.models import IdempotencyKey


def test_idempotency_unique_constraint(db_session):
    key = IdempotencyKey(key="k1", scope="webhook", household_id=1, action_type="ingest", target_ref="1", result_hash="h")
    db_session.add(key)
    db_session.commit()

    db_session.add(IdempotencyKey(key="k1", scope="webhook", household_id=1, action_type="ingest", target_ref="1", result_hash="h"))
    with pytest.raises(IntegrityError):
        db_session.commit()
