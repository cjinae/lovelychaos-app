from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import PreferenceRule


def resolve_execution_disposition(db: Session, household_id: int, category: str, default_disposition: str) -> dict:
    rules = db.scalars(
        select(PreferenceRule)
        .where(
            PreferenceRule.household_id == household_id,
            PreferenceRule.category == category,
            PreferenceRule.enabled.is_(True),
        )
        .order_by(PreferenceRule.priority.desc())
    ).all()
    if rules:
        top = rules[0]
        return {"execution_disposition": default_disposition, "source": top.source}
    return {"execution_disposition": default_disposition, "source": "system_default"}
