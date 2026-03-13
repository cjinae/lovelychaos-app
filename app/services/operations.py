from __future__ import annotations

import uuid
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.enums import ProcessingState, WebhookStatus
from app.models import Operation


class NotificationSender:
    def __init__(self):
        self.fail_until_attempt = 0

    def send_completion(self, operation: Operation) -> bool:
        if operation.notify_attempts < self.fail_until_attempt:
            return False
        return True


def create_operation(db: Session, household_id: int, message: str) -> Operation:
    op = Operation(
        operation_id=str(uuid.uuid4()),
        household_id=household_id,
        status=WebhookStatus.COMMAND_ACCEPTED.value,
        processing_state=ProcessingState.QUEUED.value,
        mutation_executed=False,
        user_message=message,
    )
    db.add(op)
    db.flush()
    return op


def process_operation(db: Session, operation_id: str, notifier: NotificationSender) -> Optional[Operation]:
    op = db.scalar(select(Operation).where(Operation.operation_id == operation_id))
    if not op:
        return None

    # Dedup terminal operations.
    if op.notification_status in {"sent", "failed"}:
        return op

    if op.processing_state == ProcessingState.QUEUED.value:
        op.processing_state = ProcessingState.IN_PROGRESS.value

    # Command mutation is considered complete before outbound completion-notification delivery.
    op.status = WebhookStatus.COMMAND_COMPLETED.value
    op.mutation_executed = True
    op.user_message = "Command processed"

    op.notify_attempts += 1
    if notifier.send_completion(op):
        op.notification_status = "sent"
        op.processing_state = ProcessingState.COMPLETED.value
        op.user_message = "Command processed and completion notification sent"
    else:
        if op.notify_attempts >= 3:
            op.notification_status = "failed"
            op.processing_state = ProcessingState.FAILED.value
            op.user_message = "Command processed but completion notification failed after max retries"
        else:
            op.notification_status = "retrying"
            op.processing_state = ProcessingState.IN_PROGRESS.value
            op.user_message = "Command processed, notification retry scheduled"
    db.flush()
    return op
