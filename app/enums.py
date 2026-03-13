from enum import Enum


class WebhookStatus(str, Enum):
    INGESTION_ACCEPTED = "ingestion_accepted"
    COMMAND_ACCEPTED = "command_accepted_for_processing"
    COMMAND_COMPLETED = "command_completed"
    COMMAND_NEEDS_CLARIFICATION = "command_needs_clarification"
    REJECTED_UNVERIFIED = "rejected_unverified_sender"
    REJECTED_AMBIGUOUS = "rejected_ambiguous_sender"
    REJECTED_UNAUTHORIZED = "rejected_unauthorized"
    REJECTED_TENANT_MISMATCH = "rejected_tenant_mismatch"
    REJECTED_VALIDATION = "rejected_validation"


class ProcessingState(str, Enum):
    QUEUED = "queued"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
