PAYLOAD_CLEAN = {
    "provider": "mock-email",
    "provider_event_id": "evt-1",
    "provider_message_id": "msg-1",
    "sender": "admin@example.com",
    "recipient_alias": "schedule@example.com",
    "subject": "School Closure Alert",
    "body_text": "School closure on 2099-10-01 08:30 due to weather.",
}

PAYLOAD_UNKNOWN_SENDER = {
    **PAYLOAD_CLEAN,
    "provider_event_id": "evt-unknown",
    "provider_message_id": "msg-unknown",
    "sender": "intruder@example.com",
}

PAYLOAD_LOW_CONFIDENCE = {
    **PAYLOAD_CLEAN,
    "provider_event_id": "evt-low",
    "provider_message_id": "msg-low",
    "body_text": "unclear update maybe some day.",
}

PAYLOAD_COMMAND_CONFIRM = {
    **PAYLOAD_CLEAN,
    "provider_event_id": "evt-cmd",
    "provider_message_id": "msg-cmd",
    "body_text": "more info about Pizza Lunch",
}

PAYLOAD_COMMAND_ASYNC = {
    **PAYLOAD_CLEAN,
    "provider_event_id": "evt-async",
    "provider_message_id": "msg-async",
    "body_text": "more info about Pizza Lunch later",
}

PAYLOAD_SMS_CONFIRM = {
    "provider": "mock-sms",
    "provider_event_id": "sms-evt-1",
    "provider_message_id": "sms-msg-1",
    "sender_phone": "+15550000001",
    "recipient_phone": "+15551112222",
    "body_text": "more info about Pizza Lunch",
}
