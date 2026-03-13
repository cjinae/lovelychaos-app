from sqlalchemy import select

from app.models import Household, NotificationDelivery, User
from tests.fixtures import PAYLOAD_CLEAN


def test_spouse_receive_only_notification_sent_on_mutation(client, db_session):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-spouse-notify-1"
    payload["provider_message_id"] = "msg-spouse-notify-1"
    payload["subject"] = "School Closure Spouse Notify"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200
    assert response.json()["status"] == "ingestion_accepted"

    deliveries = db_session.scalars(
        select(NotificationDelivery).where(NotificationDelivery.template == "event_created")
    ).all()
    assert any(d.recipient_type == "admin" and d.status == "sent" for d in deliveries)
    assert any(d.recipient_type == "spouse" and d.status == "sent" for d in deliveries)


def test_spouse_notifications_disabled_skips_spouse_delivery(client, db_session):
    household = db_session.scalar(select(Household).where(Household.id == 1))
    assert household is not None
    household.spouse_notifications_enabled = False
    db_session.commit()

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-spouse-notify-2"
    payload["provider_message_id"] = "msg-spouse-notify-2"
    payload["subject"] = "School Closure No Spouse"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200

    deliveries = db_session.scalars(
        select(NotificationDelivery).where(NotificationDelivery.template == "event_created")
    ).all()
    assert any(d.recipient_type == "admin" for d in deliveries)
    assert not any(d.recipient_type == "spouse" for d in deliveries)


def test_admin_sms_notification_sent_on_mutation_when_phone_configured(client, db_session):
    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-admin-sms-notify-1"
    payload["provider_message_id"] = "msg-admin-sms-notify-1"
    payload["subject"] = "School Closure Admin SMS"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200

    deliveries = db_session.scalars(
        select(NotificationDelivery).where(NotificationDelivery.template == "event_created")
    ).all()
    assert any(d.recipient_type == "admin" and d.channel == "sms" and d.status == "sent" for d in deliveries)


def test_admin_sms_notification_skipped_without_phone(client, db_session):
    user = db_session.scalar(select(User).where(User.household_id == 1, User.is_admin.is_(True)))
    assert user is not None
    user.phone = None
    db_session.commit()

    payload = dict(PAYLOAD_CLEAN)
    payload["provider_event_id"] = "evt-admin-sms-notify-2"
    payload["provider_message_id"] = "msg-admin-sms-notify-2"
    payload["subject"] = "School Closure No Admin SMS"

    response = client.post("/webhooks/email/inbound", json=payload, headers={"x-signature": "local-dev-secret"})
    assert response.status_code == 200

    deliveries = db_session.scalars(
        select(NotificationDelivery).where(NotificationDelivery.template == "event_created")
    ).all()
    assert not any(d.recipient_type == "admin" and d.channel == "sms" for d in deliveries)
