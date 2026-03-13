from __future__ import annotations

from dataclasses import dataclass
import uuid

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Household, NotificationDelivery, User


class NotificationSendError(Exception):
    pass


@dataclass
class NotificationResult:
    status: str
    provider_ref: str | None = None


class NotificationProvider:
    def send_email(self, to_email: str, subject: str, body: str) -> NotificationResult:
        raise NotImplementedError

    def send_sms(self, to_phone: str, body: str) -> NotificationResult:
        raise NotImplementedError


class MockNotificationProvider(NotificationProvider):
    def send_email(self, to_email: str, subject: str, body: str) -> NotificationResult:
        return NotificationResult(status="sent", provider_ref=f"mock-email-{uuid.uuid4()}")

    def send_sms(self, to_phone: str, body: str) -> NotificationResult:
        return NotificationResult(status="sent", provider_ref=f"mock-sms-{uuid.uuid4()}")


class ResendNotificationProvider(NotificationProvider):
    def __init__(
        self,
        api_key: str,
        from_email: str,
        twilio_account_sid: str = "",
        twilio_auth_token: str = "",
        twilio_messaging_service_sid: str = "",
        twilio_phone_number: str = "",
    ):
        self.api_key = api_key
        self.from_email = from_email
        self.twilio_account_sid = twilio_account_sid
        self.twilio_auth_token = twilio_auth_token
        self.twilio_messaging_service_sid = twilio_messaging_service_sid
        self.twilio_phone_number = twilio_phone_number

    def send_email(self, to_email: str, subject: str, body: str) -> NotificationResult:
        if not self.api_key or not self.from_email:
            raise NotificationSendError("Resend is not configured")
        with httpx.Client(timeout=10) as client:
            response = client.post(
                "https://api.resend.com/emails",
                json={"from": self.from_email, "to": [to_email], "subject": subject, "text": body},
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            )
        if response.status_code >= 400:
            raise NotificationSendError(f"Resend error: {response.status_code}")
        payload = response.json()
        return NotificationResult(status="sent", provider_ref=payload.get("id"))

    def send_sms(self, to_phone: str, body: str) -> NotificationResult:
        if not self.twilio_account_sid or not self.twilio_auth_token:
            raise NotificationSendError("Twilio is not configured")
        if not self.twilio_messaging_service_sid and not self.twilio_phone_number:
            raise NotificationSendError("Twilio sender is not configured")
        payload = {"To": to_phone, "Body": body}
        if self.twilio_messaging_service_sid:
            payload["MessagingServiceSid"] = self.twilio_messaging_service_sid
        else:
            payload["From"] = self.twilio_phone_number
        with httpx.Client(timeout=10) as client:
            response = client.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{self.twilio_account_sid}/Messages.json",
                data=payload,
                auth=(self.twilio_account_sid, self.twilio_auth_token),
            )
        if response.status_code >= 400:
            raise NotificationSendError(f"Twilio error: {response.status_code}")
        twilio_payload = response.json()
        return NotificationResult(status="sent", provider_ref=twilio_payload.get("sid"))


def dispatch_household_notification(
    db: Session,
    provider: NotificationProvider,
    household_id: int,
    template: str,
    subject: str,
    message: str,
) -> dict:
    household = db.scalar(select(Household).where(Household.id == household_id))
    admin = db.scalar(select(User).where(User.household_id == household_id, User.is_admin.is_(True)))
    if not household or not admin:
        return {"sent": 0, "failed": 0}

    sent = 0
    failed = 0

    def _record(recipient_type: str, channel: str, target: str, result: NotificationResult | None, status: str):
        db.add(
            NotificationDelivery(
                household_id=household_id,
                recipient_type=recipient_type,
                channel=channel,
                target=target,
                template=template,
                message=message,
                status=status,
                provider_ref=result.provider_ref if result else None,
            )
        )

    # Admin email notification.
    try:
        result = provider.send_email(admin.email, subject=subject, body=message)
        _record("admin", "email", admin.email, result, "sent")
        sent += 1
    except Exception:
        _record("admin", "email", admin.email, None, "failed")
        failed += 1

    if admin.phone:
        try:
            result = provider.send_sms(admin.phone, body=message)
            _record("admin", "sms", admin.phone, result, "sent")
            sent += 1
        except Exception:
            _record("admin", "sms", admin.phone, None, "failed")
            failed += 1

    # Spouse receive-only notification path.
    if household.spouse_notifications_enabled and household.spouse_phone:
        try:
            result = provider.send_sms(household.spouse_phone, body=message)
            _record("spouse", "sms", household.spouse_phone, result, "sent")
            sent += 1
        except Exception:
            _record("spouse", "sms", household.spouse_phone, None, "failed")
            failed += 1

    return {"sent": sent, "failed": failed}


def send_email_notification(
    db: Session,
    provider: NotificationProvider,
    household_id: int,
    to_email: str,
    template: str,
    subject: str,
    message: str,
    recipient_type: str = "admin",
) -> dict:
    try:
        result = provider.send_email(to_email, subject=subject, body=message)
        delivery = NotificationDelivery(
            household_id=household_id,
            recipient_type=recipient_type,
            channel="email",
            target=to_email,
            template=template,
            message=message,
            status="sent",
            provider_ref=result.provider_ref,
        )
        db.add(delivery)
        return {"sent": 1, "failed": 0}
    except Exception:
        delivery = NotificationDelivery(
            household_id=household_id,
            recipient_type=recipient_type,
            channel="email",
            target=to_email,
            template=template,
            message=message,
            status="failed",
        )
        db.add(delivery)
        return {"sent": 0, "failed": 1}


def send_channel_notification(
    db: Session,
    provider: NotificationProvider,
    *,
    household_id: int,
    recipient_type: str,
    channel: str,
    target: str,
    template: str,
    subject: str,
    message: str,
) -> dict:
    try:
        if channel == "sms":
            result = provider.send_sms(target, body=message)
        else:
            result = provider.send_email(target, subject=subject, body=message)
        delivery = NotificationDelivery(
            household_id=household_id,
            recipient_type=recipient_type,
            channel=channel,
            target=target,
            template=template,
            message=message,
            status="sent",
            provider_ref=result.provider_ref,
        )
        db.add(delivery)
        return {"sent": 1, "failed": 0}
    except Exception:
        delivery = NotificationDelivery(
            household_id=household_id,
            recipient_type=recipient_type,
            channel=channel,
            target=target,
            template=template,
            message=message,
            status="failed",
        )
        db.add(delivery)
        return {"sent": 0, "failed": 1}
