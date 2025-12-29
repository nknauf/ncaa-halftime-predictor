# app/messaging.py

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from twilio.rest import Client


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AlertConfig:
    sms_enabled: bool
    twilio_account_sid: Optional[str]
    twilio_auth_token: Optional[str]
    twilio_from_number: Optional[str]
    timeout_seconds: int = 10


def alert_config_from_env() -> AlertConfig:
    """
    Load SMS alert configuration from environment variables.
    """
    sid = os.getenv("TWILIO_ACCOUNT_SID")
    token = os.getenv("TWILIO_AUTH_TOKEN")
    from_number = os.getenv("TWILIO_FROM_NUMBER")

    sms_enabled = all([sid, token, from_number])

    return AlertConfig(
        sms_enabled=sms_enabled,
        twilio_account_sid=sid,
        twilio_auth_token=token,
        twilio_from_number=from_number,
    )


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def load_sms_recipients(conn, min_confidence: float) -> List[str]:
    """
    Load active SMS subscribers who want alerts at or below this confidence.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT phone_number
        FROM sms_subscribers
        WHERE is_active = 1
          AND (min_confidence IS NULL OR min_confidence <= ?)
        """,
        (min_confidence,),
    )
    return [row[0] for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# SMS sending
# ---------------------------------------------------------------------------

def send_sms(
    *,
    account_sid: str,
    auth_token: str,
    from_number: str,
    to_number: str,
    body: str,
) -> None:
    """
    Send a single SMS message via Twilio.
    """
    client = Client(account_sid, auth_token)
    client.messages.create(
        from_=from_number,
        to=to_number,
        body=body[:1500],  # SMS safety cap
    )


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def build_halftime_message(
    away_name: str,
    home_name: str,
    away_score: int,
    home_score: int,
    p_home: float,
    confidence_score: float,
    confidence_bucket: str,
    extra: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Build a concise halftime SMS message.
    """
    lines = [
        f"[HALFTIME] {away_name} @ {home_name}",
        f"Score: {away_score}-{home_score}",
        f"Home win prob: {p_home:.1%}",
        f"Confidence: {confidence_bucket} ({confidence_score:.3f})",
    ]

    if extra:
        if "halftime_margin" in extra:
            lines.append(f"Margin (home): {extra['halftime_margin']}")
        if "hqs" in extra:
            lines.append(f"HQS: {extra['hqs']:.3f}")
        if "shooting_extreme" in extra:
            lines.append(f"Shooting fluke: {bool(extra['shooting_extreme'])}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Notification orchestration
# ---------------------------------------------------------------------------

def notify_if_confident(
    *,
    conn,
    alert_cfg: AlertConfig,
    confidence_score: float,
    threshold: float,
    message: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Send SMS alerts if confidence threshold is met.
    Returns True if a notification attempt was made.
    """
    if confidence_score < threshold:
        return False

    if not alert_cfg.sms_enabled:
        print("[SMS DISABLED]")
        print(message)
        if metadata:
            print("[metadata]", metadata)
        return True

    recipients = load_sms_recipients(conn, confidence_score)

    for phone in recipients:
        try:
            send_sms(
                account_sid=alert_cfg.twilio_account_sid,
                auth_token=alert_cfg.twilio_auth_token,
                from_number=alert_cfg.twilio_from_number,
                to_number=phone,
                body=message,
            )
        except Exception as e:
            print(f"[SMS ERROR] {phone}: {e}")

    return True
