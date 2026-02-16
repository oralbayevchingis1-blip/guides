"""Email-отправщик — поддержка Resend API и SMTP.

Используется для ретаргетинг-кампаний и приветственных писем.
"""

import asyncio
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def _get_config() -> dict:
    from src.config import settings
    return {
        "resend_api_key": settings.RESEND_API_KEY,
        "smtp_host": settings.SMTP_HOST,
        "smtp_port": settings.SMTP_PORT,
        "smtp_user": settings.SMTP_USER,
        "smtp_password": settings.SMTP_PASSWORD,
        "from_email": settings.FROM_EMAIL,
        "from_name": settings.FROM_NAME,
    }


def is_email_configured() -> bool:
    """Проверяет, настроен ли хотя бы один способ отправки."""
    cfg = _get_config()
    return bool(cfg["resend_api_key"]) or bool(cfg["smtp_host"] and cfg["smtp_user"])


async def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    text_body: str = "",
) -> bool:
    """Отправляет email через Resend API или SMTP.

    Returns:
        True если отправлено успешно.
    """
    cfg = _get_config()

    if cfg["resend_api_key"]:
        return await _send_resend(to_email, subject, html_body, cfg)

    if cfg["smtp_host"] and cfg["smtp_user"]:
        return await _send_smtp(to_email, subject, html_body, text_body, cfg)

    logger.info("Email not configured — skipping send to %s", to_email[:20])
    return False


async def _send_resend(
    to: str, subject: str, html: str, cfg: dict,
) -> bool:
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {cfg['resend_api_key']}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": f"{cfg['from_name']} <{cfg['from_email']}>",
                    "to": [to],
                    "subject": subject,
                    "html": html,
                },
            )
            if resp.status in (200, 201):
                logger.info("Email sent via Resend to %s", to)
                return True
            err = await resp.text()
            logger.error("Resend error %d: %s", resp.status, err[:200])
            return False
    except Exception as e:
        logger.error("Resend send failed: %s", e)
        return False


async def _send_smtp(
    to: str, subject: str, html: str, text: str, cfg: dict,
) -> bool:
    def _do_send():
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{cfg['from_name']} <{cfg['from_email']}>"
            msg["To"] = to

            if text:
                msg.attach(MIMEText(text, "plain", "utf-8"))
            msg.attach(MIMEText(html, "html", "utf-8"))

            with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as server:
                server.starttls()
                server.login(cfg["smtp_user"], cfg["smtp_password"])
                server.send_message(msg)

            logger.info("Email sent via SMTP to %s", to)
            return True
        except Exception as e:
            logger.error("SMTP send failed: %s", e)
            return False

    return await asyncio.to_thread(_do_send)
