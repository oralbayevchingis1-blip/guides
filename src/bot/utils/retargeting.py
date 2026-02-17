"""Cross-channel retargeting и UTM deep link generation.

Модуль решает три задачи:
1. Генерация UTM-tagged deep links для email / рекламы / соцсетей.
2. Отправка серверных событий (Facebook Conversions API) при скачивании гайда.
3. Формирование аудиторий для рекламных платформ.

Настройка (env):
    FB_PIXEL_ID      — ID пикселя Facebook (пустой = отключено)
    FB_ACCESS_TOKEN  — токен серверных событий (Conversions API)
    BOT_USERNAME     — username бота (автоматически из main.py)
"""

import hashlib
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

# Кэш bot username (заполняется при старте бота)
_bot_username: str = ""


def set_bot_username(username: str) -> None:
    """Устанавливается один раз при старте бота (main.py)."""
    global _bot_username
    _bot_username = username
    logger.info("Retargeting: bot username set to @%s", username)


# ── UTM Deep Link Builder ────────────────────────────────────────────


def make_deep_link(
    payload: str,
    *,
    source: str = "",
    medium: str = "",
    campaign: str = "",
    bot_username: str = "",
) -> str:
    """Генерирует UTM-tagged deep link для бота.

    Args:
        payload: Payload для /start (напр., ``guide_taxes``)
        source: utm_source (email, facebook, linkedin, instagram)
        medium: utm_medium (newsletter, cpc, organic, retargeting)
        campaign: utm_campaign (feb2026, tax_webinar)
        bot_username: Username бота (если не задан — использует кэш)

    Returns:
        ``https://t.me/BotName?start=guide_taxes--src_email--med_newsletter``

    Examples:
        >>> make_deep_link("guide_taxes", source="email", medium="followup")
        'https://t.me/MyBot?start=guide_taxes--src_email--med_followup'
    """
    username = bot_username or _bot_username
    if not username:
        logger.warning("Bot username not set — using placeholder")
        username = "SOLIS_law_bot"

    parts = [payload]
    if source:
        parts.append(f"src_{source}")
    if medium:
        parts.append(f"med_{medium}")
    if campaign:
        parts.append(f"cmp_{campaign}")

    start_param = "--".join(parts)
    return f"https://t.me/{username}?start={start_param}"


def make_guide_deep_link(
    guide_id: str,
    *,
    source: str = "email",
    medium: str = "followup",
    campaign: str = "",
) -> str:
    """Shortcut: deep link для конкретного гайда."""
    return make_deep_link(
        f"guide_{guide_id}",
        source=source,
        medium=medium,
        campaign=campaign,
    )


def make_consult_deep_link(
    *,
    source: str = "email",
    medium: str = "followup",
    campaign: str = "",
) -> str:
    """Shortcut: deep link для записи на консультацию."""
    return make_deep_link(
        "consult",
        source=source,
        medium=medium,
        campaign=campaign,
    )


def make_bot_link(
    *,
    source: str = "",
    medium: str = "",
    campaign: str = "",
) -> str:
    """Shortcut: deep link на /start бота (без payload)."""
    return make_deep_link(
        "start",
        source=source,
        medium=medium,
        campaign=campaign,
    )


# ── Facebook Conversions API ─────────────────────────────────────────


def _get_fb_config() -> tuple[str, str]:
    """Возвращает (pixel_id, access_token) из env."""
    import os
    return (
        os.getenv("FB_PIXEL_ID", ""),
        os.getenv("FB_ACCESS_TOKEN", ""),
    )


def _hash_for_fb(value: str) -> str:
    """SHA-256 хэш для Facebook (нормализация + lowercase)."""
    return hashlib.sha256(value.strip().lower().encode()).hexdigest()


async def send_fb_event(
    event_name: str,
    *,
    email: str = "",
    user_id: int = 0,
    guide_id: str = "",
    custom_data: dict[str, Any] | None = None,
) -> bool:
    """Отправляет серверное событие в Facebook Conversions API.

    Поддерживаемые события:
        - Lead          — скачивание гайда
        - Schedule       — запись на консультацию
        - ViewContent   — просмотр превью гайда
        - CompleteRegistration — регистрация (email + consent)

    Args:
        event_name: Название события (Lead, Schedule и т.д.)
        email: Email пользователя (хэшируется перед отправкой)
        user_id: Telegram user_id (external_id для матчинга)
        guide_id: ID гайда (для custom_data)
        custom_data: Дополнительные параметры

    Returns:
        True если событие отправлено успешно.
    """
    pixel_id, access_token = _get_fb_config()
    if not pixel_id or not access_token:
        return False

    try:
        import aiohttp

        user_data: dict[str, Any] = {}
        if email:
            user_data["em"] = [_hash_for_fb(email)]
        if user_id:
            user_data["external_id"] = [_hash_for_fb(str(user_id))]

        event_data: dict[str, Any] = {
            "event_name": event_name,
            "event_time": int(time.time()),
            "action_source": "other",
            "user_data": user_data,
        }

        cd = dict(custom_data or {})
        if guide_id:
            cd["content_ids"] = [guide_id]
            cd["content_type"] = "product"
        if cd:
            event_data["custom_data"] = cd

        payload = {"data": [event_data]}

        url = f"https://graph.facebook.com/v18.0/{pixel_id}/events"

        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                url,
                params={"access_token": access_token},
                json=payload,
            )
            if resp.status == 200:
                logger.info(
                    "FB event sent: %s email=%s guide=%s",
                    event_name, email[:15] if email else "-", guide_id or "-",
                )
                return True
            err = await resp.text()
            logger.warning("FB event error %d: %s", resp.status, err[:200])
            return False

    except Exception as e:
        logger.warning("FB Conversions API error: %s", e)
        return False


async def track_download_event(
    user_id: int,
    email: str,
    guide_id: str,
) -> None:
    """Трекает скачивание гайда в Facebook (fire-and-forget)."""
    await send_fb_event(
        "Lead",
        email=email,
        user_id=user_id,
        guide_id=guide_id,
        custom_data={"currency": "KZT", "value": 0},
    )


async def track_consultation_event(
    user_id: int,
    email: str,
) -> None:
    """Трекает запись на консультацию в Facebook."""
    await send_fb_event(
        "Schedule",
        email=email,
        user_id=user_id,
    )


async def track_registration_event(
    user_id: int,
    email: str,
) -> None:
    """Трекает регистрацию (email + согласие) в Facebook."""
    await send_fb_event(
        "CompleteRegistration",
        email=email,
        user_id=user_id,
    )


# ── Audience Helpers ─────────────────────────────────────────────────


def build_audience_csv(
    emails: set[str],
    *,
    include_header: bool = True,
) -> str:
    """Генерирует CSV для Facebook Custom Audiences.

    Формат: email (один на строку), первая строка — заголовок.
    Facebook принимает как plain email, так и хэшированный SHA-256.
    """
    lines = []
    if include_header:
        lines.append("email")
    for email in sorted(emails):
        lines.append(email.strip().lower())
    return "\n".join(lines)


def build_audience_csv_hashed(emails: set[str]) -> str:
    """CSV с SHA-256 хэшированными email (повышенная безопасность)."""
    lines = ["email_hash"]
    for email in sorted(emails):
        lines.append(_hash_for_fb(email))
    return "\n".join(lines)
