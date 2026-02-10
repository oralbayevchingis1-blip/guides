"""Логирование согласий и действий для compliance."""

import logging
from datetime import datetime, timezone

from src.database.crud import save_consent_log

logger = logging.getLogger(__name__)


async def log_consent(
    user_id: int,
    consent_type: str,
    ip: str | None = None,
) -> None:
    """Логирование согласия пользователя для юридических требований.

    Args:
        user_id: Telegram ID пользователя.
        consent_type: Тип согласия (personal_data, email_marketing и т.д.).
        ip: IP-адрес (если доступен).
    """
    log_entry = {
        "user_id": user_id,
        "consent_type": consent_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ip_address": ip,
        "user_agent": "Telegram Bot",
    }

    logger.info("Согласие зафиксировано: %s", log_entry)

    # Сохраняем в базу данных
    await save_consent_log(log_entry)
