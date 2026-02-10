"""P4. Интеграция Sentry для профессионального мониторинга ошибок.

Подключает sentry-sdk если SENTRY_DSN задан в .env.
Если не задан — тихо работает без Sentry (noop fallback).

Использование:
    from src.bot.utils.sentry_integration import init_sentry, capture_exception
    init_sentry()               # при старте бота
    capture_exception(exc)      # в обработчиках ошибок
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_sentry_enabled = False


def init_sentry() -> bool:
    """Инициализирует Sentry SDK если SENTRY_DSN задан.

    Returns:
        True если Sentry инициализирован.
    """
    global _sentry_enabled

    try:
        from src.config import settings
        dsn = getattr(settings, "SENTRY_DSN", "")
        if not dsn:
            logger.info("Sentry: SENTRY_DSN не задан — мониторинг отключен")
            return False

        import sentry_sdk
        from sentry_sdk.integrations.aiohttp import AioHttpIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration

        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=0.2,  # 20% транзакций для performance
            profiles_sample_rate=0.1,
            environment=getattr(settings, "ENVIRONMENT", "production"),
            release=getattr(settings, "VERSION", "1.0.0"),
            integrations=[
                LoggingIntegration(level=logging.WARNING, event_level=logging.ERROR),
                AioHttpIntegration(),
            ],
            # Фильтруем чувствительные данные
            before_send=_before_send,
        )

        _sentry_enabled = True
        logger.info("Sentry: инициализирован (DSN: %s...)", dsn[:30])
        return True

    except ImportError:
        logger.info("Sentry: sentry-sdk не установлен — мониторинг отключен")
        return False
    except Exception as e:
        logger.warning("Sentry: не удалось инициализировать: %s", e)
        return False


def _before_send(event, hint):
    """Фильтр Sentry: маскируем чувствительные данные перед отправкой."""
    # Маскируем API ключи в breadcrumbs и exception values
    if "exception" in event:
        for exc_info in event["exception"].get("values", []):
            value = exc_info.get("value", "")
            if any(secret in value for secret in ("sk-proj-", "AIza", "ghp_")):
                exc_info["value"] = "***MASKED_SECRET_IN_EXCEPTION***"
    return event


def capture_exception(exc: Exception, **extra) -> None:
    """Отправляет исключение в Sentry (если подключен).

    Args:
        exc: Исключение для трекинга.
        **extra: Дополнительный контекст (user_id, action, etc.)
    """
    if not _sentry_enabled:
        return

    try:
        import sentry_sdk
        with sentry_sdk.push_scope() as scope:
            for key, value in extra.items():
                scope.set_extra(key, value)
            sentry_sdk.capture_exception(exc)
    except Exception:
        pass  # Sentry не должен ломать бота


def capture_message(message: str, level: str = "info", **extra) -> None:
    """Отправляет сообщение в Sentry."""
    if not _sentry_enabled:
        return

    try:
        import sentry_sdk
        with sentry_sdk.push_scope() as scope:
            for key, value in extra.items():
                scope.set_extra(key, value)
            sentry_sdk.capture_message(message, level=level)
    except Exception:
        pass


def set_user_context(user_id: int, username: str = "") -> None:
    """Устанавливает контекст пользователя для Sentry."""
    if not _sentry_enabled:
        return

    try:
        import sentry_sdk
        sentry_sdk.set_user({"id": str(user_id), "username": username})
    except Exception:
        pass


def is_enabled() -> bool:
    """Проверяет, активен ли Sentry."""
    return _sentry_enabled
