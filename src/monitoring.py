"""Мониторинг и самоисцеление: AI-диагностика ошибок + уведомления админа.

- AdminNotifier: отправляет ошибки админу в Telegram
- SelfHealingMiddleware: перехватывает исключения, отправляет traceback AI
  для диагностики, логирует рекомендации по исправлению

Использование:
    from src.monitoring import SelfHealingMiddleware, AdminNotifier
    dp.message.middleware(SelfHealingMiddleware(bot))
"""

import asyncio
import logging
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware, Bot
from aiogram.types import CallbackQuery, Message

from src.config import settings

logger = logging.getLogger(__name__)

# Дедупликация ошибок: не спамить одной и той же ошибкой
_error_counts: dict[str, int] = defaultdict(int)
_error_cooldown: dict[str, float] = {}
_ERROR_COOLDOWN_SECONDS = 300  # 5 минут между одинаковыми ошибками


class AdminNotifier:
    """Отправляет уведомления об ошибках админу в Telegram."""

    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    async def notify(self, text: str) -> None:
        """Отправляет сообщение админу с дедупликацией."""
        error_key = text[:100]
        now = datetime.now(timezone.utc).timestamp()

        # Проверяем cooldown
        last_sent = _error_cooldown.get(error_key, 0)
        if now - last_sent < _ERROR_COOLDOWN_SECONDS:
            _error_counts[error_key] += 1
            return

        _error_cooldown[error_key] = now
        suppressed = _error_counts.get(error_key, 0)
        _error_counts[error_key] = 0

        full_text = text
        if suppressed > 0:
            full_text += f"\n\n(+{suppressed} подавлено за последние 5 мин)"

        if len(full_text) > 4000:
            full_text = full_text[:4000] + "..."

        try:
            await self.bot.send_message(
                chat_id=settings.ADMIN_ID,
                text=full_text,
                parse_mode=None,
            )
        except Exception as e:
            logger.error("Failed to notify admin: %s", e)


class SelfHealingMiddleware(BaseMiddleware):
    """Перехватывает исключения, отправляет AI-диагностику и логирует.

    Когда хендлер падает с ошибкой:
    1. Формирует traceback
    2. Отправляет его AI для анализа
    3. AI определяет возможную причину и рекомендацию
    4. Логирует рекомендацию + уведомляет админа
    """

    def __init__(self, bot: Bot) -> None:
        super().__init__()
        self.notifier = AdminNotifier(bot)

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        try:
            return await handler(event, data)
        except Exception as exc:
            # Формируем traceback
            tb = traceback.format_exc()
            exc_name = type(exc).__name__
            exc_msg = str(exc)[:300]

            user = getattr(event, "from_user", None)
            user_id = user.id if user else 0

            # Логируем ошибку
            logger.error(
                "Handler exception [user=%s]: %s: %s\n%s",
                user_id, exc_name, exc_msg, tb[-1000:],
            )

            # AI-диагностика (fire-and-forget, не блокируем пользователя)
            asyncio.create_task(
                self._ai_diagnose(exc_name, exc_msg, tb[-2000:], user_id)
            )

            # Re-raise чтобы не глотать ошибку
            raise

    async def _ai_diagnose(
        self, exc_name: str, exc_msg: str, tb: str, user_id: int
    ) -> None:
        """Отправляет traceback AI для диагностики."""
        try:
            from src.bot.utils.ai_client import get_orchestrator

            ai = get_orchestrator()
            diagnosis_prompt = (
                f"Проанализируй ошибку Telegram-бота и дай рекомендацию.\n\n"
                f"ИСКЛЮЧЕНИЕ: {exc_name}: {exc_msg}\n\n"
                f"TRACEBACK (последние 2000 символов):\n{tb}\n\n"
                f"Ответь кратко в формате:\n"
                f"ПРИЧИНА: [возможная причина]\n"
                f"РЕКОМЕНДАЦИЯ: [как исправить]\n"
                f"КРИТИЧНОСТЬ: [HIGH/MEDIUM/LOW]"
            )

            diagnosis = await ai.call_gemini(
                diagnosis_prompt,
                "Ты — DevOps-инженер. Анализируй ошибки Python/Aiogram и давай рекомендации.",
                max_tokens=512,
                temperature=0.2,
            )

            # Логируем AI-диагностику
            logger.warning(
                "🧠 AI DIAGNOSIS [user=%s, %s]:\n%s",
                user_id, exc_name, diagnosis,
            )

            # Уведомляем админа
            await self.notifier.notify(
                f"⚠️ ОШИБКА БОТА\n\n"
                f"Exception: {exc_name}: {exc_msg[:200]}\n"
                f"User: {user_id}\n\n"
                f"🧠 AI-диагностика:\n{diagnosis[:1500]}"
            )

        except Exception as e:
            # AI-диагностика не должна ломать бота
            logger.error("AI diagnosis failed: %s", e)
            # Всё равно уведомляем админа о базовой ошибке
            await self.notifier.notify(
                f"⚠️ ОШИБКА БОТА\n\n"
                f"Exception: {exc_name}: {exc_msg[:200]}\n"
                f"User: {user_id}\n"
                f"(AI-диагностика недоступна: {e})"
            )
