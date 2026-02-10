"""Anti-flood middleware: ограничение запросов к AI на пользователя.

Ограничивает количество AI-запросов (через /consult, /chat) на одного
пользователя до N запросов в день. Админ освобождён от лимитов.

Использование:
    from src.bot.middlewares.rate_limit import AIRateLimitMiddleware
    dp.message.middleware(AIRateLimitMiddleware())
"""

import logging
import time
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message

from src.config import settings

logger = logging.getLogger(__name__)

# Дневной лимит AI-запросов на пользователя
AI_DAILY_LIMIT = 10

# Команды/состояния, которые используют AI
AI_COMMANDS = {"/consult", "/chat"}

# Хранилище: {user_id: [timestamp, timestamp, ...]}
_ai_usage: dict[int, list[float]] = defaultdict(list)

# Сброс счётчиков раз в сутки
_DAY_SECONDS = 86400


class AIRateLimitMiddleware(BaseMiddleware):
    """Middleware для ограничения AI-запросов на пользователя.

    - Лимит: AI_DAILY_LIMIT запросов в день
    - Админ (ADMIN_ID) освобождён от лимита
    - Отслеживает только команды, связанные с AI
    """

    def __init__(self, daily_limit: int = AI_DAILY_LIMIT) -> None:
        super().__init__()
        self.daily_limit = daily_limit

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        user = getattr(event, "from_user", None)
        if not user:
            return await handler(event, data)

        user_id = user.id

        # Админ не ограничен
        if user_id == settings.ADMIN_ID:
            return await handler(event, data)

        # Проверяем только AI-команды
        is_ai_request = False
        if isinstance(event, Message) and event.text:
            cmd = event.text.strip().split()[0].lower() if event.text.strip() else ""
            if cmd in AI_COMMANDS:
                is_ai_request = True
        elif isinstance(event, CallbackQuery):
            if event.data and event.data in ("start_consult",):
                is_ai_request = True

        if not is_ai_request:
            return await handler(event, data)

        # Очищаем старые записи (>24ч)
        now = time.time()
        _ai_usage[user_id] = [
            ts for ts in _ai_usage[user_id]
            if now - ts < _DAY_SECONDS
        ]

        # Проверяем лимит
        if len(_ai_usage[user_id]) >= self.daily_limit:
            remaining = _DAY_SECONDS - (now - _ai_usage[user_id][0])
            hours = int(remaining // 3600)
            minutes = int((remaining % 3600) // 60)

            await _send_limit_message(event, hours, minutes, self.daily_limit)
            logger.info(
                "AI rate limit hit: user_id=%s, count=%d/%d",
                user_id, len(_ai_usage[user_id]), self.daily_limit,
            )
            return  # Блокируем запрос

        # Записываем использование
        _ai_usage[user_id].append(now)
        remaining = self.daily_limit - len(_ai_usage[user_id])

        # Передаём remaining в data для хендлера (информационно)
        data["ai_requests_remaining"] = remaining

        return await handler(event, data)


async def _send_limit_message(
    event: Message | CallbackQuery,
    hours: int,
    minutes: int,
    limit: int,
) -> None:
    """Отправляет сообщение о превышении лимита."""
    text = (
        f"⚠️ Вы исчерпали дневной лимит AI-запросов ({limit} в день).\n\n"
        f"Лимит обновится через {hours}ч {minutes}мин.\n\n"
        "Для неограниченных консультаций обратитесь к нашим юристам:\n"
        "📞 @SOLISlegal"
    )

    if isinstance(event, Message):
        await event.answer(text)
    elif isinstance(event, CallbackQuery):
        await event.answer(text[:200], show_alert=True)
        if event.message:
            await event.message.answer(text)


def get_user_ai_usage(user_id: int) -> dict:
    """Возвращает статистику AI-запросов пользователя (для /report)."""
    now = time.time()
    usage = [ts for ts in _ai_usage.get(user_id, []) if now - ts < _DAY_SECONDS]
    return {
        "today": len(usage),
        "limit": AI_DAILY_LIMIT,
        "remaining": max(0, AI_DAILY_LIMIT - len(usage)),
    }


def get_total_ai_usage_today() -> dict:
    """Возвращает общую статистику AI-запросов за день (для /report)."""
    now = time.time()
    total = 0
    unique_users = 0
    for user_id, timestamps in _ai_usage.items():
        today_count = sum(1 for ts in timestamps if now - ts < _DAY_SECONDS)
        if today_count > 0:
            total += today_count
            unique_users += 1
    return {
        "total_requests": total,
        "unique_users": unique_users,
    }
