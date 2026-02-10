"""P2. Продвинутый ThrottlingMiddleware — защита от DDoS и спама.

Два уровня:
1. Global throttle: не более 1 сообщения/сек на пользователя (все команды)
2. AI throttle: не более 5 AI-запросов/час (уже есть в rate_limit.py)

Этот middleware обрабатывает уровень 1 — общий антифлуд.
"""

import logging
import time
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message

from src.config import settings

logger = logging.getLogger(__name__)

# Хранилище последних запросов: {user_id: last_timestamp}
_last_message: dict[int, float] = {}

# Счётчик подозрительных пользователей (быстрые запросы подряд)
_flood_score: dict[int, int] = defaultdict(int)

# Конфигурация
MIN_INTERVAL_SEC = 0.5       # Минимум между сообщениями
FLOOD_THRESHOLD = 10         # После N быстрых запросов → мягкий бан
FLOOD_BAN_SECONDS = 60       # Бан на N секунд
FLOOD_SCORE_DECAY = 30       # Сброс flood_score через N секунд


# {user_id: ban_until_timestamp}
_bans: dict[int, float] = {}


class ThrottlingMiddleware(BaseMiddleware):
    """Общий антифлуд: 1 msg/sec + мягкий бан при спаме."""

    def __init__(
        self,
        min_interval: float = MIN_INTERVAL_SEC,
        flood_threshold: int = FLOOD_THRESHOLD,
        ban_seconds: int = FLOOD_BAN_SECONDS,
    ) -> None:
        super().__init__()
        self.min_interval = min_interval
        self.flood_threshold = flood_threshold
        self.ban_seconds = ban_seconds

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

        now = time.time()

        # Проверяем бан
        ban_until = _bans.get(user_id, 0)
        if now < ban_until:
            remaining = int(ban_until - now)
            logger.warning("Throttled user %s (banned for %ds)", user_id, remaining)
            if isinstance(event, Message):
                await event.answer(
                    f"⏳ Слишком много запросов. Подождите {remaining} сек."
                )
            elif isinstance(event, CallbackQuery):
                await event.answer(
                    f"Подождите {remaining} сек.", show_alert=True
                )
            return

        # Проверяем интервал
        last = _last_message.get(user_id, 0)
        delta = now - last

        if delta < self.min_interval:
            _flood_score[user_id] += 1

            # Проверяем порог флуда
            if _flood_score[user_id] >= self.flood_threshold:
                _bans[user_id] = now + self.ban_seconds
                _flood_score[user_id] = 0
                logger.warning(
                    "User %s soft-banned for %ds (flood score exceeded)",
                    user_id, self.ban_seconds,
                )
                if isinstance(event, Message):
                    await event.answer(
                        f"🚫 Обнаружен флуд. Бот заблокирован на {self.ban_seconds} секунд.\n"
                        "Пожалуйста, не отправляйте сообщения так быстро."
                    )
                return

            # Мягкий троттлинг — пропускаем без ответа
            return

        # Decay flood score
        if delta > FLOOD_SCORE_DECAY:
            _flood_score[user_id] = 0

        _last_message[user_id] = now
        return await handler(event, data)


def get_throttle_stats() -> dict:
    """Статистика троттлинга для /report."""
    now = time.time()
    active_bans = sum(1 for t in _bans.values() if t > now)
    return {
        "active_bans": active_bans,
        "flood_scores": dict(_flood_score),
        "total_tracked": len(_last_message),
    }
