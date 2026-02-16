"""Rate limiter — защита от спама и флуда.

Два уровня защиты:

1. **ThrottleMiddleware** — aiogram outer-middleware.
   Ограничивает общее число событий от одного user_id в скользящем окне.
   При превышении — бот молча игнорирует (или шлёт предупреждение раз в N секунд).
   Админ (`ADMIN_ID`) не ограничивается.

2. **CriticalRateLimiter** — проверка для «дорогих» действий
   (отправка email, запись на консультацию, скачивание гайда).
   Более жёсткие лимиты: 3 попытки за 60 с.
   Вызывается вручную из обработчиков.
"""

import logging
import time
from collections import defaultdict, deque
from typing import Any, Callable

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, TelegramObject

from src.config import settings

logger = logging.getLogger(__name__)


# ─────────────────── Per-User Sliding Window ──────────────────────────


class _SlidingWindow:
    """Скользящее окно событий для одного ключа."""

    __slots__ = ("_timestamps", "_max_size")

    def __init__(self, max_size: int = 200) -> None:
        self._timestamps: deque[float] = deque(maxlen=max_size)
        self._max_size = max_size

    def add(self, now: float) -> None:
        self._timestamps.append(now)

    def count_in_window(self, now: float, window: float) -> int:
        cutoff = now - window
        # Отсекаем старые (deque отсортирован по времени)
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
        return len(self._timestamps)


# ─────────────────── Throttle Middleware (general) ────────────────────


class ThrottleMiddleware(BaseMiddleware):
    """Per-user rate limiter.

    Args:
        rate:     Максимум событий в окне.
        period:   Размер окна в секундах.
        silent:   Если True — молча дропает. Если False — шлёт предупреждение
                  (не чаще раз в ``warn_cooldown`` секунд).
        warn_cooldown: Минимальный интервал между предупреждениями одному юзеру.
    """

    def __init__(
        self,
        rate: int = 5,
        period: int = 60,
        *,
        silent: bool = False,
        warn_cooldown: int = 30,
    ) -> None:
        super().__init__()
        self._rate = rate
        self._period = period
        self._silent = silent
        self._warn_cooldown = warn_cooldown
        self._windows: dict[int, _SlidingWindow] = defaultdict(_SlidingWindow)
        self._last_warn: dict[int, float] = {}

        # Счётчики для мониторинга
        self.total_throttled: int = 0
        self.total_passed: int = 0

    async def __call__(
        self,
        handler: Callable,
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user_id = _extract_user_id(event)
        if user_id is None:
            return await handler(event, data)

        # Админ не ограничивается
        if user_id == settings.ADMIN_ID:
            return await handler(event, data)

        now = time.monotonic()
        window = self._windows[user_id]
        count = window.count_in_window(now, self._period)

        if count >= self._rate:
            self.total_throttled += 1
            _track_throttle(user_id)

            if not self._silent:
                last = self._last_warn.get(user_id, 0)
                if now - last >= self._warn_cooldown:
                    self._last_warn[user_id] = now
                    await _send_throttle_warning(event)

            logger.warning(
                "Throttled user_id=%s (%d/%d in %ds)",
                user_id, count, self._rate, self._period,
            )
            return  # Дропаем событие

        window.add(now)
        self.total_passed += 1
        return await handler(event, data)

    def cleanup(self) -> None:
        """Удаляет данные неактивных пользователей (вызывать периодически)."""
        now = time.monotonic()
        stale = [
            uid for uid, w in self._windows.items()
            if w.count_in_window(now, self._period * 10) == 0
        ]
        for uid in stale:
            del self._windows[uid]
            self._last_warn.pop(uid, None)


# ─────────────────── Critical Action Limiter ──────────────────────────


class CriticalRateLimiter:
    """Жёсткий лимитер для «дорогих» действий.

    Использование в обработчике::

        if not critical_limiter.allow(user_id, "email"):
            await message.answer("Слишком много попыток...")
            return
    """

    def __init__(self, rate: int = 3, period: int = 60) -> None:
        self._rate = rate
        self._period = period
        self._windows: dict[str, _SlidingWindow] = defaultdict(_SlidingWindow)
        self.total_blocked: int = 0

    def allow(self, user_id: int, action: str) -> bool:
        """Возвращает True если действие разрешено."""
        if user_id == settings.ADMIN_ID:
            return True

        key = f"{user_id}:{action}"
        now = time.monotonic()
        window = self._windows[key]
        count = window.count_in_window(now, self._period)

        if count >= self._rate:
            self.total_blocked += 1
            _track_throttle(user_id, action)
            logger.warning(
                "Critical action blocked: user=%s action=%s (%d/%d)",
                user_id, action, count, self._rate,
            )
            return False

        window.add(now)
        return True


# Глобальные экземпляры
throttle_mw = ThrottleMiddleware(rate=8, period=60, silent=False, warn_cooldown=30)
critical_limiter = CriticalRateLimiter(rate=3, period=60)


# ─────────────────── Helpers ──────────────────────────────────────────


def _extract_user_id(event: TelegramObject) -> int | None:
    """Извлекает user_id из Message или CallbackQuery."""
    if isinstance(event, Message):
        return event.from_user.id if event.from_user else None
    if isinstance(event, CallbackQuery):
        return event.from_user.id if event.from_user else None
    return None


async def _send_throttle_warning(event: TelegramObject) -> None:
    """Отправляет пользователю короткое предупреждение."""
    text = "⏳ Пожалуйста, не так быстро. Подождите немного."
    try:
        if isinstance(event, Message):
            await event.answer(text)
        elif isinstance(event, CallbackQuery):
            await event.answer(text, show_alert=True)
    except Exception:
        pass


def _track_throttle(user_id: int, action: str = "general") -> None:
    """Трекает throttle-событие в мониторинг (best-effort)."""
    try:
        from src.bot.utils.monitoring import metrics
        metrics.inc("throttled_total")
        metrics.inc(f"throttled.{action}")
    except Exception:
        pass
