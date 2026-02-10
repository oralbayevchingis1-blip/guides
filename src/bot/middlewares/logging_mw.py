"""Middleware для структурированного логирования с request_id и таймингами."""

import json
import logging
import time
import uuid
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, Update

logger = logging.getLogger(__name__)


class TraceMiddleware(BaseMiddleware):
    """Структурированное логирование: request_id, timing, user_id, handler.

    Каждому update присваивается уникальный request_id (uuid4[:8]).
    Замеряется время выполнения хендлера.
    request_id передаётся в data для использования хендлерами.
    """

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        request_id = uuid.uuid4().hex[:8]
        data["request_id"] = request_id

        user = getattr(event, "from_user", None)
        user_id = user.id if user else 0
        username = user.username if user else "?"

        # Определяем тип события
        event_type = "message"
        event_text = ""
        if isinstance(event, Message):
            event_text = (event.text or "<non-text>")[:80]
        elif isinstance(event, CallbackQuery):
            event_type = "callback"
            event_text = (event.data or "")[:64]

        start = time.perf_counter()
        status = "ok"
        error_msg = ""

        try:
            result = await handler(event, data)
            return result
        except Exception as exc:
            status = "error"
            error_msg = str(exc)[:200]
            raise
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 1)

            log_entry = {
                "request_id": request_id,
                "user_id": user_id,
                "username": username,
                "event_type": event_type,
                "text": event_text,
                "duration_ms": duration_ms,
                "status": status,
            }
            if error_msg:
                log_entry["error"] = error_msg

            logger.info(json.dumps(log_entry, ensure_ascii=False))

            # Обновляем last_activity (fire-and-forget)
            if user_id:
                try:
                    from src.database.crud import update_user_activity
                    import asyncio
                    asyncio.create_task(update_user_activity(user_id))
                except Exception:
                    pass


# Alias для обратной совместимости
LoggingMiddleware = TraceMiddleware
