"""Middleware для логирования входящих сообщений."""

import logging
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import Message

logger = logging.getLogger(__name__)


class LoggingMiddleware(BaseMiddleware):
    """Логирует каждое входящее сообщение."""

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        user = event.from_user
        logger.info(
            "Сообщение от user_id=%s (%s): %s",
            user.id if user else "unknown",
            user.username if user else "unknown",
            event.text[:80] if event.text else "<non-text>",
        )
        return await handler(event, data)
