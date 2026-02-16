"""P1. Централизованная обработка ошибок — Global ErrorHandlingMiddleware.

Любая ошибка в боте:
1. Логируется с полным стэктрейсом
2. Отправляется админу с деталями
3. Пользователь получает вежливое «Мы уже чиним»

Работает ПЕРЕД SelfHealingMiddleware (тот делает AI-диагностику),
а этот middleware обеспечивает user-facing ответ и гарантированный лог.
"""

import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware, Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.config import settings

# Ошибки Telegram, которые безопасно подавлять (не ломают бота, не требуют действий)
_SUPPRESSED_TELEGRAM_ERRORS = (
    "message is not modified",
    "query is too old",
    "message to edit not found",
    "message can't be edited",
    "message can't be deleted",
    "bot was blocked by the user",
    "user is deactivated",
    "chat not found",
)

logger = logging.getLogger(__name__)

# Счётчик ошибок по типу (для отчётов)
_error_counter: dict[str, int] = {}


def _mask_secrets(text: str) -> str:
    """Маскирует API-ключи и токены в тексте стэктрейса."""
    import re
    # Маскируем всё, что похоже на токен/ключ
    text = re.sub(r'(sk-proj-|sk-|AIza|ghp_|ghu_)\S{10,}', r'\1***MASKED***', text)
    text = re.sub(r'(\d{8,12}:AA[A-Za-z0-9_-]{30,})', '***BOT_TOKEN_MASKED***', text)
    text = re.sub(r'([A-Za-z0-9+/]{40,}={0,2})', lambda m: m.group()[:8] + '***', text)
    return text


class ErrorHandlingMiddleware(BaseMiddleware):
    """Глобальный перехватчик ошибок с user-facing ответом и admin-нотификацией.

    Ставится ПЕРВЫМ в цепочке middleware.
    """

    def __init__(self, bot: Bot) -> None:
        super().__init__()
        self.bot = bot

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any],
    ) -> Any:
        try:
            return await handler(event, data)
        except Exception as exc:
            # Подавляем безобидные ошибки Telegram API (не ломают бота)
            if isinstance(exc, TelegramBadRequest):
                exc_lower = str(exc).lower()
                if any(s in exc_lower for s in _SUPPRESSED_TELEGRAM_ERRORS):
                    logger.debug("Suppressed TelegramBadRequest: %s", exc)
                    if isinstance(event, CallbackQuery):
                        try:
                            await event.answer()
                        except Exception:
                            pass
                    return

            exc_name = type(exc).__name__
            exc_msg = str(exc)[:300]
            tb = traceback.format_exc()

            user = getattr(event, "from_user", None)
            user_id = user.id if user else 0
            username = (user.username or "") if user else ""

            # 1. Лог с полным стэктрейсом (секреты замаскированы)
            safe_tb = _mask_secrets(tb)
            logger.error(
                "GLOBAL ERROR [user=%s @%s] %s: %s\n%s",
                user_id, username, exc_name, exc_msg, safe_tb[-2000:],
            )

            # 2. Счётчик ошибок
            _error_counter[exc_name] = _error_counter.get(exc_name, 0) + 1

            # 3. Вежливый ответ пользователю (не молчим)
            await self._reply_user(event)

            # 4. Уведомление админу
            await self._notify_admin(exc_name, exc_msg, safe_tb[-1500:], user_id, username)

            # 5. Sentry (P4)
            try:
                from src.bot.utils.sentry_integration import capture_exception, set_user_context
                set_user_context(user_id, username)
                capture_exception(exc, user_id=user_id, handler="middleware")
            except Exception:
                pass

            # НЕ re-raise — пользователь уже получил ответ.

    async def _reply_user(self, event: Message | CallbackQuery) -> None:
        """Отправляет пользователю дружелюбное сообщение об ошибке."""
        text = (
            "⚠️ Произошла временная ошибка.\n\n"
            "Мы уже получили уведомление и работаем над исправлением.\n"
            "Попробуйте повторить действие через минуту.\n\n"
            "Если проблема повторяется — напишите @SOLISlegal"
        )
        try:
            if isinstance(event, CallbackQuery):
                await event.answer("⚠️ Произошла ошибка. Попробуйте снова.", show_alert=True)
                if event.message:
                    await event.message.answer(text)
            elif isinstance(event, Message):
                await event.answer(text)
        except Exception:
            pass  # Не можем даже ответить — ничего не поделать

    async def _notify_admin(
        self,
        exc_name: str,
        exc_msg: str,
        tb_safe: str,
        user_id: int,
        username: str,
    ) -> None:
        """Уведомляет админа об ошибке."""
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        count = _error_counter.get(exc_name, 1)

        text = (
            f"🚨 <b>Ошибка бота</b> [{now} UTC]\n\n"
            f"<b>{exc_name}</b>: {exc_msg[:200]}\n"
            f"👤 User: {user_id} @{username}\n"
            f"📊 Повторений: {count}\n\n"
            f"<pre>{tb_safe[-800:]}</pre>"
        )

        try:
            await self.bot.send_message(
                chat_id=settings.ADMIN_ID,
                text=text[:4000],
            )
        except Exception:
            # Fallback: без HTML
            try:
                await self.bot.send_message(
                    chat_id=settings.ADMIN_ID,
                    text=_mask_secrets(text[:4000]),
                    parse_mode=None,
                )
            except Exception:
                pass


def get_error_stats() -> dict[str, int]:
    """Возвращает статистику ошибок по типам (для /report)."""
    return dict(_error_counter)


def reset_error_stats() -> None:
    """Сбрасывает счётчик ошибок."""
    _error_counter.clear()
