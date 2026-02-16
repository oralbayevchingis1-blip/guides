"""Обработчик проверки подписки на канал."""

import logging

from aiogram import Bot, F, Router
from aiogram.types import CallbackQuery

from src.bot.keyboards.inline import guides_menu_keyboard, subscription_keyboard
from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.subscription_check import check_subscription
from src.constants import get_text

router = Router()
logger = logging.getLogger(__name__)


@router.callback_query(F.data == "check_subscription")
async def check_subscription_callback(
    callback: CallbackQuery,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Повторная проверка подписки по нажатию кнопки."""
    user_id = callback.from_user.id
    is_subscribed = await check_subscription(user_id, bot)

    # Загружаем тексты и каталог из кеша
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    if is_subscribed:
        logger.info("Пользователь %s подтвердил подписку", user_id)
        await callback.message.edit_text(
            get_text(texts, "subscription_success"),
            reply_markup=guides_menu_keyboard(catalog),
        )
    else:
        logger.info("Пользователь %s не подписан", user_id)
        await callback.answer(
            get_text(texts, "subscription_fail"),
            show_alert=True,
        )
