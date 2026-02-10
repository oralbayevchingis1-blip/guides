"""Утилита проверки подписки пользователя на канал."""

import logging

from aiogram import Bot
from aiogram.enums import ChatMemberStatus

from src.config import settings

logger = logging.getLogger(__name__)


async def check_subscription(user_id: int, bot: Bot) -> bool:
    """Проверяет, подписан ли пользователь на канал.

    Args:
        user_id: Telegram ID пользователя.
        bot: Экземпляр бота.

    Returns:
        True если пользователь является участником канала.
    """
    try:
        member = await bot.get_chat_member(
            chat_id=settings.CHANNEL_USERNAME,
            user_id=user_id,
        )
        is_subscribed = member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        )
        logger.info(
            "Проверка подписки user_id=%s: %s (статус=%s)",
            user_id,
            is_subscribed,
            member.status,
        )
        return is_subscribed
    except Exception as e:
        logger.error("Ошибка проверки подписки user_id=%s: %s", user_id, e)
        return False
