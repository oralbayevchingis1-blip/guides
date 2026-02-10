"""Реферальная программа.

Пользователь получает уникальную ссылку t.me/bot?start=ref_{user_id}.
Когда по ней приходит новый пользователь:
- Реферер получает уведомление и доступ к бонусному контенту.
- Реферал проходит обычную воронку, но с пометкой источника.

Команда: /referral — получить свою реферальную ссылку.
"""

import logging

from aiogram import Bot, Router
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.config import settings
from src.database.crud import count_referrals, save_referral

router = Router()
logger = logging.getLogger(__name__)


@router.message(Command("referral"))
async def cmd_referral(message: Message, bot: Bot) -> None:
    """Показывает реферальную ссылку и статистику пользователя."""
    if message.from_user is None:
        return

    user_id = message.from_user.id
    bot_info = await bot.get_me()
    bot_username = bot_info.username

    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"

    # Считаем рефералов
    ref_count = await count_referrals(user_id)

    text = (
        "🤝 *Реферальная программа SOLIS Partners*\n\n"
        f"Ваша уникальная ссылка:\n`{ref_link}`\n\n"
        "Поделитесь с коллегами — когда они перейдут по вашей ссылке "
        "и скачают гайд, вы получите доступ к эксклюзивным материалам!\n\n"
        f"👥 Приведено друзей: *{ref_count}*\n"
    )

    # Кнопка «Поделиться»
    share_text = (
        "Рекомендую бесплатные юридические гайды от SOLIS Partners! "
        "Полезно для IT-бизнеса, стартапов и корпоративного права 🇰🇿"
    )
    share_url = f"https://t.me/share/url?url={ref_link}&text={share_text}"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📤 Поделиться ссылкой",
                    url=share_url,
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📚 Посмотреть гайды",
                    callback_data="show_all_guides",
                ),
            ],
        ]
    )

    await message.answer(text, reply_markup=keyboard)


async def notify_referrer(
    bot: Bot,
    referrer_id: int,
    new_user_name: str,
) -> None:
    """Уведомляет реферера о новом пользователе по его ссылке.

    Args:
        bot: Экземпляр бота.
        referrer_id: User ID реферера.
        new_user_name: Имя нового пользователя.
    """
    try:
        await bot.send_message(
            chat_id=referrer_id,
            text=(
                f"🎉 Отличная новость!\n\n"
                f"По вашей реферальной ссылке пришёл новый пользователь: "
                f"*{new_user_name}*\n\n"
                f"Спасибо за рекомендацию! 🤝"
            ),
        )
    except Exception as e:
        logger.warning("Не удалось уведомить реферера %s: %s", referrer_id, e)
