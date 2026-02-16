"""Публикация постов в Telegram-канал @SOLISlegal.

Возможности:
    — Анонс нового гайда (при загрузке через /admin)
    — Еженедельный дайджест «Топ гайдов»
    — Ручная публикация через /channel_post
"""

import logging
from typing import Optional

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from src.config import settings

logger = logging.getLogger(__name__)


def _bot_link(bot_username: str, guide_id: str, source: str = "channel") -> str:
    """Deep link на гайд с UTM."""
    return f"https://t.me/{bot_username}?start=guide_{guide_id}--{source}"


async def post_new_guide(
    bot: Bot,
    guide: dict,
    *,
    bot_username: Optional[str] = None,
) -> bool:
    """Публикует анонс нового гайда в канал.

    Returns:
        True если пост отправлен успешно.
    """
    if not bot_username:
        info = await bot.get_me()
        bot_username = info.username

    title = guide.get("title", "Новый гайд")
    desc = guide.get("description", "")
    guide_id = guide.get("id", "")

    link = _bot_link(bot_username, guide_id)

    text = (
        f"🔹 <b>Новый гайд: {title}</b>\n\n"
    )
    if desc:
        text += f"{desc}\n\n"
    text += (
        "Скачайте бесплатный PDF с пошаговыми инструкциями "
        "и чек-листами прямо в нашем боте 👇"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔹 Скачать гайд", url=link)],
    ])

    try:
        await bot.send_message(
            chat_id=settings.CHANNEL_USERNAME,
            text=text,
            reply_markup=kb,
        )
        logger.info("Channel post sent: new guide '%s'", title)
        return True
    except Exception as e:
        logger.error("Failed to post to channel: %s", e)
        return False


async def post_weekly_digest(
    bot: Bot,
    catalog: list[dict],
    *,
    bot_username: Optional[str] = None,
    top_n: int = 3,
) -> bool:
    """Публикует еженедельный дайджест с подборкой гайдов."""
    if not catalog:
        return False

    if not bot_username:
        info = await bot.get_me()
        bot_username = info.username

    # Берём первые top_n гайдов (или случайные — можно усложнить позже)
    selected = catalog[:top_n]

    lines = ["🔹 <b>Подборка бесплатных гайдов от SOLIS Partners</b>\n"]

    for guide in selected:
        title = guide.get("title", "?")
        desc = guide.get("description", "")
        short_desc = f" — {desc[:80]}" if desc else ""
        lines.append(f"— <b>{title}</b>{short_desc}")

    lines.append(
        "\nВсе гайды — в формате PDF с пошаговыми инструкциями, "
        "чек-листами и примерами документов.\n\n"
        "Скачивайте бесплатно в нашем боте 👇"
    )

    text = "\n".join(lines)

    start_link = f"https://t.me/{bot_username}?start=digest--channel"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔹 Открыть бота", url=start_link)],
    ])

    try:
        await bot.send_message(
            chat_id=settings.CHANNEL_USERNAME,
            text=text,
            reply_markup=kb,
        )
        logger.info("Channel weekly digest posted (%d guides)", len(selected))
        return True
    except Exception as e:
        logger.error("Failed to post digest to channel: %s", e)
        return False


async def post_custom(bot: Bot, text: str) -> bool:
    """Публикует произвольный текст в канал."""
    try:
        await bot.send_message(chat_id=settings.CHANNEL_USERNAME, text=text)
        logger.info("Custom channel post sent")
        return True
    except Exception as e:
        logger.error("Failed to post to channel: %s", e)
        return False
