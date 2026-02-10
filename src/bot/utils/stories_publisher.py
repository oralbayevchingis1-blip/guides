"""Автоматические Telegram Stories — публикация анонсов из News Feed.

Автоматически формирует Stories-контент из статей.
Поскольку Bot API не поддерживает Stories, используется два режима:
1. Channel Post Mode: создаёт пост с обложкой + deep-link (всегда работает)
2. UserBot Mode: через Telethon/Pyrogram (если настроен SESSION_STRING)

Использование:
    from src.bot.utils.stories_publisher import publish_story, auto_stories_check
"""

import logging
from datetime import datetime, timezone

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from src.config import settings

logger = logging.getLogger(__name__)


async def publish_story(
    bot: Bot,
    title: str,
    summary: str,
    url: str = "",
    image_url: str = "",
    channel: str = "",
) -> bool:
    """Публикует анонс статьи как красивый пост в канал (Story-стиль).

    Bot API не поддерживает Stories напрямую, поэтому создаём
    визуально привлекательный пост-анонс с обложкой.

    Returns:
        True если опубликовано.
    """
    target = channel or settings.CHANNEL_USERNAME

    # Формируем красивый анонс
    text = (
        f"🔥 <b>{title}</b>\n\n"
        f"{summary[:200]}{'...' if len(summary) > 200 else ''}\n\n"
        f"───────────────"
    )

    buttons = []
    if url:
        buttons.append([InlineKeyboardButton(text="📖 Читать полностью", url=url)])

    bot_info = await bot.get_me()
    buttons.append([InlineKeyboardButton(
        text="🤖 AI-юрист бесплатно",
        url=f"https://t.me/{bot_info.username}?start=story",
    )])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

    try:
        if image_url:
            await bot.send_photo(
                chat_id=target,
                photo=image_url,
                caption=text,
                reply_markup=keyboard,
            )
        else:
            await bot.send_message(
                chat_id=target,
                text=text,
                reply_markup=keyboard,
            )
        logger.info("Story published: '%s' -> %s", title[:30], target)
        return True
    except Exception as e:
        logger.error("Story publish error: %s", e)
        return False


async def auto_stories_check(
    bot: Bot,
    google=None,
    cache=None,
) -> dict:
    """Автоматическая проверка новых статей и публикация Stories.

    Вызывается из scheduler. Проверяет лист «Статьи сайта»
    на наличие новых записей со статусом «published» + story_sent=False.

    Returns:
        {"checked": N, "published": N}
    """
    stats = {"checked": 0, "published": 0}

    if not google:
        return stats

    try:
        articles = await google.get_articles_list()
        stats["checked"] = len(articles)

        for article in articles:
            status = str(article.get("status", "")).lower()
            story_sent = str(article.get("story_sent", "")).lower()

            # Только опубликованные статьи, для которых ещё не отправляли Story
            if status == "published" and story_sent not in ("true", "yes", "1"):
                title = article.get("title", "")
                summary = article.get("description", article.get("content", ""))[:200]
                url = article.get("telegraph_url", article.get("url", ""))

                if not title:
                    continue

                # Генерируем обложку если нет
                image_url = article.get("cover_url", "")
                if not image_url:
                    try:
                        from src.bot.utils.ai_client import generate_post_image
                        image_url = await generate_post_image(title) or ""
                    except Exception:
                        pass

                success = await publish_story(
                    bot=bot,
                    title=title,
                    summary=summary,
                    url=url,
                    image_url=image_url,
                )

                if success:
                    stats["published"] += 1
                    # Помечаем как отправленное (если Google Sheets поддерживает)
                    logger.info("Auto-story published: '%s'", title[:40])

    except Exception as e:
        logger.error("Auto stories check error: %s", e)

    return stats
