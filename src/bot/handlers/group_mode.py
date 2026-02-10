"""Режим «Сотрудник» для групп — мониторинг юридических чатов.

Когда бота добавляют в группу:
1. Переходит в режим мониторинга (без спама).
2. Реагирует на прямое упоминание (@bot) или ключевые фразы.
3. AI анализирует контекст чата и даёт предварительный ответ из Data Room.
4. Предлагает записаться на полноценную консультацию.

Ключевые фразы-триггеры:
  «нужна консультация», «юрист», «помощь», «вопрос по закону»,
  «подскажите», «как быть», «что делать», «правовой вопрос»
"""

import asyncio
import logging
import re

from aiogram import Bot, F, Router
from aiogram.enums import ChatType
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.config import settings

router = Router()
logger = logging.getLogger(__name__)

# Фразы-триггеры (case-insensitive)
TRIGGER_PHRASES = [
    r"нужна консультация",
    r"юридическ\w+ вопрос",
    r"помощь юрист",
    r"подскажите.*закон",
    r"как быть.*юридич",
    r"что делать.*право",
    r"правовой вопрос",
    r"нужен юрист",
    r"вопрос по закону",
    r"трудовой кодекс",
    r"налогов\w+ вопрос",
]

TRIGGER_PATTERN = re.compile(
    "|".join(TRIGGER_PHRASES),
    re.IGNORECASE,
)

# Cooldown: не отвечаем в одном чате чаще 1 раза в 5 минут
_group_cooldown: dict[int, float] = {}
COOLDOWN_SECONDS = 300


def _is_group(message: Message) -> bool:
    """Проверяет, что сообщение из группы или супергруппы."""
    return message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP)


def _is_triggered(message: Message, bot_username: str = "") -> bool:
    """Проверяет, упомянут ли бот или есть ключевая фраза."""
    text = message.text or message.caption or ""
    if not text:
        return False

    # Прямое упоминание
    if bot_username and f"@{bot_username}" in text:
        return True

    # Ответ на сообщение бота
    if message.reply_to_message and message.reply_to_message.from_user:
        # Если отвечают на сообщение бота (будет проверено в runtime)
        pass

    # Ключевые фразы
    if TRIGGER_PATTERN.search(text):
        return True

    return False


@router.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def group_monitor(
    message: Message,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Мониторинг групповых чатов — отвечает на триггеры."""
    if message.from_user is None:
        return

    # Не отвечаем на сообщения ботов
    if message.from_user.is_bot:
        return

    bot_info = await bot.get_me()
    bot_username = bot_info.username or ""

    if not _is_triggered(message, bot_username):
        return

    # Cooldown check
    import time
    chat_id = message.chat.id
    now = time.time()
    last = _group_cooldown.get(chat_id, 0)
    if now - last < COOLDOWN_SECONDS:
        return
    _group_cooldown[chat_id] = now

    text = message.text or ""
    logger.info("Group trigger in chat=%s: '%s...'", chat_id, text[:50])

    try:
        # RAG контекст из Data Room
        from src.bot.utils.rag import find_relevant_context
        from src.bot.utils.ai_client import ask_legal_safe

        context = await find_relevant_context(text, google, cache)
        answer = await ask_legal_safe(text, context=context)

        # Короткий ответ для группы (не больше 500 символов)
        short_answer = answer[:500]
        if len(answer) > 500:
            short_answer += "..."

        response = (
            f"⚖️ <b>AI-юрист SOLIS Partners</b>\n\n"
            f"{short_answer}\n\n"
            f"───────────────\n"
            f"<i>Это предварительная оценка. "
            f"Для полной консультации перейдите в бота.</i>"
        )

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text="🤖 Полная AI-консультация",
                    url=f"https://t.me/{bot_username}?start=group_consult",
                )],
                [InlineKeyboardButton(
                    text="📞 Живой юрист",
                    url="https://t.me/SOLISlegal",
                )],
            ]
        )

        try:
            await message.reply(response, reply_markup=keyboard)
        except Exception:
            await message.reply(response, reply_markup=keyboard, parse_mode=None)

    except Exception as e:
        logger.error("Group AI error: %s", e)
        # Fallback: просто предлагаем перейти в бота
        await message.reply(
            "⚖️ Могу помочь! Перейдите в мой личный чат для AI-консультации:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(
                        text="🤖 Перейти в бота",
                        url=f"https://t.me/{bot_username}?start=group_consult",
                    )],
                ]
            ),
        )
