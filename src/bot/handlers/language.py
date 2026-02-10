"""Команда /lang — выбор языка интерфейса.

Поддерживаемые языки: RU, KZ, EN.
Автодетекция при первом сообщении.
"""

import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.bot.utils.i18n import LANGUAGES, detect_language, get_user_lang, set_user_lang, t

router = Router()
logger = logging.getLogger(__name__)


def _lang_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора языка."""
    buttons = [
        [InlineKeyboardButton(
            text=f"{label}",
            callback_data=f"lang_{code}",
        )]
        for code, label in LANGUAGES.items()
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(Command("lang"))
async def cmd_lang(message: Message) -> None:
    """Показывает выбор языка."""
    lang = get_user_lang(message.from_user.id)
    text = t("choose_language", lang)
    await message.answer(text, reply_markup=_lang_keyboard())


@router.callback_query(F.data.startswith("lang_"))
async def set_language(callback: CallbackQuery) -> None:
    """Устанавливает язык пользователя."""
    code = callback.data.removeprefix("lang_")
    if code not in LANGUAGES:
        await callback.answer("Неизвестный язык")
        return

    set_user_lang(callback.from_user.id, code)
    label = LANGUAGES[code]

    await callback.message.edit_text(
        f"✅ {label}\n\n"
        f"{t('welcome_subscribed', code)}"
    )
    await callback.answer(f"Язык: {label}")
