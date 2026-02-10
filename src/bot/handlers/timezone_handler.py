"""Timezone handler — установка часового пояса через /timezone или Location.

Команды:
    /timezone — выбор часового пояса
    Отправка геолокации — автоопределение
"""

import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

from src.bot.utils.timezone_manager import (
    COMMON_TIMEZONES,
    get_user_local_time,
    get_user_tz,
    set_user_timezone,
    timezone_from_location,
)

router = Router()
logger = logging.getLogger(__name__)


@router.message(Command("timezone"))
async def cmd_timezone(message: Message) -> None:
    """Показывает выбор часового пояса."""
    current_tz = get_user_tz(message.from_user.id)
    local_time = get_user_local_time(message.from_user.id)

    text = (
        f"🕐 <b>Ваш часовой пояс</b>\n\n"
        f"Текущий: <code>{current_tz}</code>\n"
        f"Местное время: <b>{local_time.strftime('%H:%M')}</b>\n\n"
        f"Выберите ваш часовой пояс или отправьте геолокацию:"
    )

    buttons = [
        [InlineKeyboardButton(text=label, callback_data=f"tz_{tz}")]
        for label, tz in COMMON_TIMEZONES.items()
    ]

    # Кнопка отправки геолокации
    location_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Определить автоматически", request_location=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await message.answer("Или отправьте геолокацию:", reply_markup=location_kb)


@router.callback_query(F.data.startswith("tz_"))
async def set_timezone_callback(callback: CallbackQuery) -> None:
    """Устанавливает часовой пояс из выбора."""
    tz_str = callback.data.removeprefix("tz_")

    if set_user_timezone(callback.from_user.id, tz_str):
        local_time = get_user_local_time(callback.from_user.id)
        await callback.message.edit_text(
            f"✅ Часовой пояс установлен: <code>{tz_str}</code>\n"
            f"🕐 Местное время: <b>{local_time.strftime('%H:%M')}</b>\n\n"
            f"Утренний дайджест будет приходить в 09:00 по вашему времени."
        )
        await callback.answer("✅ Часовой пояс сохранён!")
    else:
        await callback.answer("❌ Ошибка установки часового пояса", show_alert=True)


@router.message(F.location)
async def handle_location(message: Message) -> None:
    """Определяет часовой пояс по геолокации."""
    lat = message.location.latitude
    lon = message.location.longitude

    tz_str = timezone_from_location(lat, lon)
    set_user_timezone(message.from_user.id, tz_str)

    local_time = get_user_local_time(message.from_user.id)

    await message.answer(
        f"✅ Часовой пояс определён: <code>{tz_str}</code>\n"
        f"🕐 Местное время: <b>{local_time.strftime('%H:%M')}</b>\n\n"
        f"Утренний дайджест будет приходить в 09:00 по вашему времени.",
        reply_markup=ReplyKeyboardRemove(),
    )
