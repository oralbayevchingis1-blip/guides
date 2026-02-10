"""Waitlist — обработчики для списков ожидания.

Пользователь может записаться в waitlist для услуг "Coming Soon".
При релизе — получает уведомление.

Доступ: через меню /start или inline-кнопки.
Admin: /waitlist — управление списками ожидания.
"""

import logging

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.bot.utils.waitlist import (
    add_to_waitlist,
    get_all_waitlists,
    get_coming_soon,
    get_waitlist_count,
    notify_waitlist_release,
)
from src.config import settings

router = Router()
logger = logging.getLogger(__name__)


@router.message(Command("waitlist"))
async def cmd_waitlist(message: Message, google=None, cache=None) -> None:
    """Показывает доступные Coming Soon услуги."""
    if not google:
        await message.answer("⚠️ Сервис временно недоступен.")
        return

    try:
        data_room = await google.get_data_room()
    except Exception:
        data_room = []

    coming = get_coming_soon(data_room)

    if not coming:
        await message.answer(
            "📋 Сейчас нет новых услуг в разработке.\n\n"
            "Мы уведомим вас, когда появится что-то интересное!"
        )
        return

    text = "🚀 <b>Скоро запустим:</b>\n\n"
    buttons = []

    for svc in coming:
        wl_count = get_waitlist_count(svc["id"])
        text += (
            f"📌 <b>{svc['title']}</b>\n"
            f"   {svc.get('description', '')[:100]}\n"
            f"   👥 В ожидании: {wl_count}\n\n"
        )
        buttons.append([InlineKeyboardButton(
            text=f"📝 Записаться: {svc['title'][:30]}",
            callback_data=f"wl_{svc['id'][:40]}",
        )])

    await message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("wl_"))
async def join_waitlist(callback: CallbackQuery) -> None:
    """Пользователь записывается в waitlist."""
    service_id = callback.data.removeprefix("wl_")
    user_id = callback.from_user.id

    added = add_to_waitlist(service_id, user_id)
    count = get_waitlist_count(service_id)

    if added:
        await callback.answer("✅ Вы в списке ожидания!", show_alert=True)
        await callback.message.answer(
            f"📋 <b>Записано!</b>\n\n"
            f"Вы в списке ожидания для «<b>{service_id}</b>».\n"
            f"👥 Всего ожидают: {count}\n\n"
            f"Мы уведомим вас первым при запуске!"
        )
    else:
        await callback.answer("ℹ️ Вы уже в списке!", show_alert=True)


@router.callback_query(F.data.startswith("wl_release_"))
async def release_waitlist(callback: CallbackQuery, bot: Bot) -> None:
    """Админ запускает уведомление waitlist (релиз услуги)."""
    if callback.from_user.id != settings.ADMIN_ID:
        await callback.answer("⛔ Только для администратора")
        return

    service_id = callback.data.removeprefix("wl_release_")
    result = await notify_waitlist_release(bot, service_id)

    await callback.message.answer(
        f"📢 Waitlist «{service_id}» уведомлён!\n\n"
        f"📊 Всего: {result['total']}\n"
        f"✅ Отправлено: {result['sent']}\n"
        f"❌ Ошибок: {result['failed']}"
    )
    await callback.answer()
