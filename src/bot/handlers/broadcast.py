"""Команда /broadcast — массовая рассылка.

Формат: /broadcast Текст сообщения
"""

import asyncio
import logging

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.config import settings
from src.database.crud import get_all_user_ids

router = Router()
logger = logging.getLogger(__name__)


class BroadcastStates(StatesGroup):
    confirm = State()


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message, state: FSMContext) -> None:
    """Инициация рассылки."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    text = message.text
    if text is None:
        return

    broadcast_text = text.removeprefix("/broadcast").strip()

    if not broadcast_text:
        await message.answer(
            "❌ Укажите текст рассылки.\n\n"
            "Формат: <code>/broadcast Ваш текст сообщения</code>"
        )
        return

    user_ids = await get_all_user_ids()
    user_count = len(user_ids)

    if user_count == 0:
        await message.answer("❌ Нет пользователей для рассылки.")
        return

    await state.update_data(broadcast_text=broadcast_text)
    await state.set_state(BroadcastStates.confirm)

    preview = broadcast_text[:200] + ("..." if len(broadcast_text) > 200 else "")

    await message.answer(
        f"📢 <b>Подтверждение рассылки</b>\n\n"
        f"Текст:\n{preview}\n\n"
        f"👥 Получателей: <b>{user_count}</b>\n\n"
        f"Отправить?",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Отправить", callback_data="broadcast_confirm"),
                    InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel"),
                ]
            ]
        ),
    )


@router.callback_query(F.data == "broadcast_confirm", BroadcastStates.confirm)
async def broadcast_confirm(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """Подтверждение и выполнение рассылки."""
    if callback.from_user.id != settings.ADMIN_ID:
        return

    data = await state.get_data()
    broadcast_text = data.get("broadcast_text", "")
    await state.clear()

    if not broadcast_text:
        await callback.answer("Текст рассылки пуст.", show_alert=True)
        return

    await callback.answer()
    await callback.message.edit_text("⏳ Рассылка запущена...")

    user_ids = await get_all_user_ids()
    total = len(user_ids)
    sent = 0
    failed = 0

    for i, uid in enumerate(user_ids, 1):
        try:
            await bot.send_message(chat_id=uid, text=broadcast_text)
            sent += 1
        except Exception as e:
            failed += 1
            logger.warning("Broadcast fail uid=%s: %s", uid, e)

        if i % 10 == 0 or i == total:
            try:
                await callback.message.edit_text(
                    f"⏳ Рассылка: {i}/{total}\n✅ Доставлено: {sent}\n❌ Ошибок: {failed}"
                )
            except Exception:
                pass

        await asyncio.sleep(0.05)

    await callback.message.edit_text(
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"📊 Всего: {total}\n✅ Доставлено: {sent}\n❌ Ошибок: {failed}"
    )
    logger.info("Broadcast: total=%d, sent=%d, failed=%d", total, sent, failed)


@router.callback_query(F.data == "broadcast_cancel", BroadcastStates.confirm)
async def broadcast_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    """Отмена рассылки."""
    if callback.from_user.id != settings.ADMIN_ID:
        return

    await state.clear()
    await callback.message.edit_text("❌ Рассылка отменена.")
    await callback.answer()
