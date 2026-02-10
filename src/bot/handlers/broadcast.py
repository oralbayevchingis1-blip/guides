"""Команда /broadcast — массовая рассылка сообщений подписчикам.

Доступна только администратору (ADMIN_ID).
Формат: /broadcast Текст сообщения
Бот показывает подтверждение перед отправкой и прогресс.
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
    """Состояния для подтверждения рассылки."""

    confirm = State()


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message, state: FSMContext) -> None:
    """Инициация рассылки. Формат: /broadcast Текст сообщения."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    # Извлекаем текст после команды
    text = message.text
    if text is None:
        return

    # Убираем саму команду
    broadcast_text = text.removeprefix("/broadcast").strip()

    if not broadcast_text:
        await message.answer(
            "❌ Укажите текст рассылки.\n\n"
            "Формат: `/broadcast Ваш текст сообщения`"
        )
        return

    # Получаем количество пользователей
    user_ids = await get_all_user_ids()
    user_count = len(user_ids)

    if user_count == 0:
        await message.answer("❌ Нет пользователей для рассылки.")
        return

    # Сохраняем текст в FSM и показываем подтверждение
    await state.update_data(broadcast_text=broadcast_text)
    await state.set_state(BroadcastStates.confirm)

    preview = broadcast_text[:200] + ("..." if len(broadcast_text) > 200 else "")

    await message.answer(
        f"📢 *Подтверждение рассылки*\n\n"
        f"Текст:\n{preview}\n\n"
        f"👥 Получателей: *{user_count}*\n\n"
        f"Отправить?",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Отправить",
                        callback_data="broadcast_confirm",
                    ),
                    InlineKeyboardButton(
                        text="❌ Отмена",
                        callback_data="broadcast_cancel",
                    ),
                ]
            ]
        ),
    )


@router.callback_query(F.data == "broadcast_confirm", BroadcastStates.confirm)
async def broadcast_confirm(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
) -> None:
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

    # Отправляем каждому пользователю с задержкой (Telegram rate limits)
    for i, uid in enumerate(user_ids, 1):
        try:
            await bot.send_message(chat_id=uid, text=broadcast_text)
            sent += 1
        except Exception as e:
            failed += 1
            logger.warning("Broadcast fail uid=%s: %s", uid, e)

        # Обновляем прогресс каждые 10 сообщений
        if i % 10 == 0 or i == total:
            try:
                await callback.message.edit_text(
                    f"⏳ Рассылка: {i}/{total}\n"
                    f"✅ Доставлено: {sent}\n"
                    f"❌ Ошибок: {failed}"
                )
            except Exception:
                pass  # Telegram может отклонить edit если текст не изменился

        # Задержка для соблюдения лимитов Telegram API
        await asyncio.sleep(0.05)  # ~20 msg/sec

    # Финальный отчёт
    await callback.message.edit_text(
        f"✅ *Рассылка завершена!*\n\n"
        f"📊 Всего: {total}\n"
        f"✅ Доставлено: {sent}\n"
        f"❌ Ошибок: {failed}"
    )

    logger.info(
        "Broadcast завершён: total=%d, sent=%d, failed=%d",
        total, sent, failed,
    )


@router.callback_query(F.data == "broadcast_cancel", BroadcastStates.confirm)
async def broadcast_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    """Отмена рассылки."""
    if callback.from_user.id != settings.ADMIN_ID:
        return

    await state.clear()
    await callback.message.edit_text("❌ Рассылка отменена.")
    await callback.answer()
