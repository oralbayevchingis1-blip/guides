"""Команда /broadcast — массовая рассылка с умной сегментацией.

Форматы:
    /broadcast Текст                — рассылка всем
    /broadcast #it Текст            — только юзерам с интересами IT
    /broadcast #corporate Текст     — только корпоративное право
    /broadcast #startup Текст       — стартапы и IT-бизнес

Сегменты: it, corporate, startup, finance, tax, labor, aifc, m&a
"""

import asyncio
import logging
import re

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.config import settings
from src.database.crud import get_all_user_ids

router = Router()
logger = logging.getLogger(__name__)

# Регулярка для извлечения тегов сегментации
_TAG_RE = re.compile(r"#(\w+)")


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
            "Формат: <code>/broadcast Ваш текст сообщения</code>\n\n"
            "🎯 <b>Сегментация:</b>\n"
            "<code>/broadcast #it Текст</code> — только IT-юзерам\n"
            "<code>/broadcast #corporate Текст</code> — корпоративное право\n"
            "<code>/broadcast #startup Текст</code> — стартапы\n"
            "<code>/broadcast #finance Текст</code> — финансы\n"
            "<code>/broadcast #all Текст</code> — всем"
        )
        return

    # Извлекаем теги сегментации
    tags = _TAG_RE.findall(broadcast_text)
    clean_text = _TAG_RE.sub("", broadcast_text).strip()
    if not clean_text:
        clean_text = broadcast_text

    # Получаем пользователей (с сегментацией или всех)
    user_ids = await get_all_user_ids()
    segment_label = "всем"

    if tags and "all" not in tags:
        try:
            from src.bot.utils.growth_engine import segment_users
            from src.bot.utils.google_sheets import GoogleSheetsClient

            # Пробуем получить google из middleware data
            google = message.bot.get("google") if hasattr(message.bot, "get") else None
            if google:
                leads = await google.get_recent_leads(limit=500)
                user_ids = segment_users(leads, user_ids, tags)
            segment_label = f"сегмент: {', '.join(tags)}"
        except Exception as e:
            logger.warning("Segmentation failed, sending to all: %s", e)

    user_count = len(user_ids)

    if user_count == 0:
        await message.answer("❌ Нет пользователей для рассылки.")
        return

    # Сохраняем текст в FSM и показываем подтверждение
    await state.update_data(broadcast_text=clean_text, segment_tags=tags)
    await state.set_state(BroadcastStates.confirm)

    preview = clean_text[:200] + ("..." if len(clean_text) > 200 else "")

    await message.answer(
        f"📢 <b>Подтверждение рассылки</b>\n\n"
        f"Текст:\n{preview}\n\n"
        f"👥 Получателей: <b>{user_count}</b>\n"
        f"🎯 Аудитория: <b>{segment_label}</b>\n\n"
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
        f"✅ <b>Рассылка завершена!</b>\n\n"
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
