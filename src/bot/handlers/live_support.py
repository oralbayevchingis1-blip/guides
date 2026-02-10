"""Живая поддержка — Shared Inbox: передача диалога от AI к живому юристу.

Поток:
1. Пользователь нажимает «Позвать человека» в /consult.
2. Бот пересылает историю AI-переписки в админ-чат.
3. Юрист отвечает через бота — по user_id.
4. Пользователь получает ответ от живого юриста.

Команды:
- /reply {user_id} {текст} — ответ пользователю из админ-чата.
"""

import logging

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from src.config import settings

router = Router()
logger = logging.getLogger(__name__)

# Хранилище AI-диалогов для пересылки: {user_id: [messages]}
_ai_history: dict[int, list[dict]] = {}

# Активные тикеты: {user_id: True}
_active_tickets: dict[int, bool] = {}


class LiveSupportStates(StatesGroup):
    """FSM для ответа админа пользователю."""
    waiting_for_reply = State()


def save_ai_exchange(user_id: int, question: str, answer: str) -> None:
    """Сохраняет пару вопрос-ответ для Live Support."""
    _ai_history.setdefault(user_id, []).append({
        "question": question[:500],
        "answer": answer[:500],
    })
    # Максимум 10 последних сообщений
    if len(_ai_history[user_id]) > 10:
        _ai_history[user_id] = _ai_history[user_id][-10:]


# ═══════════════════════════════════════════════════════════════════════════
#  КНОПКА «Позвать человека» (вызывается из consult.py)
# ═══════════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "call_human")
async def call_human_support(callback: CallbackQuery) -> None:
    """Пользователь запрашивает живого юриста."""
    user_id = callback.from_user.id
    name = callback.from_user.full_name or ""
    username = callback.from_user.username or ""

    _active_tickets[user_id] = True

    # Подтверждение пользователю
    await callback.message.answer(
        "👨‍⚖️ <b>Запрос на живую консультацию отправлен!</b>\n\n"
        "Наш юрист получил ваш вопрос и историю переписки с AI.\n"
        "Ожидайте ответа — обычно в течение 15-30 минут.\n\n"
        "Или напишите напрямую: @SOLISlegal",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📞 Связаться напрямую", url="https://t.me/SOLISlegal")],
            ]
        ),
    )
    await callback.answer("✅ Запрос отправлен юристу!")

    # Формируем историю AI-диалога
    history = _ai_history.get(user_id, [])
    history_text = ""
    for i, exchange in enumerate(history[-5:], 1):
        q = exchange["question"][:200]
        a = exchange["answer"][:200]
        history_text += f"\n<b>Q{i}:</b> {q}\n<b>A{i}:</b> {a}\n"

    if not history_text:
        history_text = "\n<i>(нет сохранённой истории)</i>\n"

    # Уведомление админу
    try:
        admin_text = (
            f"🆘 <b>ЗАПРОС ЖИВОЙ ПОДДЕРЖКИ</b>\n\n"
            f"👤 {name} (@{username})\n"
            f"🆔 <code>{user_id}</code>\n\n"
            f"📜 <b>История AI-диалога:</b>"
            f"{history_text}\n"
            f"───────────────\n"
            f"Для ответа: <code>/reply {user_id} ваш текст</code>"
        )

        await callback.message.bot.send_message(
            chat_id=settings.ADMIN_ID,
            text=admin_text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(
                        text=f"💬 Написать @{username}" if username else "💬 Ответить",
                        url=f"https://t.me/{username}" if username else f"https://t.me/SOLISlegal",
                    )],
                    [InlineKeyboardButton(
                        text=f"✍️ Ответить через бота",
                        callback_data=f"reply_to_{user_id}",
                    )],
                ]
            ),
        )
    except Exception as e:
        logger.error("Live support admin notification failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════════
#  ОТВЕТ ЮРИСТА через /reply
# ═══════════════════════════════════════════════════════════════════════════


@router.message(Command("reply"))
async def cmd_reply(message: Message, bot: Bot) -> None:
    """Юрист отвечает пользователю: /reply {user_id} {текст}."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    text = (message.text or "").removeprefix("/reply").strip()
    if not text:
        await message.answer(
            "Формат: <code>/reply USER_ID ваш текст ответа</code>"
        )
        return

    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Укажите USER_ID и текст ответа.")
        return

    try:
        target_user_id = int(parts[0])
    except ValueError:
        await message.answer("Некорректный USER_ID.")
        return

    reply_text = parts[1]

    try:
        await bot.send_message(
            chat_id=target_user_id,
            text=(
                f"👨‍⚖️ <b>Ответ юриста SOLIS Partners:</b>\n\n"
                f"{reply_text}\n\n"
                f"───────────────\n"
                f"Для дальнейших вопросов: /consult или @SOLISlegal"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Задать ещё вопрос", callback_data="start_consult")],
                    [InlineKeyboardButton(text="📞 Связаться", url="https://t.me/SOLISlegal")],
                ]
            ),
        )
        # Закрываем тикет
        _active_tickets.pop(target_user_id, None)
        await message.answer(f"✅ Ответ отправлен пользователю {target_user_id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки: {e}")


@router.callback_query(F.data.startswith("reply_to_"))
async def reply_to_user_callback(callback: CallbackQuery, state: FSMContext) -> None:
    """Начинает FSM для ответа через кнопку."""
    if callback.from_user.id != settings.ADMIN_ID:
        await callback.answer("⛔ Только для администратора")
        return

    target_id = callback.data.removeprefix("reply_to_")
    await state.update_data(reply_target=target_id)
    await state.set_state(LiveSupportStates.waiting_for_reply)

    await callback.message.answer(
        f"✍️ Введите ответ для пользователя <code>{target_id}</code>:"
    )
    await callback.answer()


@router.message(LiveSupportStates.waiting_for_reply)
async def send_reply(message: Message, state: FSMContext, bot: Bot) -> None:
    """Отправляет ответ юриста пользователю."""
    if message.from_user.id != settings.ADMIN_ID:
        return

    data = await state.get_data()
    target_id = data.get("reply_target")
    await state.clear()

    if not target_id:
        await message.answer("Ошибка: цель ответа не найдена.")
        return

    reply_text = message.text or ""
    if not reply_text:
        await message.answer("Пустой ответ не отправлен.")
        return

    try:
        await bot.send_message(
            chat_id=int(target_id),
            text=(
                f"👨‍⚖️ <b>Ответ юриста SOLIS Partners:</b>\n\n"
                f"{reply_text}\n\n"
                f"───────────────\n"
                f"Для дальнейших вопросов: /consult или @SOLISlegal"
            ),
        )
        _active_tickets.pop(int(target_id), None)
        await message.answer(f"✅ Ответ отправлен пользователю {target_id}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


def get_active_tickets() -> dict[int, bool]:
    """Возвращает активные тикеты."""
    return dict(_active_tickets)
