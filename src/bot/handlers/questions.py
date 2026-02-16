"""Задать вопрос юристу — двусторонний канал через бота.

Flow пользователя:
    1. Кнопка «❓ Задать вопрос» → бот: «Опишите ваш вопрос»
    2. Пользователь пишет текст
    3. Бот: «✅ Вопрос принят!» → сохраняем в БД + Sheets → уведомляем админа
    4. Админ нажимает «✍ Ответить» → пишет ответ → бот пересылает пользователю

Flow ответа:
    1. Админ нажимает callback «answer_q_{id}»
    2. Бот: «Напишите ответ:» (FSM)
    3. Админ пишет текст
    4. Бот отправляет пользователю ответ с CTA на консультацию
"""

import asyncio
import logging
from datetime import datetime, timezone

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

from src.bot.keyboards.inline import after_guide_keyboard
from src.bot.utils.ai_assistant import get_ai_answer
from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.monitoring import metrics
from src.bot.utils.throttle import critical_limiter
from src.config import settings
from src.database.crud import (
    answer_question,
    get_lead_by_user_id,
    get_question,
    get_questions_stats,
    get_unanswered_questions,
    get_user_downloaded_guides,
    save_question,
    track,
)

router = Router()
logger = logging.getLogger(__name__)


class QuestionForm(StatesGroup):
    """FSM для сбора вопроса и ответа."""
    waiting_for_question = State()
    waiting_for_answer = State()  # админ отвечает


def _esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── Точки входа (пользователь) ───────────────────────────────────────


@router.callback_query(F.data == "ask_question")
async def ask_question_cb(callback: CallbackQuery, state: FSMContext) -> None:
    """Начало через inline-кнопку."""
    await callback.answer()
    await _start_question(callback.message, state, callback.from_user.id)


@router.message(Command("question"))
async def ask_question_cmd(message: Message, state: FSMContext) -> None:
    """Начало через команду /question."""
    await _start_question(message, state, message.from_user.id)


@router.message(F.text == "❓ Задать вопрос")
async def ask_question_menu(message: Message, state: FSMContext) -> None:
    """Начало через ReplyKeyboard."""
    await _start_question(message, state, message.from_user.id)


async def _start_question(message: Message, state: FSMContext, user_id: int) -> None:
    """Запрашивает текст вопроса."""
    if not critical_limiter.allow(user_id, "question"):
        await message.answer("⏳ Вы недавно уже отправляли вопрос. Подождите немного.")
        return

    await state.clear()
    await state.set_state(QuestionForm.waiting_for_question)
    await state.update_data(question_user_id=user_id)

    await message.answer(
        "🔹 <b>Задайте ваш вопрос юристу</b>\n\n"
        "Опишите вашу ситуацию или вопрос — наш юрист ответит "
        "в течение рабочего дня.\n\n"
        "<i>Например: «Как правильно оформить ТОО с иностранным учредителем?»</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_question")],
        ]),
    )


# ── Получение вопроса ────────────────────────────────────────────────


@router.message(QuestionForm.waiting_for_question)
async def receive_question(
    message: Message,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
) -> None:
    """Сохраняет вопрос и уведомляет админа."""
    text = (message.text or "").strip()

    if text.startswith("/"):
        await state.clear()
        return

    if len(text) < 10:
        await message.answer(
            "Пожалуйста, опишите ваш вопрос подробнее (минимум 10 символов)."
        )
        return

    data = await state.get_data()
    user_id = data.get("question_user_id", message.from_user.id)
    await state.clear()

    # Сохраняем в БД
    question = await save_question(user_id, text)
    metrics.inc("questions_submitted")

    # Воронка
    asyncio.create_task(track(user_id, "ask_question"))

    # Мгновенный ИИ-ответ
    thinking_msg = await message.answer("🔹 <b>Анализирую ваш вопрос...</b>")
    try:
        ai_answer = await get_ai_answer(text)
        await thinking_msg.edit_text(
            f"🔹 <b>Ваш вопрос принят!</b>\n\n{ai_answer}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="🔹 Записаться на консультацию",
                    callback_data="book_consultation",
                )],
                [InlineKeyboardButton(
                    text="🔹 Другие гайды",
                    callback_data="show_categories",
                )],
                [InlineKeyboardButton(
                    text="🔹 Задать ещё вопрос",
                    callback_data="ask_question",
                )],
            ]),
        )
    except Exception as e:
        logger.warning("AI answer display failed: %s", e)
        await thinking_msg.edit_text(
            "🔹 <b>Ваш вопрос принят!</b>\n\n"
            "Наш юрист ответит в течение рабочего дня прямо здесь, в боте.\n\n"
            "А пока — посмотрите наши гайды по юридическим темам:",
            reply_markup=after_guide_keyboard(user_id),
        )

    # Сохраняем в Sheets
    username = message.from_user.username or ""
    asyncio.create_task(
        google.append_question(
            user_id=user_id,
            username=username,
            name=message.from_user.full_name or "",
            question=text,
        )
    )

    # Уведомляем админа
    asyncio.create_task(
        _notify_admin_question(bot, question.id, user_id, username, text)
    )

    logger.info("Question #%s from user %s (AI-assisted)", question.id, user_id)


# ── Уведомление админа ───────────────────────────────────────────────


async def _notify_admin_question(
    bot: Bot,
    question_id: int,
    user_id: int,
    username: str,
    question_text: str,
) -> None:
    """Отправляет админу уведомление с кнопкой «Ответить»."""
    try:
        lead = await get_lead_by_user_id(user_id)
        name = lead.name if lead else "—"
        sphere = getattr(lead, "business_sphere", None) or "" if lead else ""
        email = lead.email if lead else "—"

        guides = await get_user_downloaded_guides(user_id)
        guides_str = ", ".join(guides[:5]) if guides else "нет"
        warmth = "Hot" if len(guides) >= 3 else "Warm" if guides else "Cold"

        username_display = f"@{username}" if username else "нет"
        now = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")

        sphere_line = f"🏢 Сфера: {_esc(sphere)}\n" if sphere else ""

        text = (
            f"❓ <b>Новый вопрос от пользователя!</b>\n\n"
            f"👤 {_esc(name)} ({username_display})\n"
            f"📧 {_esc(email)}\n"
            f"{sphere_line}"
            f"📚 Скачал: {_esc(guides_str)}\n"
            f"📊 Warmth: <b>{warmth}</b>\n\n"
            f"💬 <b>Вопрос:</b>\n"
            f"<i>«{_esc(question_text[:500])}»</i>\n\n"
            f"🕐 {now}\n"
            f"🆔 Question #{question_id} | User <code>{user_id}</code>"
        )

        buttons = [
            [InlineKeyboardButton(
                text="✍ Ответить",
                callback_data=f"answer_q_{question_id}",
            )],
        ]
        if username:
            buttons.append([InlineKeyboardButton(
                text="💬 Написать в Telegram",
                url=f"https://t.me/{username}",
            )])
        buttons.append([InlineKeyboardButton(
            text="📊 Открыть CRM",
            url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
        )])

        msg = await bot.send_message(
            chat_id=settings.ADMIN_ID,
            text=text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )

        # Сохраняем message_id для последующего редактирования
        from src.database.models import async_session, Question
        async with async_session() as session:
            from sqlalchemy import select
            stmt = select(Question).where(Question.id == question_id)
            result = await session.execute(stmt)
            q = result.scalar_one_or_none()
            if q:
                q.admin_message_id = msg.message_id
                await session.commit()

    except Exception as e:
        logger.error("Admin notification for question failed: %s", e)


# ── Ответ админа ─────────────────────────────────────────────────────


@router.callback_query(F.data.startswith("answer_q_"))
async def start_answer(callback: CallbackQuery, state: FSMContext) -> None:
    """Админ начинает отвечать на вопрос."""
    if callback.from_user.id != settings.ADMIN_ID:
        await callback.answer("Только администратор может отвечать.", show_alert=True)
        return

    question_id = int(callback.data.removeprefix("answer_q_"))
    question = await get_question(question_id)

    if not question:
        await callback.answer("Вопрос не найден.", show_alert=True)
        return

    if question.status == "answered":
        await callback.answer("Уже отвечено.", show_alert=True)
        return

    await callback.answer()
    await state.set_state(QuestionForm.waiting_for_answer)
    await state.update_data(answering_question_id=question_id)

    await callback.message.answer(
        f"✍ <b>Напишите ответ на вопрос #{question_id}:</b>\n\n"
        f"<i>«{_esc(question.question_text[:300])}»</i>\n\n"
        "Ваш ответ будет отправлен пользователю от имени бота.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_question")],
        ]),
    )


@router.message(QuestionForm.waiting_for_answer)
async def process_answer(
    message: Message,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
) -> None:
    """Обрабатывает ответ админа и пересылает пользователю."""
    text = (message.text or "").strip()

    if text.startswith("/"):
        await state.clear()
        return

    if len(text) < 5:
        await message.answer("Пожалуйста, напишите ответ подробнее.")
        return

    data = await state.get_data()
    question_id = data.get("answering_question_id")
    await state.clear()

    if not question_id:
        await message.answer("Ошибка: вопрос не найден.")
        return

    # Сохраняем ответ
    question = await answer_question(question_id, text)
    if not question:
        await message.answer("Ошибка: вопрос не найден в базе.")
        return

    metrics.inc("questions_answered")
    asyncio.create_task(track(question.user_id, "question_answered"))

    # Отправляем пользователю
    try:
        user_text = (
            "🔹 <b>Ответ юриста на ваш вопрос:</b>\n\n"
            f"{_esc(text)}\n\n"
            "Для детального разбора вашей ситуации запишитесь "
            "на бесплатную 15-минутную консультацию."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔹 Записаться на консультацию", callback_data="book_consultation")],
            [InlineKeyboardButton(text="🔹 Задать ещё вопрос", callback_data="ask_question")],
            [InlineKeyboardButton(text="🔹 Все гайды", callback_data="show_categories")],
        ])
        await bot.send_message(chat_id=question.user_id, text=user_text, reply_markup=kb)

        await message.answer(
            f"✅ Ответ на вопрос #{question_id} отправлен пользователю."
        )

    except Exception as e:
        logger.error("Failed to send answer to user %s: %s", question.user_id, e)
        await message.answer(
            f"⚠️ Ответ сохранён, но не удалось отправить пользователю: {e}"
        )

    # Обновляем Sheets
    asyncio.create_task(
        google.update_question_answer(question_id, text)
    )

    logger.info("Question #%s answered", question_id)


# ── Отмена ───────────────────────────────────────────────────────────


@router.callback_query(F.data == "cancel_question")
async def cancel_question(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        "Хорошо. Если появится вопрос — нажмите «❓ Задать вопрос» в меню."
    )
    await callback.answer()


# ── Админ-команда /questions ─────────────────────────────────────────


@router.message(Command("questions"))
async def cmd_questions(message: Message) -> None:
    """Показывает список неотвеченных вопросов."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    try:
        stats = await get_questions_stats()
        unanswered = await get_unanswered_questions(limit=10)

        lines = [
            f"❓ <b>Вопросы пользователей</b>\n",
            f"📊 Всего: {stats['total']} | Без ответа: <b>{stats['unanswered']}</b> | "
            f"Отвечено: {stats['answered']}\n",
        ]

        if unanswered:
            lines.append("<b>Ожидают ответа:</b>\n")
            for q in unanswered:
                age = ""
                if q.created_at:
                    delta = datetime.now(timezone.utc) - q.created_at
                    if delta.days:
                        age = f" ({delta.days}д назад)"
                    else:
                        hours = delta.seconds // 3600
                        age = f" ({hours}ч назад)" if hours else " (только что)"
                lines.append(
                    f"  #{q.id} <code>{q.user_id}</code>{age}\n"
                    f"    «{_esc(q.question_text[:80])}…»"
                )
        else:
            lines.append("✅ Все вопросы отвечены!")

        buttons = []
        for q in unanswered[:5]:
            buttons.append([InlineKeyboardButton(
                text=f"✍ Ответить на #{q.id}",
                callback_data=f"answer_q_{q.id}",
            )])

        await message.answer(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None,
        )
    except Exception as e:
        logger.error("Questions command error: %s", e, exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")
