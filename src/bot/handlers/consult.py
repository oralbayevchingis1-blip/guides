"""AI мини-консультация через Gemini.

Пользователь задаёт юридический вопрос — Gemini даёт краткий ответ
с контекстом SOLIS Partners и предлагает записаться на консультацию.

Команда: /consult или кнопка "Задать вопрос юристу"
"""

import asyncio
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

from src.bot.utils.ai_client import ask_legal_safe
from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.config import settings

router = Router()
logger = logging.getLogger(__name__)


class ConsultStates(StatesGroup):
    """Состояния AI-консультации."""

    waiting_for_question = State()


# ──────────────────────── Команда /consult ────────────────────────────


@router.message(Command("consult"))
async def cmd_consult(message: Message, state: FSMContext) -> None:
    """Начало AI мини-консультации."""
    if message.from_user is None:
        return

    await state.set_state(ConsultStates.waiting_for_question)
    await message.answer(
        "🤖 <b>AI Мини-консультация от SOLIS Partners</b>\n\n"
        "Задайте ваш юридический вопрос, и наш AI-ассистент "
        "даст краткий ответ на основе казахстанского законодательства.\n\n"
        "⚖️ <i>Обратите внимание: это предварительная консультация, "
        "не заменяющая полноценную юридическую помощь.</i>\n\n"
        "Напишите ваш вопрос 👇",
    )


@router.callback_query(F.data == "start_consult")
async def start_consult_callback(
    callback: CallbackQuery, state: FSMContext
) -> None:
    """Начало консультации по нажатию кнопки."""
    await state.set_state(ConsultStates.waiting_for_question)
    await callback.message.answer(
        "🤖 <b>AI Мини-консультация от SOLIS Partners</b>\n\n"
        "Задайте ваш юридический вопрос 👇\n\n"
        "⚖️ <i>Ответ носит ознакомительный характер.</i>",
    )
    await callback.answer()


# ──────────────────────── Обработка вопроса ───────────────────────────


@router.message(ConsultStates.waiting_for_question)
async def process_question(
    message: Message, state: FSMContext, google: GoogleSheetsClient, cache: TTLCache,
) -> None:
    """Получаем вопрос, отправляем в Gemini (с RAG-контекстом), возвращаем ответ."""
    question = message.text.strip() if message.text else ""

    # Пропуск команд
    if question.startswith("/"):
        await state.clear()
        return

    if not question or len(question) < 5:
        await message.answer(
            "Пожалуйста, опишите ваш вопрос подробнее (минимум 5 символов)."
        )
        return

    # P5: Телеметрия
    try:
        from src.bot.utils.telemetry import track_event
        asyncio.create_task(track_event(message.from_user.id, "consult_question"))
    except Exception:
        pass

    # C5: Sentiment Analysis — определение срочности
    try:
        from src.bot.utils.email_sender import analyze_sentiment, send_urgency_alert
        sentiment = analyze_sentiment(question)
        if sentiment["needs_alert"]:
            asyncio.create_task(
                send_urgency_alert(message.bot, message.from_user.id, question, sentiment)
            )
    except Exception:
        pass

    # Показываем что думаем
    thinking_msg = await message.answer("🔍 Анализирую ваш вопрос...")

    try:
        # L3+C8: Legal Search + Practice Area context
        from src.bot.utils.legal_search import search_legal_context
        from src.bot.utils.vector_search import (
            get_practice_context,
            search_consult_history,
            format_search_results,
        )

        rag_context = await search_legal_context(question, google, cache)

        # C8: Practice Area AI — узкоспециализированный контекст
        practice_ctx = get_practice_context(question)
        if practice_ctx:
            rag_context = f"{practice_ctx}\n\n{rag_context}" if rag_context else practice_ctx

        # C9: Vector Search 2.0 — похожие прецеденты
        try:
            similar = await search_consult_history(question, google, cache, top_k=3)
            precedent_ctx = format_search_results(similar)
            if precedent_ctx:
                rag_context = f"{rag_context}\n\n{precedent_ctx}" if rag_context else precedent_ctx
        except Exception:
            pass

        answer = await ask_legal_safe(question, context=rag_context)

        # Логируем вопрос для Auto-FAQ
        asyncio.create_task(
            google.log_consult(
                user_id=message.from_user.id,
                question=question,
                answer=answer[:300],
            )
        )

        # Lead Scoring — фоновый анализ потенциала клиента
        try:
            from src.bot.utils.lead_scoring import analyze_and_score_lead
            asyncio.create_task(
                analyze_and_score_lead(message.from_user.id, google, cache, message.bot)
            )
        except Exception:
            pass  # scoring is non-critical

        # Планируем NPS-запрос через 2 часа
        try:
            from src.bot.handlers.feedback import schedule_feedback
            from src.bot.utils.scheduler import get_scheduler
            scheduler = get_scheduler()
            if scheduler:
                schedule_feedback(scheduler, message.bot, message.from_user.id, delay_hours=2.0)
        except Exception:
            pass  # NPS is non-critical

        # Формируем ответ с CTA (HTML)
        response = (
            f"🤖 <b>Ответ AI-ассистента SOLIS Partners:</b>\n\n"
            f"{answer}\n\n"
            f"───────────────\n"
            f"⚖️ <i>Данная информация носит ознакомительный характер "
            f"и не является юридической консультацией.</i>"
        )

        # Сохраняем для Live Support
        try:
            from src.bot.handlers.live_support import save_ai_exchange
            save_ai_exchange(message.from_user.id, question, answer[:500])
        except Exception:
            pass

        # P5: Телеметрия — ответ получен
        try:
            from src.bot.utils.telemetry import track_event
            asyncio.create_task(track_event(message.from_user.id, "consult_answered"))
        except Exception:
            pass

        # Карма за консультацию
        try:
            from src.bot.utils.karma import add_karma
            add_karma(message.from_user.id, 0, "consult")
        except Exception:
            pass

        # Кнопки: записаться, задать ещё, позвать человека, гайды
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="📞 Записаться на консультацию",
                        url="https://t.me/SOLISlegal",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="🔄 Задать ещё вопрос",
                        callback_data="start_consult",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="👨‍⚖️ Позвать живого юриста",
                        callback_data="call_human",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        text="📚 Посмотреть гайды",
                        callback_data="show_all_guides",
                    ),
                ],
            ]
        )

        # Удаляем сообщение "Анализирую..."
        try:
            await thinking_msg.delete()
        except Exception:
            pass

        try:
            await message.answer(response, reply_markup=keyboard)
        except Exception:
            # Если HTML не парсится — отправляем без форматирования
            plain = (
                "🤖 Ответ AI-ассистента SOLIS Partners:\n\n"
                f"{answer}\n\n"
                "───────────────\n"
                "⚖️ Данная информация носит ознакомительный характер "
                "и не является юридической консультацией."
            )
            await message.answer(plain, reply_markup=keyboard, parse_mode=None)

        logger.info(
            "AI-консультация: user_id=%s, вопрос=%s",
            message.from_user.id,
            question[:50],
        )

    except Exception as e:
        logger.error("Ошибка Gemini: %s", e)
        try:
            await thinking_msg.delete()
        except Exception:
            pass

        await message.answer(
            "❌ Извините, AI-ассистент временно недоступен.\n\n"
            "Вы можете задать вопрос напрямую нашим юристам:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="📞 Связаться с юристом",
                            url="https://t.me/SOLISlegal",
                        ),
                    ],
                ]
            ),
        )

    # Сбрасываем состояние
    await state.clear()
