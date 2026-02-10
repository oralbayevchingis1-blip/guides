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

from src.bot.utils.ai_client import ask_legal
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
        "🤖 *AI Мини-консультация от SOLIS Partners*\n\n"
        "Задайте ваш юридический вопрос, и наш AI-ассистент "
        "даст краткий ответ на основе казахстанского законодательства.\n\n"
        "⚖️ _Обратите внимание: это предварительная консультация, "
        "не заменяющая полноценную юридическую помощь._\n\n"
        "Напишите ваш вопрос 👇",
    )


@router.callback_query(F.data == "start_consult")
async def start_consult_callback(
    callback: CallbackQuery, state: FSMContext
) -> None:
    """Начало консультации по нажатию кнопки."""
    await state.set_state(ConsultStates.waiting_for_question)
    await callback.message.answer(
        "🤖 *AI Мини-консультация от SOLIS Partners*\n\n"
        "Задайте ваш юридический вопрос 👇\n\n"
        "⚖️ _Ответ носит ознакомительный характер._",
    )
    await callback.answer()


# ──────────────────────── Обработка вопроса ───────────────────────────


@router.message(ConsultStates.waiting_for_question)
async def process_question(
    message: Message, state: FSMContext, google: GoogleSheetsClient
) -> None:
    """Получаем вопрос, отправляем в Gemini, возвращаем ответ."""
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

    # Показываем что думаем
    thinking_msg = await message.answer("🔍 Анализирую ваш вопрос...")

    try:
        answer = await ask_legal(question)

        # Логируем вопрос для Auto-FAQ
        asyncio.create_task(
            google.log_consult(
                user_id=message.from_user.id,
                question=question,
                answer=answer[:300],
            )
        )

        # Формируем ответ с CTA
        response = (
            f"🤖 *Ответ AI-ассистента SOLIS Partners:*\n\n"
            f"{answer}\n\n"
            f"---\n"
            f"⚖️ _Данная информация носит ознакомительный характер "
            f"и не является юридической консультацией._"
        )

        # Кнопки: записаться, задать ещё вопрос, назад к гайдам
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
            # Если markdown не парсится — отправляем без форматирования
            plain = (
                "🤖 Ответ AI-ассистента SOLIS Partners:\n\n"
                f"{answer}\n\n"
                "---\n"
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
