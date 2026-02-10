"""AI-стратег: свободный чат, еженедельные сессии, генерация идей.

Команды:
    /chat   — свободный диалог с AI-маркетологом
    /ideas  — быстрая генерация идей контента
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta

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

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.config import settings

router = Router()
logger = logging.getLogger(__name__)

ALMATY_TZ = timezone(timedelta(hours=5))


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id == settings.ADMIN_ID


class StrategyChat(StatesGroup):
    conversation = State()


# ═══════════════════════════════════════════════════════════════════════
#  СВОБОДНЫЙ ЧАТ С AI-СТРАТЕГОМ
# ═══════════════════════════════════════════════════════════════════════


@router.message(Command("chat"))
async def cmd_chat(message: Message, state: FSMContext) -> None:
    """Начинает свободный диалог с AI-маркетологом."""
    if not _is_admin(message.from_user and message.from_user.id):
        return

    await state.set_state(StrategyChat.conversation)
    await state.update_data(history=[])

    await message.answer(
        "🧠 <b>AI-стратег SOLIS Partners</b>\n\n"
        "Я — ваш маркетинговый стратег. Знаю всё о компании, "
        "слежу за новостями и аналитикой.\n\n"
        "Спрашивайте что угодно:\n"
        "• Что постить на этой неделе?\n"
        "• Какой контент работает лучше?\n"
        "• Придумай воронку для новой услуги\n"
        "• Проанализируй наших лидов\n\n"
        "Для выхода: /stop",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💡 Предложи идеи", callback_data="strat_ideas")],
                [InlineKeyboardButton(text="📊 Анализ за неделю", callback_data="strat_weekly")],
                [InlineKeyboardButton(text="🚪 Выход", callback_data="strat_exit")],
            ]
        ),
    )


@router.message(Command("stop"), StrategyChat.conversation)
async def cmd_stop_chat(message: Message, state: FSMContext) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    await state.clear()
    await message.answer("Чат завершён. Возвращайтесь когда угодно — /chat")


@router.callback_query(F.data == "strat_exit")
async def exit_strategy(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.answer("Чат завершён")
    await callback.message.edit_reply_markup(reply_markup=None)


@router.message(StrategyChat.conversation)
async def strategy_conversation(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Обрабатывает сообщения в режиме стратегического чата."""
    if not _is_admin(message.from_user and message.from_user.id):
        return

    user_text = (message.text or "").strip()
    if not user_text:
        return
    # Пропуск команд — выходим из чата
    if user_text.startswith("/"):
        await state.clear()
        return

    thinking = await message.answer("🧠 Думаю... (GPT)")

    try:
        from src.bot.utils.ai_client import ask_marketing
        from src.bot.utils.rag import find_relevant_context

        # RAG: приоритизированный контекст по запросу + общий контекст
        rag = await find_relevant_context(user_text, google, cache)
        general_ctx = await _build_strategy_context(google, cache)
        context = (rag + "\n\n" + general_ctx) if rag else general_ctx

        # Получаем историю диалога
        data = await state.get_data()
        history = data.get("history", [])

        history_text = ""
        for entry in history[-6:]:  # Последние 6 сообщений
            role = entry.get("role", "")
            text = entry.get("text", "")[:300]
            history_text += f"{role}: {text}\n"

        response = await ask_marketing(
            prompt=user_text,
            context=context,
            history=history_text,
            max_tokens=2048,
            temperature=0.7,
        )

        # Сохраняем историю
        history.append({"role": "Админ", "text": user_text})
        history.append({"role": "AI", "text": response})
        await state.update_data(history=history)

        # Сохраняем диалог в Sheets (async)
        asyncio.create_task(google.log_ai_conversation(
            admin_message=user_text,
            ai_response=response[:500],
        ))

        await thinking.delete()

        if len(response) > 4000:
            response = response[:4000] + "..."

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="📝 Публикация", callback_data="strat_to_publish"),
                    InlineKeyboardButton(text="📢 В канал", callback_data="strat_to_channel"),
                ],
                [InlineKeyboardButton(text="🚪 Завершить чат", callback_data="strat_exit")],
            ]
        )
        try:
            await message.answer(response, reply_markup=kb)
        except Exception:
            await message.answer(response, reply_markup=kb, parse_mode=None)

    except Exception as e:
        logger.error("Strategy chat error: %s", e)
        try:
            await thinking.edit_text(f"❌ Ошибка: {e}")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════
#  БЫСТРЫЕ ДЕЙСТВИЯ ИЗ ЧАТА
# ═══════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "strat_ideas")
async def strategy_ideas(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Быстрая генерация идей."""
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer("Генерирую...")

    try:
        from src.bot.utils.ai_client import ask_marketing

        context = await _build_strategy_context(google, cache)

        response = await ask_marketing(
            prompt=(
                "Предложи 3-5 конкретных идей контента на эту неделю.\n"
                "Для каждой: заголовок, тип (статья/пост/гайд), почему сейчас.\n"
                "Кратко, по делу."
            ),
            context=context,
            max_tokens=1500,
            temperature=0.8,
        )

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📝 Реализовать идею", callback_data="strat_to_publish")],
                [InlineKeyboardButton(text="💡 Ещё", callback_data="strat_ideas")],
            ]
        )
        text = f"💡 <b>Идеи на эту неделю:</b>\n\n{response}"
        try:
            await callback.message.answer(text, reply_markup=kb)
        except Exception:
            await callback.message.answer(text, reply_markup=kb, parse_mode=None)

    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")


@router.callback_query(F.data == "strat_weekly")
async def strategy_weekly(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Анализ за неделю."""
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer("Анализирую...")

    try:
        from src.bot.utils.ai_client import ask_marketing
        from src.database.crud import get_all_user_ids

        user_ids = await get_all_user_ids()
        leads = await google.get_recent_leads(limit=100)
        catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

        context = (
            f"Пользователей бота: {len(user_ids)}\n"
            f"Лидов в CRM: {len(leads)}\n"
            f"Гайдов в каталоге: {len(catalog)}\n"
            f"Последние 10 лидов: {', '.join(l.get('name', '?') + ' (' + l.get('guide', '?') + ')' for l in leads[:10])}"
        )

        response = await ask_marketing(
            prompt=(
                "Сделай краткий анализ за неделю:\n"
                "1. Общая оценка (хорошо/плохо/нормально)\n"
                "2. Что работает (какие гайды популярны)\n"
                "3. Что улучшить\n"
                "4. План на следующую неделю (3 действия)\n"
                "Кратко, с цифрами."
            ),
            context=context,
            max_tokens=1500,
            temperature=0.5,
        )

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💡 Идеи", callback_data="strat_ideas")],
                [InlineKeyboardButton(text="✅ Принято", callback_data="digest_ack")],
            ]
        )
        text = f"📊 <b>Еженедельный анализ:</b>\n\n{response}"
        try:
            await callback.message.answer(text, reply_markup=kb)
        except Exception:
            await callback.message.answer(text, reply_markup=kb, parse_mode=None)

    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")


@router.callback_query(F.data == "strat_to_publish")
async def strat_to_publish(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    await state.clear()
    await callback.message.answer(
        "Отправьте /publish и вставьте текст статьи — AI автоматически оформит."
    )


@router.callback_query(F.data == "strat_to_channel")
async def strat_to_channel(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    await callback.message.answer(
        "Отправьте /admin -> Маркетинг -> Пост в канал."
    )


# ═══════════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ
# ═══════════════════════════════════════════════════════════════════════


async def _build_strategy_context(
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> str:
    """Собирает полный контекст для AI-стратега."""
    parts = []

    try:
        # Дата-рум
        data_room = await google.get_data_room()
        if data_room:
            dr_text = "\n".join(
                f"- [{item.get('category', '')}] {item.get('title', '')}"
                for item in data_room[:15]
            )
            parts.append(f"ДАТА-РУМ КОМПАНИИ:\n{dr_text}")

        # Аналитика
        from src.database.crud import get_all_user_ids
        user_ids = await get_all_user_ids()
        parts.append(f"ПОЛЬЗОВАТЕЛЕЙ: {len(user_ids)}")

        leads = await google.get_recent_leads(limit=20)
        parts.append(f"ЛИДОВ В CRM: {len(leads)}")
        if leads:
            recent = ", ".join(f"{l.get('name', '?')} ({l.get('guide', '?')})" for l in leads[:5])
            parts.append(f"ПОСЛЕДНИЕ ЛИДЫ: {recent}")

        # Каталог гайдов
        catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
        if catalog:
            guides = ", ".join(g.get("title", g.get("id", "?")) for g in catalog)
            parts.append(f"ГАЙДЫ В БОТЕ: {guides}")

        # Последние статьи
        articles = await google.get_articles_list(limit=5)
        if articles:
            art_text = ", ".join(a.get("title", "?") for a in articles)
            parts.append(f"ПОСЛЕДНИЕ СТАТЬИ: {art_text}")

    except Exception as e:
        logger.error("Error building context: %s", e)
        parts.append(f"(ошибка загрузки контекста: {e})")

    return "\n".join(parts)
