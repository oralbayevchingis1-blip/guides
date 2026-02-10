"""AI Lead Scoring — автоматическая оценка потенциала лидов.

Анализирует историю вопросов пользователя из Consult Log.
Если пользователь спрашивает про МФЦА, M&A или крупные контракты —
помечает лид как HOT и уведомляет админа.

Использование:
    from src.bot.utils.lead_scoring import analyze_and_score_lead
    await analyze_and_score_lead(user_id, google, cache, bot)
"""

import asyncio
import json
import logging
import re

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.config import settings

logger = logging.getLogger(__name__)

# Ключевые слова для быстрого скоринга (без AI)
HOT_KEYWORDS = {
    "мфца", "aifc", "m&a", "слияние", "поглощение", "ipo",
    "миллион", "миллиард", "крупн", "инвестиц", "фонд",
    "листинг", "акции", "облигац", "esop", "опцион",
}

WARM_KEYWORDS = {
    "тоо", "регистрац", "налог", "лицензи", "контракт",
    "договор", "спор", "суд", "арбитраж", "банкрот",
    "трудов", "увольнен", "штраф", "проверк",
}


async def analyze_and_score_lead(
    user_id: int,
    google: GoogleSheetsClient,
    cache: TTLCache,
    bot: Bot,
) -> dict:
    """Анализирует историю вопросов пользователя и оценивает потенциал.

    Returns:
        {"score": int, "label": str, "reason": str}
    """
    try:
        # 1. Получаем историю консультаций пользователя
        consult_log = await google.get_consult_log(limit=200)
        user_questions = [
            row.get("question", row.get("Вопрос", ""))
            for row in consult_log
            if str(row.get("user_id", row.get("User ID", ""))) == str(user_id)
        ]

        if not user_questions:
            return {"score": 0, "label": "Cold", "reason": "Нет вопросов"}

        all_text = " ".join(user_questions).lower()

        # 2. Быстрая эвристика по ключевым словам
        hot_matches = [kw for kw in HOT_KEYWORDS if kw in all_text]
        warm_matches = [kw for kw in WARM_KEYWORDS if kw in all_text]

        # 3. AI-скоринг для глубокого анализа
        score = 0
        label = "Cold"
        reason = ""

        if hot_matches:
            # Быстрый HOT без AI
            score = 80 + min(len(hot_matches) * 5, 20)  # 80-100
            label = "HOT"
            reason = f"Ключевые темы: {', '.join(hot_matches[:5])}"
        elif warm_matches:
            score = 40 + min(len(warm_matches) * 10, 40)  # 40-80
            label = "Warm"
            reason = f"Темы: {', '.join(warm_matches[:5])}"
        else:
            score = max(10, len(user_questions) * 5)  # 10-50 за активность
            label = "Warm" if score >= 30 else "Cold"
            reason = f"Активность: {len(user_questions)} вопросов"

        # 4. AI-дополнение для HOT/Warm лидов (если доступен)
        if score >= 40 and len(user_questions) >= 2:
            try:
                from src.bot.utils.ai_client import ask_marketing

                questions_text = "\n".join(f"- {q[:150]}" for q in user_questions[-10:])
                ai_analysis = await ask_marketing(
                    prompt=(
                        f"Проанализируй вопросы пользователя и оцени его потенциал как клиента "
                        f"юридической фирмы (0-100).\n\n"
                        f"Вопросы пользователя:\n{questions_text}\n\n"
                        f"Ответь СТРОГО в формате JSON:\n"
                        f'{{"score": число, "label": "HOT/Warm/Cold", "reason": "причина"}}'
                    ),
                    max_tokens=256,
                    temperature=0.2,
                )

                json_match = re.search(r'\{.*\}', ai_analysis, re.DOTALL)
                if json_match:
                    ai_data = json.loads(json_match.group())
                    ai_score = int(ai_data.get("score", 0))
                    # Берём максимум из эвристики и AI
                    if ai_score > score:
                        score = ai_score
                        label = ai_data.get("label", label)
                        reason = ai_data.get("reason", reason)
            except Exception as e:
                logger.warning("AI lead scoring failed, using heuristics: %s", e)

        # 5. Записываем скоринг в Sheets
        await google.update_lead_score(user_id, score, label)

        # 6. Уведомляем админа о HOT лидах
        if label == "HOT":
            leads = await google.get_recent_leads(limit=100)
            user_lead = None
            for lead in reversed(leads):
                if str(lead.get("user_id", "")) == str(user_id):
                    user_lead = lead
                    break

            name = user_lead.get("name", "—") if user_lead else "—"
            username = user_lead.get("username", "") if user_lead else ""

            msg = (
                f"🔥🔥🔥 *СРОЧНАЯ СДЕЛКА!*\n\n"
                f"Пользователь *{name}* (score: {score}/100)\n"
                f"Причина: {reason}\n"
                f"Вопросов задано: {len(user_questions)}\n\n"
                f"Последний вопрос:\n_{user_questions[-1][:200]}_"
            )

            buttons = []
            if username:
                buttons.append([InlineKeyboardButton(
                    text=f"💬 Написать @{username}",
                    url=f"https://t.me/{username}",
                )])
            buttons.append([InlineKeyboardButton(
                text="📊 Открыть CRM",
                url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
            )])

            try:
                await bot.send_message(
                    chat_id=settings.ADMIN_ID,
                    text=msg,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                )
            except Exception:
                await bot.send_message(
                    chat_id=settings.ADMIN_ID,
                    text=msg,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                    parse_mode=None,
                )

        result = {"score": score, "label": label, "reason": reason}
        logger.info("Lead scored: user_id=%s -> %s", user_id, result)
        return result

    except Exception as e:
        logger.error("Lead scoring error for user_id=%s: %s", user_id, e)
        return {"score": 0, "label": "Error", "reason": str(e)}
