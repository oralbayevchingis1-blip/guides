"""Auto-FAQ Discovery — автоматическое обнаружение часто задаваемых вопросов.

Раз в сутки AI анализирует все вопросы из Consult Log.
Если 5+ пользователей спросили одно и то же — формулирует идеальный ответ
и добавляет его в Data Room (категория «Черновики FAQ») для утверждения.

Использование:
    from src.bot.utils.auto_faq import run_auto_faq_discovery
    await run_auto_faq_discovery(google=google, cache=cache, bot=bot)
"""

import json
import logging
import re
from collections import Counter

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.config import settings

logger = logging.getLogger(__name__)

# Минимум пользователей с похожим вопросом для создания FAQ
MIN_SIMILAR_USERS = 5


async def run_auto_faq_discovery(
    *,
    google: GoogleSheetsClient,
    cache: TTLCache,
    bot: Bot,
) -> int:
    """Анализирует Consult Log и создаёт черновики FAQ.

    Returns:
        Количество созданных FAQ-черновиков.
    """
    try:
        # 1. Загружаем все вопросы
        consult_log = await google.get_consult_log(limit=500)
        if len(consult_log) < MIN_SIMILAR_USERS:
            logger.info("Auto-FAQ: недостаточно вопросов (%d)", len(consult_log))
            return 0

        # Собираем уникальные вопросы по пользователям
        questions_by_user: dict[str, list[str]] = {}
        all_questions: list[str] = []

        for row in consult_log:
            uid = str(row.get("user_id", row.get("User ID", "")))
            q = row.get("question", row.get("Вопрос", "")).strip()
            if uid and q and len(q) > 10:
                questions_by_user.setdefault(uid, []).append(q)
                all_questions.append(q)

        if len(all_questions) < MIN_SIMILAR_USERS:
            return 0

        # 2. AI кластеризация вопросов
        from src.bot.utils.ai_client import ask_marketing

        questions_text = "\n".join(f"- {q[:200]}" for q in all_questions[-100:])

        cluster_response = await ask_marketing(
            prompt=(
                "Проанализируй следующие юридические вопросы пользователей "
                "и сгруппируй их по темам.\n\n"
                "Для каждой группы из 5+ похожих вопросов верни JSON:\n"
                '[{"topic": "Тема", "count": число_похожих, '
                '"sample_questions": ["вопрос1", "вопрос2"], '
                '"ideal_answer": "Идеальный краткий ответ от юриста SOLIS Partners"}]\n\n'
                "ПРАВИЛА:\n"
                "- Только группы с 5+ похожими вопросами\n"
                "- ideal_answer: структурированный, со ссылками на статьи законов РК\n"
                "- Добавь дисклеймер о необходимости полной консультации\n"
                "- Максимум 5 FAQ-групп\n"
                "- Верни ТОЛЬКО JSON\n\n"
                f"ВОПРОСЫ:\n{questions_text}"
            ),
            max_tokens=2048,
            temperature=0.3,
        )

        # 3. Парсим результат
        json_match = re.search(r'\[.*\]', cluster_response, re.DOTALL)
        if not json_match:
            logger.info("Auto-FAQ: no clusters found")
            return 0

        try:
            clusters = json.loads(json_match.group())
        except json.JSONDecodeError:
            logger.warning("Auto-FAQ: invalid JSON from AI")
            return 0

        if not clusters or not isinstance(clusters, list):
            return 0

        # 4. Загружаем существующие FAQ для дедупликации
        existing_data_room = await google.get_data_room()
        existing_titles = {
            item.get("title", item.get("Заголовок", "")).lower()
            for item in existing_data_room
        }

        created = 0
        for cluster in clusters[:5]:
            topic = cluster.get("topic", "")
            count = cluster.get("count", 0)
            answer = cluster.get("ideal_answer", "")

            if count < MIN_SIMILAR_USERS or not topic or not answer:
                continue

            # Дедупликация
            if topic.lower() in existing_titles:
                continue

            # 5. Добавляем в Data Room как черновик
            await google.append_data_room(
                category="Черновики FAQ",
                title=f"[FAQ] {topic}",
                content=answer[:2000],
            )
            created += 1

            logger.info("Auto-FAQ created: %s (count=%d)", topic, count)

        # 6. Уведомляем админа
        if created > 0:
            msg = (
                f"🧠 *Auto-FAQ: обнаружено {created} новых FAQ*\n\n"
                "Черновики добавлены в Data Room (категория «Черновики FAQ»).\n"
                "Проверьте и утвердите их в Google Sheets."
            )

            faq_list = "\n".join(
                f"• {c.get('topic', '?')} ({c.get('count', '?')} вопросов)"
                for c in clusters[:created]
            )
            msg += f"\n\n{faq_list}"

            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(
                        text="📊 Открыть Data Room",
                        url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
                    )],
                    [InlineKeyboardButton(
                        text="✅ Принято",
                        callback_data="digest_ack",
                    )],
                ]
            )

            try:
                await bot.send_message(
                    chat_id=settings.ADMIN_ID,
                    text=msg,
                    reply_markup=keyboard,
                )
            except Exception:
                await bot.send_message(
                    chat_id=settings.ADMIN_ID,
                    text=msg,
                    reply_markup=keyboard,
                    parse_mode=None,
                )

        logger.info("Auto-FAQ discovery complete: %d FAQs created", created)
        return created

    except Exception as e:
        logger.error("Auto-FAQ error: %s", e)
        return 0
