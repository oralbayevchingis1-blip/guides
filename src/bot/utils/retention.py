"""Retention Loop — возвращение «спящих» пользователей.

Если пользователь не заходил 14+ дней:
1. AI анализирует его прошлые интересы (скачанные гайды, вопросы)
2. Генерирует персонализированное сообщение
3. Отправляет с кнопками для повторного вовлечения

Использование:
    from src.bot.utils.retention import check_sleeping_users
    # Запускается из scheduler ежедневно
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from src.config import settings

logger = logging.getLogger(__name__)

# Минимум дней неактивности
SLEEP_THRESHOLD_DAYS = 14

# Cooldown между re-engagement (не чаще 1 раза в 30 дней)
_last_reengaged: dict[int, float] = {}
REENGAGE_COOLDOWN_DAYS = 30


async def check_sleeping_users(
    bot: Bot,
    google=None,
    cache=None,
) -> dict:
    """Находит спящих пользователей и отправляет им персонализированные сообщения.

    Returns:
        {"checked": N, "sleeping": N, "reengaged": N, "skipped": N}
    """
    from src.database.models import async_session, User
    from sqlalchemy import select

    now = datetime.now(timezone.utc)
    threshold = now - timedelta(days=SLEEP_THRESHOLD_DAYS)
    cooldown_ts = now - timedelta(days=REENGAGE_COOLDOWN_DAYS)

    stats = {"checked": 0, "sleeping": 0, "reengaged": 0, "skipped": 0}

    try:
        async with async_session() as session:
            # Пользователи, которые не заходили > SLEEP_THRESHOLD_DAYS
            stmt = select(User).where(
                User.last_activity < threshold,
                User.last_activity.isnot(None),
            )
            result = await session.execute(stmt)
            sleeping_users = list(result.scalars().all())

        stats["checked"] = len(sleeping_users)
        stats["sleeping"] = len(sleeping_users)

        for user in sleeping_users[:20]:  # Лимит 20 за цикл
            uid = user.user_id

            # Проверяем cooldown
            last = _last_reengaged.get(uid, 0)
            if last > cooldown_ts.timestamp():
                stats["skipped"] += 1
                continue

            # Генерируем сообщение
            text = await _generate_reengage_message(uid, user.full_name or "", google, cache)
            if not text:
                stats["skipped"] += 1
                continue

            # Отправляем
            try:
                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(
                            text="🤖 Задать вопрос AI-юристу",
                            callback_data="start_consult",
                        )],
                        [InlineKeyboardButton(
                            text="📚 Новые гайды",
                            callback_data="show_all_guides",
                        )],
                        [InlineKeyboardButton(
                            text="🔕 Не напоминать",
                            callback_data="retention_optout",
                        )],
                    ]
                )
                await bot.send_message(chat_id=uid, text=text, reply_markup=keyboard)
                _last_reengaged[uid] = now.timestamp()
                stats["reengaged"] += 1
            except Exception as e:
                logger.debug("Retention send failed for %s: %s", uid, e)
                stats["skipped"] += 1

            await asyncio.sleep(0.1)  # Rate limit

    except Exception as e:
        logger.error("Retention check error: %s", e)

    logger.info("Retention check: %s", stats)
    return stats


async def _generate_reengage_message(
    user_id: int,
    name: str,
    google=None,
    cache=None,
) -> str | None:
    """AI генерирует персонализированное сообщение для спящего пользователя."""

    # Получаем интересы пользователя
    interests = []
    if google:
        try:
            leads = await google.get_recent_leads(limit=200)
            for lead in leads:
                if str(lead.get("user_id", "")) == str(user_id):
                    guide = lead.get("guide", lead.get("selected_guide", ""))
                    if guide:
                        interests.append(guide)
        except Exception:
            pass

    greeting = f"👋 {name}! " if name else "👋 "

    if interests:
        # Персонализированное сообщение на основе интересов
        guides_text = ", ".join(set(interests))
        try:
            from src.bot.utils.ai_client import ask_marketing

            result = await ask_marketing(
                prompt=(
                    f"Пользователь {name} скачивал гайды: {guides_text}.\n"
                    "Он не заходил в бот 2 недели.\n"
                    "Напиши КОРОТКОЕ (2-3 предложения) дружеское сообщение:\n"
                    "1. Упомяни, что появились обновления по его теме\n"
                    "2. Задай вопрос, связанный с его интересами\n"
                    "3. Предложи посмотреть новые материалы\n"
                    "Формат: чистый текст без HTML-тегов."
                ),
                max_tokens=200,
                temperature=0.8,
            )
            return f"{greeting}{result.strip()}"
        except Exception as e:
            logger.warning("Retention AI failed: %s", e)

    # Fallback без AI
    return (
        f"{greeting}Давно не виделись!\n\n"
        "У нас появились новые материалы по юридическим вопросам "
        "для бизнеса в Казахстане.\n\n"
        "Посмотрите, что нового? 👇"
    )
