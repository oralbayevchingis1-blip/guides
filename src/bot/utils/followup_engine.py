"""Движок персонализированных follow-up сообщений.

Вместо «одинаковые для всех» — оценивает контекст пользователя
и выбирает оптимальный сценарий:

    SKIP      — цель достигнута (консультация / вопрос), не беспокоим
    UPGRADE   — пользователь активен, даём продвинутый контент
    ADAPT     — скачал другой гайд — адаптируем рекомендацию
    WINBACK   — неактивен давно — re-engagement
    EMAIL_ONLY — бот заблокирован, отправляем email
    STANDARD  — обычный follow-up (как было)
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger(__name__)


class FollowupScenario(str, Enum):
    SKIP = "skip"
    UPGRADE = "upgrade"
    ADAPT = "adapt"
    WINBACK = "winback"
    EMAIL_ONLY = "email_only"
    STANDARD = "standard"


@dataclass
class UserFollowupContext:
    """Контекст пользователя на момент отправки follow-up."""
    downloads_since: set[str]        # гайды, скачанные после trigger-гайда
    downloaded_recommended: bool     # скачал ли рекомендованный гайд
    has_consultation: bool           # записался на консультацию
    has_question: bool               # задал вопрос юристу
    total_downloads: int             # всего скачиваний
    days_inactive: int               # дней без активности
    bot_blocked: bool                # бот заблокирован
    has_email: bool                  # есть ли email для fallback


async def evaluate_context(
    user_id: int,
    guide_id: str,
    step: int,
) -> UserFollowupContext:
    """Собирает контекст пользователя для выбора сценария."""
    from src.database.crud import (
        count_user_downloads,
        get_lead_by_user_id,
        get_user_downloaded_guides,
    )
    from src.database.models import async_session, User, Question
    from sqlalchemy import select, func as sa_func

    downloaded = await get_user_downloaded_guides(user_id)
    downloads_since = {g for g in downloaded if g != guide_id}
    total = await count_user_downloads(user_id)
    lead = await get_lead_by_user_id(user_id)
    has_email = bool(lead and lead.email and "@" in lead.email)

    # Проверяем консультацию
    has_consultation = "__consultation__" in downloaded

    # Проверяем вопрос
    has_question = False
    try:
        async with async_session() as session:
            stmt = select(sa_func.count()).select_from(Question).where(
                Question.user_id == user_id,
            )
            result = await session.execute(stmt)
            has_question = (result.scalar_one() or 0) > 0
    except Exception:
        pass

    # Проверяем активность и блокировку
    days_inactive = 0
    bot_blocked = False
    try:
        async with async_session() as session:
            stmt = select(User).where(User.user_id == user_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()
            if user:
                bot_blocked = bool(user.bot_blocked)
                if user.last_activity:
                    delta = datetime.now(timezone.utc) - user.last_activity
                    days_inactive = delta.days
                else:
                    days_inactive = 7
    except Exception:
        pass

    # Проверяем, скачал ли рекомендованный гайд
    from src.bot.utils.smart_recommendations import smart_recommender
    recommended = await smart_recommender.get_recommendation(guide_id, exclude=set())
    downloaded_recommended = recommended in downloads_since if recommended else False

    return UserFollowupContext(
        downloads_since=downloads_since,
        downloaded_recommended=downloaded_recommended,
        has_consultation=has_consultation,
        has_question=has_question,
        total_downloads=total,
        days_inactive=days_inactive,
        bot_blocked=bot_blocked,
        has_email=has_email,
    )


def select_scenario(ctx: UserFollowupContext, step: int) -> FollowupScenario:
    """Выбирает сценарий follow-up на основе контекста.

    Таблица решений:
        consultation? → SKIP
        bot_blocked + email → EMAIL_ONLY
        bot_blocked no email → SKIP
        downloaded_recommended → UPGRADE
        inactive 3+ days (step 1/2) → WINBACK
        new downloads → ADAPT
        default → STANDARD
    """
    # Цель достигнута
    if ctx.has_consultation:
        return FollowupScenario.SKIP

    # Бот заблокирован
    if ctx.bot_blocked:
        return FollowupScenario.EMAIL_ONLY if ctx.has_email else FollowupScenario.SKIP

    # Пользователь активен — скачал рекомендованный гайд
    if ctx.downloaded_recommended:
        return FollowupScenario.UPGRADE

    # Неактивен давно
    if ctx.days_inactive >= 3 and step >= 1:
        return FollowupScenario.WINBACK

    # Скачал другие гайды (но не рекомендованный)
    if ctx.downloads_since and step >= 1:
        return FollowupScenario.ADAPT

    return FollowupScenario.STANDARD


# ── Тексты для сценариев (fallback) ──────────────────────────────────

SCENARIO_FALLBACKS: dict[str, dict[int, str]] = {
    "upgrade": {
        0: (
            "{greeting}вы уже активно используете наши материалы — отлично!\n\n"
            "Для тех, кто глубоко в теме, мы подготовили продвинутый материал:\n\n"
            "{next_guide_block}\n\n"
            "{social_proof_line}"
        ),
        1: (
            "{greeting}вижу, вы серьёзно подходите к юридическим вопросам. "
            "Вот экспертный разбор, который будет вам полезен:\n\n"
            "{case_line}"
            "{next_guide_block}"
        ),
        2: (
            "{greeting}вы один из самых активных пользователей нашей библиотеки!\n\n"
            "Предлагаем персональную консультацию — обсудим именно ваши задачи.\n\n"
            "{consult_scarcity}"
        ),
    },
    "winback": {
        1: (
            "{greeting}давно не виделись! За это время мы подготовили "
            "новый материал специально для вашей сферы.\n\n"
            "{next_guide_block}\n\n"
            "{social_proof_line}"
        ),
        2: (
            "{greeting}последнее напоминание: мы готовы помочь с вашим "
            "юридическим вопросом.\n\n"
            "{consult_scarcity}\n\n"
            "Запишитесь на бесплатную 15-минутную консультацию — "
            "обсудим ваши задачи."
        ),
    },
    "adapt": {
        1: (
            "{greeting}раз вам интересна тема, рекомендуем дополнительно:\n\n"
            "{next_guide_block}\n\n"
            "{case_line}"
        ),
    },
}
