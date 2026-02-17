"""Прогрессивное профилирование 2.0 — постепенный сбор данных о пользователе.

Каждый вопрос задаётся на отдельном этапе скачивания:
    2-е скачивание → сфера бизнеса
    3-е скачивание → размер команды
    4-е скачивание → стадия бизнеса

Принцип: один вопрос за визит, с возможностью пропустить.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)


@dataclass
class ProfileQuestion:
    """Описание одного вопроса профилирования."""
    field: str               # имя колонки в User
    min_downloads: int       # порог скачиваний
    prompt: str              # текст вопроса для пользователя
    options: list[tuple[str, str]]  # [(label, value), ...]
    skip_label: str = "⏭ Пропустить"
    required: bool = False   # если True — кнопка «Пропустить» не показывается


PROFILE_QUESTIONS: list[ProfileQuestion] = [
    ProfileQuestion(
        field="business_sphere",
        min_downloads=2,
        prompt=(
            "Чтобы подбирать материалы точнее под ваш бизнес, "
            "подскажите: <b>в какой сфере вы работаете?</b>\n\n"
            "Выберите из списка или напишите свой вариант:"
        ),
        options=[
            ("💻 IT / Технологии", "IT"),
            ("🏗 Строительство", "Строительство"),
            ("🛒 Ритейл / Торговля", "Ритейл"),
            ("💰 Инвестиции / Финансы", "Инвестиции"),
            ("🏭 Производство", "Производство"),
            ("🏥 Медицина", "Медицина"),
            ("📊 Консалтинг", "Консалтинг"),
        ],
        required=True,
    ),
    ProfileQuestion(
        field="company_size",
        min_downloads=3,
        prompt=(
            "Спасибо! Ещё один вопрос: <b>сколько человек в вашей команде?</b>\n\n"
            "Это поможет подобрать наиболее актуальные материалы."
        ),
        options=[
            ("👤 1–5 человек", "1-5"),
            ("👥 6–20 человек", "6-20"),
            ("🏢 21–50 человек", "21-50"),
            ("🏗 51–200 человек", "51-200"),
            ("🏙 200+ человек", "200+"),
        ],
    ),
    ProfileQuestion(
        field="company_stage",
        min_downloads=4,
        prompt=(
            "И последнее: <b>на какой стадии ваш бизнес?</b>\n\n"
            "Так мы подберём материалы под вашу ситуацию."
        ),
        options=[
            ("💡 Идея / планирование", "idea"),
            ("🚀 Стартап (< 2 лет)", "startup"),
            ("📈 Активный рост", "growth"),
            ("🏛 Зрелый бизнес", "mature"),
            ("🌍 Масштабирование", "scaling"),
        ],
    ),
]


async def get_next_question(
    user_id: int,
    download_count: int,
) -> Optional[ProfileQuestion]:
    """Определяет следующий незаполненный профильный вопрос.

    Args:
        user_id: Telegram user_id.
        download_count: Количество скачанных гайдов.

    Returns:
        ProfileQuestion или None, если все вопросы заданы.
    """
    from src.database.crud import get_user_profile

    profile = await get_user_profile(user_id)
    if not profile:
        return None

    for q in PROFILE_QUESTIONS:
        if download_count >= q.min_downloads and not profile.get(q.field):
            return q

    return None


def build_question_keyboard(question: ProfileQuestion) -> InlineKeyboardMarkup:
    """Строит inline-клавиатуру для профильного вопроса."""
    rows: list[list[InlineKeyboardButton]] = []

    for i in range(0, len(question.options), 2):
        row = []
        for label, value in question.options[i:i + 2]:
            row.append(InlineKeyboardButton(
                text=label,
                callback_data=f"profile_{question.field}_{value}",
            ))
        rows.append(row)

    if not question.required:
        rows.append([InlineKeyboardButton(
            text=question.skip_label,
            callback_data=f"profile_{question.field}_skip",
        )])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_lead_score(
    download_count: int,
    profile_fields_filled: int,
    has_consultation: bool = False,
) -> str:
    """Вычисляет warmth с учётом профиля.

    Returns:
        "Cold", "Warm", "Hot", или "Hot+" (полный профиль).
    """
    if has_consultation:
        return "Hot+"

    if download_count >= 3 and profile_fields_filled >= 3:
        return "Hot+"
    if download_count >= 3:
        return "Hot"
    if download_count >= 1:
        return "Warm"
    return "Cold"
