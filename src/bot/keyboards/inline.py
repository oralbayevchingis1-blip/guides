"""Inline-клавиатуры бота — дизайн для мобильных.

Принципы UX:
- Одна кнопка на строку для важных действий (удобно нажимать большим пальцем)
- Эмодзи в начале каждой кнопки для быстрого визуального сканирования
- Максимум 6 кнопок на экран
"""

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from src.config import settings


def subscription_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура подписки на канал."""
    channel_name = settings.CHANNEL_USERNAME.lstrip("@")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📢 Подписаться на канал",
                    url=f"https://t.me/{channel_name}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✅ Проверить подписку",
                    callback_data="check_subscription",
                )
            ],
        ]
    )


def guides_menu_keyboard(catalog: list[dict]) -> InlineKeyboardMarkup:
    """Клавиатура выбора мини-гайда (динамическая из Google Sheets).

    Каждый гайд — отдельная строка с тематическим эмодзи.
    """
    # Маппинг категорий к эмодзи
    _GUIDE_EMOJI = {
        "too": "📑", "ip": "🚀", "mfca": "🌍", "aifc": "🌍",
        "esop": "💰", "tax": "💰", "labor": "⚖️", "it": "💡",
        "ma": "💰", "m&a": "💰",
    }

    buttons = []
    for guide in catalog:
        guide_id = guide.get("id", "???")
        title = guide.get("title", guide_id)

        # Выбираем эмодзи по ID гайда
        emoji = "📚"
        gid_lower = guide_id.lower()
        for key, em in _GUIDE_EMOJI.items():
            if key in gid_lower:
                emoji = em
                break

        # Telegram лимит callback_data = 64 байта
        cb_data = f"guide_{guide_id}"
        while len(cb_data.encode("utf-8")) > 64:
            cb_data = cb_data[:-1]

        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"{emoji} {title}",
                    callback_data=cb_data,
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def consent_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура согласия — две кнопки на отдельных строках."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Даю согласие",
                    callback_data="give_consent",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отказаться",
                    callback_data="decline_consent",
                ),
            ],
        ]
    )


def after_guide_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура после выдачи гайда — одна кнопка на строку."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🤖 Задать вопрос AI-юристу",
                    callback_data="start_consult",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📚 Посмотреть другие гайды",
                    callback_data="show_all_guides",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔗 Поделиться ботом",
                    callback_data="referral_share",
                ),
            ],
        ]
    )
