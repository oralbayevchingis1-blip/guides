"""Inline-клавиатуры бота."""

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from src.config import settings


def subscription_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура подписки на канал."""
    channel_name = settings.CHANNEL_USERNAME.lstrip("@")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подписаться на канал",
                    url=f"https://t.me/{channel_name}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔍 Проверить подписку",
                    callback_data="check_subscription",
                )
            ],
        ]
    )


def guides_menu_keyboard(catalog: list[dict]) -> InlineKeyboardMarkup:
    """Клавиатура выбора мини-гайда (динамическая из Google Sheets).

    Args:
        catalog: Список словарей с ключами ``id`` и ``title``
                 (загруженный из листа «Каталог гайдов»).
    """
    buttons = []
    for guide in catalog:
        guide_id = guide.get("id", "???")
        # Telegram лимит callback_data = 64 байта; "guide_" = 6 байт
        cb_data = f"guide_{guide_id}"
        # Обрезаем СТРОГО по байтам (кириллица = 2 байта/символ)
        while len(cb_data.encode("utf-8")) > 64:
            cb_data = cb_data[:-1]
        buttons.append(
            [
                InlineKeyboardButton(
                    text=guide.get("title", guide_id),
                    callback_data=cb_data,
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def consent_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура согласия на обработку персональных данных."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Даю согласие",
                    callback_data="give_consent",
                ),
                InlineKeyboardButton(
                    text="❌ Отказаться",
                    callback_data="decline_consent",
                ),
            ]
        ]
    )


def after_guide_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура после выдачи гайда и завершения воронки.

    Позволяет пользователю:
    - Посмотреть другие гайды
    - Задать вопрос AI-юристу
    - Поделиться ботом (реферал)
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📚 Посмотреть другие гайды",
                    callback_data="show_all_guides",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🤖 Задать вопрос юристу (AI)",
                    callback_data="start_consult",
                ),
            ],
        ]
    )
