"""Клавиатуры бота — inline и reply."""

import re

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

from src.config import settings


_CAT_EMOJI = {
    "инвестиции": "💰", "investment": "💰",
    "налоги": "💰", "tax": "💰",
    "ip": "🚀", "интеллектуальная": "🚀",
    "труд": "⚖️", "labor": "⚖️", "hr": "⚖️",
    "it": "💡", "ит": "💡", "технолог": "💡",
    "мфца": "🌍", "aifc": "🌍", "mfca": "🌍",
    "m&a": "📊", "слиян": "📊",
    "корпоратив": "📑", "corporate": "📑",
    "ai": "🧠", "ии": "🧠",
}


def _slugify_cat(text: str) -> str:
    """Короткий slug для callback data категории."""
    t = text.lower().strip()
    t = re.sub(r"[^\w\s-]", "", t)
    t = re.sub(r"[\s-]+", "_", t).strip("_")
    return t[:30]


def _cat_emoji(category: str) -> str:
    """Подбирает эмодзи по названию категории."""
    low = category.lower()
    for key, em in _CAT_EMOJI.items():
        if key in low:
            return em
    return "📚"


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Постоянное меню внизу экрана."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📚 Гайды"), KeyboardButton(text="📂 Мои гайды")],
            [KeyboardButton(text="📞 Консультация"), KeyboardButton(text="❓ Задать вопрос")],
            [KeyboardButton(text="📩 Подписки")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


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
                    text="✅ Я подписался",
                    callback_data="check_subscription",
                )
            ],
        ]
    )


def categories_keyboard(catalog: list[dict]) -> InlineKeyboardMarkup:
    """Клавиатура выбора категории (первый уровень навигации)."""
    seen: dict[str, str] = {}  # category_name -> slug
    for guide in catalog:
        cat = guide.get("category", "").strip()
        if cat and cat not in seen:
            seen[cat] = _slugify_cat(cat)

    buttons = []
    for cat_name, slug in seen.items():
        emoji = _cat_emoji(cat_name)
        cb = f"cat_{slug}"
        while len(cb.encode("utf-8")) > 64:
            cb = cb[:-1]
        buttons.append([InlineKeyboardButton(text=f"{emoji} {cat_name}", callback_data=cb)])

    buttons.append([InlineKeyboardButton(text="🔹 Все гайды", callback_data="show_all_guides")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


PAGE_SIZE = 3


def _guide_button(guide: dict) -> InlineKeyboardButton:
    """Создаёт кнопку для одного гайда."""
    guide_id = guide.get("id", "???")
    title = guide.get("title", guide_id)
    cb_data = f"guide_{guide_id}"
    while len(cb_data.encode("utf-8")) > 64:
        cb_data = cb_data[:-1]
    return InlineKeyboardButton(text=f"🔹 {title}", callback_data=cb_data)


def paginated_guides_keyboard(
    catalog: list[dict],
    page: int = 0,
    *,
    page_size: int = PAGE_SIZE,
    back_cb: str = "show_categories",
    back_text: str = "⬅️ Назад к темам",
    prefix: str = "gpage",
) -> InlineKeyboardMarkup:
    """Клавиатура выбора гайда с пагинацией.

    prefix — определяет callback data навигации:
      'gpage' → gpage_0, gpage_1 ...   (все гайды)
      'cpage_<slug>' → cpage_<slug>_0   (гайды категории)
    """
    total = len(catalog)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))

    start = page * page_size
    end = start + page_size
    page_items = catalog[start:end]

    buttons: list[list[InlineKeyboardButton]] = []
    for guide in page_items:
        buttons.append([_guide_button(guide)])

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(
            text="◀️ Назад",
            callback_data=f"{prefix}_{page - 1}",
        ))
    if total_pages > 1:
        nav_row.append(InlineKeyboardButton(
            text=f"{page + 1}/{total_pages}",
            callback_data="noop",
        ))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(
            text="Ещё ▶️",
            callback_data=f"{prefix}_{page + 1}",
        ))
    if nav_row:
        buttons.append(nav_row)

    buttons.append([InlineKeyboardButton(text=back_text, callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def guides_menu_keyboard(catalog: list[dict]) -> InlineKeyboardMarkup:
    """Клавиатура выбора гайда (полный список — первая страница с пагинацией)."""
    return paginated_guides_keyboard(catalog, page=0)


def consent_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура согласия на обработку данных."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Согласен", callback_data="give_consent")],
            [InlineKeyboardButton(text="Нет, не хочу получать письма", callback_data="decline_consent")],
        ]
    )


def after_guide_keyboard(user_id: int | None = None) -> InlineKeyboardMarkup:
    """Клавиатура после выдачи гайда — другие гайды + консультация + вопрос + поделиться."""
    rows = [
        [InlineKeyboardButton(text="🔹 Другие гайды", callback_data="show_categories")],
        [InlineKeyboardButton(text="🔹 Бесплатная консультация", callback_data="book_consultation")],
        [InlineKeyboardButton(text="🔹 Задать вопрос юристу", callback_data="ask_question")],
    ]
    if user_id:
        rows.append([InlineKeyboardButton(
            text="🔹 Отправить другу",
            callback_data=f"share_bot_{user_id}",
        )])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def library_keyboard(downloaded_guides: list[dict]) -> InlineKeyboardMarkup:
    """Клавиатура 'Моя библиотека' — кнопки повторного скачивания."""
    buttons = []
    for g in downloaded_guides:
        guide_id = g.get("id", "")
        title = g.get("title", guide_id)
        cb_data = f"guide_{guide_id}"
        while len(cb_data.encode("utf-8")) > 64:
            cb_data = cb_data[:-1]
        buttons.append([InlineKeyboardButton(text=f"🔹 {title}", callback_data=cb_data)])
    buttons.append([InlineKeyboardButton(text="🔹 Все темы", callback_data="show_categories")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)
