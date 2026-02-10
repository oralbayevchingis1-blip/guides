"""Админ-панель — центральный хаб управления ботом, сайтом и маркетингом.

Двухуровневое меню:
    /admin → Главная панель
        ├── 📝 Контент        → статьи, списки, управление
        ├── 📚 Гайды          → загрузка, каталог, удаление
        ├── 📢 Маркетинг      → пост в канал, контент-пайплайн
        ├── 🧠 AI Ассистент   → чат, идеи, дайджест, Auto-FAQ
        ├── 📊 Аналитика      → лиды, статистика, источники
        └── ⚙️ Настройки      → синхронизация, кеш, Data Room
"""

import asyncio
import json as _json
import logging
import os
import re
import subprocess
import tempfile
import unicodedata

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

# Путь к sync_articles.py
SYNC_SCRIPT = os.path.join(os.path.dirname(__file__), "..", "..", "..", "sync_articles.py")

# Папка для локального хранения гайдов
GUIDES_DIR = os.path.join("data", "guides")

CATEGORIES = [
    ("News", "Новости"),
    ("Analytics", "Аналитика"),
    ("Guide", "Гайд для бизнеса"),
    ("Legal Opinion", "Мнение Партнера"),
    ("Media", "СМИ о нас"),
    ("Interview", "Интервью"),
]


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id == settings.ADMIN_ID


def _slugify(text: str) -> str:
    """Генерирует URL-совместимый ID из текста."""
    translit = {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e",
        "ё": "yo", "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k",
        "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
        "с": "s", "т": "t", "у": "u", "ф": "f", "х": "h", "ц": "ts",
        "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "",
        "э": "e", "ю": "yu", "я": "ya",
    }
    result = ""
    for char in text.lower():
        result += translit.get(char, char)
    result = re.sub(r"[^\w\s-]", "", result)
    result = re.sub(r"[\s-]+", "-", result).strip("-")
    return result[:50]


# ═══════════════════════════════════════════════════════════════════════
#  ГЛАВНОЕ МЕНЮ (уровень 1)
# ═══════════════════════════════════════════════════════════════════════


def _main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Контент", callback_data="adm_content"),
             InlineKeyboardButton(text="📚 Гайды", callback_data="adm_guides")],
            [InlineKeyboardButton(text="📢 Маркетинг", callback_data="adm_marketing"),
             InlineKeyboardButton(text="🧠 AI Ассистент", callback_data="adm_ai")],
            [InlineKeyboardButton(text="📊 Аналитика", callback_data="adm_analytics"),
             InlineKeyboardButton(text="⚙️ Настройки", callback_data="adm_settings")],
        ]
    )


@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    await state.clear()
    await message.answer(
        "🏠 <b>Панель управления SOLIS Bot</b>\n\n"
        "Выберите раздел:",
        reply_markup=_main_menu_keyboard(),
    )


@router.callback_query(F.data == "adm_home")
async def go_home(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.edit_text(
        "🏠 <b>Панель управления SOLIS Bot</b>\n\nВыберите раздел:",
        reply_markup=_main_menu_keyboard(),
    )
    await callback.answer()


# Обратная совместимость со старыми callback
@router.message(Command("admin_panel"))
async def cmd_admin_panel_compat(message: Message, state: FSMContext) -> None:
    await cmd_admin(message, state)


@router.callback_query(F.data == "cm_back_menu")
async def back_to_menu_compat(callback: CallbackQuery, state: FSMContext) -> None:
    await go_home(callback, state)


# ═══════════════════════════════════════════════════════════════════════
#  📝 КОНТЕНТ (уровень 2)
# ═══════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "adm_content")
async def menu_content(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(
        "📝 <b>Управление контентом</b>\n\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📝 Опубликовать статью", callback_data="cm_publish")],
                [InlineKeyboardButton(text="📋 Список статей", callback_data="adm_articles_list")],
                [InlineKeyboardButton(text="🔄 Синхронизировать сайт", callback_data="cm_sync")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_home")],
            ]
        ),
    )
    await callback.answer()


# ── Список статей ──


@router.callback_query(F.data == "adm_articles_list")
async def articles_list(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()

    articles = await google.get_articles_list(limit=15)
    if not articles:
        await callback.message.edit_text(
            "📋 Статей пока нет.\n\nОпубликуйте первую через «Опубликовать статью».",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📝 Опубликовать", callback_data="cm_publish")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_content")],
                ]
            ),
        )
        return

    text = "📋 <b>Статьи на сайте:</b>\n\n"
    buttons = []
    for art in reversed(articles[-10:]):
        title = art.get("title", art.get("id", "?"))[:40]
        active = str(art.get("active", "TRUE")).upper() == "TRUE"
        art_id = art.get("id", art.get("article_id", ""))
        status = "✅" if active else "❌"
        text += f"{status} {title}\n"

        cb_data = f"adm_art_toggle_{art_id}"
        if len(cb_data.encode("utf-8")) > 64:
            cb_data = cb_data[:64]
        buttons.append([InlineKeyboardButton(
            text=f"{'🔴 Скрыть' if active else '🟢 Показать'} {title[:25]}",
            callback_data=cb_data,
        )])

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_content")])

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("adm_art_toggle_"))
async def toggle_article_handler(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    if not _is_admin(callback.from_user.id):
        return
    article_id = callback.data.removeprefix("adm_art_toggle_")
    new_state = await google.toggle_article(article_id)
    status_text = "активна" if new_state else "скрыта"
    await callback.answer(f"Статья {status_text}")
    # Обновляем список
    await articles_list(callback, google)


# ═══════════════════════════════════════════════════════════════════════
#  📝 ПУБЛИКАЦИЯ СТАТЬИ (AI-powered)
# ═══════════════════════════════════════════════════════════════════════

ARTICLE_AI_PROMPT = """Ты — редактор контента юридической фирмы SOLIS Partners.
Тебе дают сырой текст статьи. Твоя задача — вернуть СТРОГО JSON (без markdown-обёрток, без ```json```) со следующими полями:

{
  "title": "Заголовок статьи (извлеки из текста или придумай точный)",
  "category": "ОДНА из: News, Analytics, Guide, Legal Opinion, Media, Interview",
  "categoryRu": "Русское название категории: Новости, Аналитика, Гайд для бизнеса, Мнение Партнера, СМИ о нас, Интервью",
  "description": "Краткое описание для превью (1-2 предложения, до 200 символов)",
  "content": "Полный текст статьи в HTML-разметке"
}

Правила для content (HTML):
- Заголовки: <h2> для главного, <h3> для подзаголовков
- Абзацы: <p>текст</p>
- Списки: <ul><li>пункт</li></ul> или <ol><li>пункт</li></ol>
- Жирный: <strong>текст</strong>
- Курсив: <em>текст</em>
- Цитаты/важное: <blockquote>текст</blockquote>
- Ссылки на законы: <strong>Статья N Закона РК...</strong>
- НЕ используй <h1> (это заголовок страницы)
- НЕ добавляй заголовок статьи в content (он уже в title)
- Сохрани ВСЕ смысловое содержание оригинала — ничего не удаляй и не сокращай
- Структурируй логически: разбей на разделы с подзаголовками если текст длинный
- Пиши на том же языке, что и оригинал

Правила для category:
- News — новости, события, изменения в законодательстве
- Analytics — аналитические обзоры, разборы, исследования
- Guide — практические руководства, пошаговые инструкции, чек-листы
- Legal Opinion — экспертное мнение, комментарий юриста
- Media — упоминания в СМИ, публикации на других площадках
- Interview — интервью с экспертами

ВАЖНО: верни ТОЛЬКО валидный JSON, без каких-либо обёрток или пояснений."""


class ArticleForm(StatesGroup):
    waiting_text = State()
    collecting_text = State()  # Сбор частей (Telegram режет длинные сообщения)
    confirm = State()
    editing_field = State()  # Для редактирования полей


@router.callback_query(F.data == "cm_publish")
async def start_publish(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await state.clear()
    await state.set_state(ArticleForm.waiting_text)
    await callback.message.edit_text(
        "📝 <b>Публикация статьи на сайт</b>\n\n"
        "Просто скиньте текст статьи целиком.\n"
        "AI автоматически:\n"
        "• Определит заголовок\n"
        "• Подберёт категорию\n"
        "• Напишет описание для превью\n"
        "• Сделает HTML-разметку\n\n"
        "Также можно отправить <b>ссылку</b> (URL) — она будет опубликована как внешняя статья.",
    )
    await callback.answer()


@router.message(Command("publish"))
async def cmd_publish(message: Message, state: FSMContext) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    await state.clear()
    await state.set_state(ArticleForm.waiting_text)
    await message.answer(
        "📝 <b>Публикация статьи</b>\n\n"
        "Скиньте текст статьи — AI сам всё разметит.\n"
        "Или отправьте ссылку (URL) для внешней статьи.",
    )


def _article_preview_keyboard(data: dict) -> InlineKeyboardMarkup:
    """Клавиатура превью статьи с кнопками редактирования."""
    rows = [
        [
            InlineKeyboardButton(text="✅ Опубликовать", callback_data="cm_article_confirm"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="adm_content"),
        ],
        [
            InlineKeyboardButton(text="✏️ Заголовок", callback_data="cm_edit_title"),
            InlineKeyboardButton(text="✏️ Категория", callback_data="cm_edit_category"),
            InlineKeyboardButton(text="✏️ Описание", callback_data="cm_edit_desc"),
        ],
        [
            InlineKeyboardButton(
                text="⭐ Золотой тег" + (" ✓" if data.get("isGoldTag") else ""),
                callback_data="cm_article_gold",
            ),
            InlineKeyboardButton(text="📥 + CTA бота", callback_data="cm_article_add_botlink"),
        ],
        [
            InlineKeyboardButton(text="📢 + Пост в канал", callback_data="cm_article_and_channel"),
        ],
        [
            InlineKeyboardButton(
                text="📱 Telegraph" + (" ✓" if data.get("telegraph_url") else " (Instant View)"),
                callback_data="cm_telegraph",
            ),
        ],
        # Новые визуальные функции
        [
            InlineKeyboardButton(
                text="🎨 Обложка (DALL-E)" + (" ✓" if data.get("cover_image_url") else ""),
                callback_data="cm_gen_cover",
            ),
        ],
        [
            InlineKeyboardButton(text="👁 Предпросмотр", callback_data="cm_preview_post"),
            InlineKeyboardButton(text="🔍 Аудит красоты", callback_data="cm_beauty_audit"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _format_preview(data: dict) -> str:
    """Формирует текст превью статьи (HTML)."""
    content_plain = re.sub(r"<[^>]+>", "", data.get("content", ""))[:200]
    gold = " ⭐" if data.get("isGoldTag") else ""
    cta = f"\n🔗 CTA: <code>{data.get('telegramBotLink', '')[:40]}...</code>" if data.get("telegramBotLink") else ""
    tg_url = data.get("telegraph_url", "")
    telegraph = f"\n📱 Telegraph: {tg_url}" if tg_url else ""
    cover = "\n🎨 Обложка: ✅" if data.get("cover_image_url") else ""

    return (
        "📋 <b>AI подготовил статью:</b>\n\n"
        f"📌 <b>{data.get('title', '')}</b>{gold}\n"
        f"📂 {data.get('categoryRu', '')}\n"
        f"📄 {data.get('description', '')}\n"
        f"{cta}{telegraph}{cover}\n\n"
        f"✍️ <i>{content_plain}...</i>\n\n"
        "Что делаем?"
    )


def _collecting_keyboard(parts_count: int, total_chars: int) -> InlineKeyboardMarkup:
    """Клавиатура для режима сбора текста."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text=f"✅ Обработать ({parts_count} ч., {total_chars} симв.)",
                callback_data="cm_process_text",
            )],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="adm_content")],
        ]
    )


@router.message(ArticleForm.waiting_text)
async def article_first_text(message: Message, state: FSMContext) -> None:
    """Первая часть текста — начинаем сбор."""
    if not _is_admin(message.from_user and message.from_user.id):
        return
    raw_text = (message.text or "").strip()

    # Пропускаем команды — сбрасываем состояние, пусть другие хендлеры обработают
    if raw_text.startswith("/"):
        await state.clear()
        return

    if len(raw_text) < 20:
        await message.answer("Текст слишком короткий. Отправьте полный текст статьи:")
        return

    # URL → внешняя ссылка (обрабатываем сразу)
    if raw_text.startswith("http") and "\n" not in raw_text and len(raw_text) < 500:
        await state.update_data(
            externalUrl=raw_text, content="", title="", description="",
            category="News", categoryRu="Новости",
        )
        await message.answer("🔗 Это ссылка. Отправьте заголовок для этой статьи:")
        await state.set_state(ArticleForm.confirm)
        await state.update_data(_need_url_title=True)
        return

    # Сохраняем первую часть, переходим в режим сбора
    await state.update_data(text_parts=[raw_text])
    await state.set_state(ArticleForm.collecting_text)

    await message.answer(
        f"✅ Получил текст ({len(raw_text)} симв.)\n\n"
        "Если Telegram разделил сообщение на части — "
        "<b>отправьте остальные</b>.\n"
        "Когда весь текст отправлен — нажмите <b>«Обработать»</b>.",
        reply_markup=_collecting_keyboard(1, len(raw_text)),
    )


@router.message(ArticleForm.collecting_text)
async def article_more_text(message: Message, state: FSMContext) -> None:
    """Дополнительные части текста."""
    if not _is_admin(message.from_user and message.from_user.id):
        return
    raw_text = (message.text or "").strip()
    if not raw_text:
        return

    # Пропускаем команды — сбрасываем состояние
    if raw_text.startswith("/"):
        await state.clear()
        return

    data = await state.get_data()
    parts = data.get("text_parts", [])
    parts.append(raw_text)
    await state.update_data(text_parts=parts)

    total = sum(len(p) for p in parts)
    await message.answer(
        f"✅ Часть {len(parts)} получена (всего {total} симв.)\n"
        "Продолжайте или нажмите <b>«Обработать»</b>.",
        reply_markup=_collecting_keyboard(len(parts), total),
    )


@router.callback_query(F.data == "cm_process_text")
async def article_process_collected(
    callback: CallbackQuery,
    state: FSMContext,
) -> None:
    """Объединяет все части и запускает AI-обработку."""
    if not _is_admin(callback.from_user.id):
        return
    data = await state.get_data()
    parts = data.get("text_parts", [])

    if not parts:
        await callback.answer("Нет текста")
        return

    await callback.answer()
    raw_text = "\n\n".join(parts)

    # Запускаем AI-обработку
    await _do_ai_article_processing(callback.message, state, raw_text)


async def _do_ai_article_processing(
    msg: Message,
    state: FSMContext,
    raw_text: str,
) -> None:
    """AI-обработка текста статьи: разметка, категоризация, превью."""
    thinking_msg = await msg.answer("🤖 AI размечает статью...")

    try:
        from src.bot.utils.ai_client import ask_content

        ai_response = await ask_content(
            raw_text,
            task="format_article",
            max_tokens=8192,
        )

        cleaned = ai_response.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            cleaned = "\n".join(lines)

        sanitized = []
        for ch in cleaned:
            if unicodedata.category(ch) == "Cc" and ch not in ("\n", "\r", "\t"):
                sanitized.append(" ")
            else:
                sanitized.append(ch)
        cleaned = "".join(sanitized)

        parsed = _json.loads(cleaned, strict=False)

        title = parsed.get("title", "").strip()
        category = parsed.get("category", "Guide").strip()
        category_ru = parsed.get("categoryRu", "").strip()
        description = parsed.get("description", "").strip()
        content = parsed.get("content", "").strip()

        # HTML-санитайзер: очистка и исправление тегов
        from src.bot.utils.html_sanitizer import sanitize_and_fix, unique_slug
        content = sanitize_and_fix(content)

        valid_cats = {c[0] for c in CATEGORIES}
        if category not in valid_cats:
            category = "Guide"
            category_ru = "Гайд для бизнеса"
        if not category_ru:
            category_ru = dict(CATEGORIES).get(category, category)

        # Генерация уникального slug
        existing_slugs: list[str] = []
        article_slug = await unique_slug(title, existing_slugs)

        article_data = {
            "title": title,
            "article_id": article_slug,
            "category": category,
            "categoryRu": category_ru,
            "description": description,
            "content": content,
            "externalUrl": "",
            "telegramBotLink": "",
            "isGoldTag": False,
            "telegraph_url": "",
        }
        await state.update_data(**article_data)

        await thinking_msg.edit_text(
            _format_preview(article_data),
            reply_markup=_article_preview_keyboard(article_data),
        )
        await state.set_state(ArticleForm.confirm)

    except Exception as e:
        logger.error("AI разметка ошибка: %s", e)
        if "<" not in raw_text:
            paragraphs = raw_text.split("\n\n")
            formatted = "\n".join(f"<p>{p.strip()}</p>" for p in paragraphs if p.strip())
        else:
            formatted = raw_text

        first_line = raw_text.split("\n")[0].strip()[:100]
        article_data = {
            "title": first_line,
            "article_id": _slugify(first_line),
            "category": "Guide",
            "categoryRu": "Гайд для бизнеса",
            "description": first_line,
            "content": formatted,
            "externalUrl": "",
            "telegramBotLink": "",
            "isGoldTag": False,
            "telegraph_url": "",
        }
        await state.update_data(**article_data)

        await thinking_msg.edit_text(
            f"⚠️ AI не смог — базовое форматирование.\n\n"
            f"📌 <b>{first_line}</b>\n📂 Гайд для бизнеса\n\nОпубликовать?",
            reply_markup=_article_preview_keyboard(article_data),
        )
        await state.set_state(ArticleForm.confirm)


# ── Редактирование полей статьи ──


@router.callback_query(F.data == "cm_edit_title", ArticleForm.confirm)
async def edit_title(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    await state.set_state(ArticleForm.editing_field)
    await state.update_data(_editing="title")
    await callback.message.answer("✏️ Введите новый заголовок:")


@router.callback_query(F.data == "cm_edit_category", ArticleForm.confirm)
async def edit_category(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    buttons = []
    for cat_en, cat_ru in CATEGORIES:
        buttons.append([InlineKeyboardButton(
            text=f"{cat_ru}", callback_data=f"cm_setcat_{cat_en}",
        )])
    await callback.message.answer(
        "📂 Выберите категорию:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("cm_setcat_"))
async def set_category(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    cat_en = callback.data.removeprefix("cm_setcat_")
    cat_ru = dict(CATEGORIES).get(cat_en, cat_en)
    await state.update_data(category=cat_en, categoryRu=cat_ru)
    await callback.answer(f"Категория: {cat_ru}")
    # Показываем обновлённое превью
    data = await state.get_data()
    await state.set_state(ArticleForm.confirm)
    await callback.message.edit_text(
        _format_preview(data),
        reply_markup=_article_preview_keyboard(data),
    )


@router.callback_query(F.data == "cm_edit_desc", ArticleForm.confirm)
async def edit_desc(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    await state.set_state(ArticleForm.editing_field)
    await state.update_data(_editing="description")
    await callback.message.answer("✏️ Введите новое описание (для превью):")


@router.message(ArticleForm.editing_field)
async def receive_edited_field(message: Message, state: FSMContext) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    text = (message.text or "").strip()
    if text.startswith("/"):
        await state.clear()
        return
    if not text:
        await message.answer("Отправьте текст:")
        return

    data = await state.get_data()
    field = data.get("_editing", "")

    if field == "title":
        await state.update_data(title=text, article_id=_slugify(text))
    elif field == "description":
        await state.update_data(description=text)

    await state.update_data(_editing="")
    await state.set_state(ArticleForm.confirm)
    data = await state.get_data()

    await message.answer(
        _format_preview(data),
        reply_markup=_article_preview_keyboard(data),
    )


# ── URL-статья: ждём заголовок ──


@router.message(ArticleForm.confirm)
async def article_url_title(message: Message, state: FSMContext) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    raw = (message.text or "").strip()
    if raw.startswith("/"):
        await state.clear()
        return

    data = await state.get_data()
    if not data.get("_need_url_title"):
        return

    title = raw
    if len(title) < 5:
        await message.answer("Заголовок слишком короткий (минимум 5 символов):")
        return

    await state.update_data(title=title, article_id=_slugify(title), description=title, _need_url_title=False)
    data = await state.get_data()

    await message.answer(
        f"📋 <b>Внешняя статья:</b>\n\n📌 <b>{title}</b>\n🔗 {data.get('externalUrl', '')}\n\nОпубликовать?",
        reply_markup=_article_preview_keyboard(data),
    )


# ── Кнопки подтверждения ──


@router.callback_query(F.data == "cm_article_gold", ArticleForm.confirm)
async def article_toggle_gold(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    new_val = not data.get("isGoldTag", False)
    await state.update_data(isGoldTag=new_val)
    await callback.answer(f"Золотой тег {'включён' if new_val else 'выключен'}")
    data = await state.get_data()
    await callback.message.edit_text(
        _format_preview(data),
        reply_markup=_article_preview_keyboard(data),
    )


@router.callback_query(F.data == "cm_article_add_botlink", ArticleForm.confirm)
async def article_add_botlink(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    await callback.answer()
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    if not catalog:
        await callback.message.answer("В каталоге нет гайдов. Загрузите через «Гайды».")
        return

    buttons = []
    for guide in catalog:
        gid = guide.get("id", "")
        gtitle = guide.get("title", gid)[:30]
        cb = f"cm_pickguide_{gid}"
        if len(cb.encode("utf-8")) > 64:
            cb = cb[:64]
        buttons.append([InlineKeyboardButton(text=f"📄 {gtitle}", callback_data=cb)])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад к превью", callback_data="cm_skip_botlink")])

    await callback.message.answer(
        "📥 <b>Привязать гайд к статье</b>\n\n"
        "Выберите гайд — в конце статьи появится CTA:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("cm_pickguide_"), ArticleForm.confirm)
async def article_pick_guide(callback: CallbackQuery, state: FSMContext) -> None:
    guide_id = callback.data.removeprefix("cm_pickguide_")
    bot_link = f"https://t.me/SOLIS_Partners_Legal_bot?start=article_{guide_id}"
    await state.update_data(telegramBotLink=bot_link)
    await callback.answer(f"Гайд привязан: {guide_id}")
    data = await state.get_data()
    await callback.message.edit_text(
        _format_preview(data),
        reply_markup=_article_preview_keyboard(data),
    )


@router.callback_query(F.data == "cm_skip_botlink", ArticleForm.confirm)
async def article_skip_botlink(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer("Без CTA бота")
    data = await state.get_data()
    await callback.message.edit_text(
        _format_preview(data), reply_markup=_article_preview_keyboard(data),
    )


# ── Telegraph (Instant View) ──


@router.callback_query(F.data == "cm_telegraph", ArticleForm.confirm)
async def publish_telegraph(callback: CallbackQuery, state: FSMContext) -> None:
    """Публикует статью в Telegraph для Instant View."""
    if not _is_admin(callback.from_user.id):
        return
    data = await state.get_data()

    # Если уже опубликовано — показываем ссылку
    if data.get("telegraph_url"):
        await callback.answer(f"Уже в Telegraph!")
        return

    content = data.get("content", "")
    title = data.get("title", "Без заголовка")

    if not content:
        await callback.answer("Нет HTML-контента для публикации")
        return

    await callback.answer("Публикую в Telegraph...")
    status_msg = await callback.message.answer("⏳ Публикую в Telegraph...")

    try:
        from src.bot.utils.telegraph_client import publish_to_telegraph

        url = await publish_to_telegraph(
            title=title,
            html_content=content,
            author_name="SOLIS Partners",
        )

        await state.update_data(telegraph_url=url)
        data = await state.get_data()

        await status_msg.edit_text(
            f"✅ <b>Статья в Telegraph!</b>\n\n"
            f"📱 {url}\n\n"
            "Эта ссылка даёт Instant View — читатели смогут "
            "читать статью прямо внутри Telegram."
        )

        # Обновляем превью с Telegraph-ссылкой
        try:
            await callback.message.edit_text(
                _format_preview(data),
                reply_markup=_article_preview_keyboard(data),
            )
        except Exception:
            pass

    except Exception as e:
        logger.error("Telegraph publish error: %s", e)
        await status_msg.edit_text(f"❌ Ошибка Telegraph: {e}")


# ── Генерация обложки (DALL-E 3) ──


@router.callback_query(F.data == "cm_gen_cover", ArticleForm.confirm)
async def generate_cover(callback: CallbackQuery, state: FSMContext) -> None:
    """Генерирует AI-обложку через DALL-E 3."""
    if not _is_admin(callback.from_user.id):
        return

    data = await state.get_data()
    if data.get("cover_image_url"):
        await callback.answer("Обложка уже сгенерирована!")
        return

    await callback.answer("🎨 Генерирую обложку через DALL-E 3...")
    status_msg = await callback.message.answer("🎨 AI создаёт обложку в стиле SOLIS Partners...")

    try:
        from src.bot.utils.ai_client import generate_post_image

        title = data.get("title", "Legal article")
        image_url = await generate_post_image(title)

        if image_url:
            await state.update_data(cover_image_url=image_url)
            data = await state.get_data()

            # Отправляем превью обложки
            try:
                await status_msg.delete()
            except Exception:
                pass
            await callback.message.answer_photo(
                photo=image_url,
                caption="🎨 <b>Обложка сгенерирована!</b>\n\n"
                "Она будет использована в Telegraph и посте в канал.",
            )

            # Обновляем главное превью
            try:
                await callback.message.edit_text(
                    _format_preview(data),
                    reply_markup=_article_preview_keyboard(data),
                )
            except Exception:
                pass
        else:
            await status_msg.edit_text("⚠️ Не удалось сгенерировать обложку. Попробуйте позже.")

    except Exception as e:
        logger.error("Cover generation error: %s", e)
        await status_msg.edit_text(f"❌ Ошибка: {e}")


# ── Предпросмотр поста (как увидят подписчики) ──


@router.callback_query(F.data == "cm_preview_post", ArticleForm.confirm)
async def preview_post(callback: CallbackQuery, state: FSMContext) -> None:
    """Отправляет превью поста так, как его увидят подписчики канала."""
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()

    data = await state.get_data()
    title = data.get("title", "")
    description = data.get("description", "")
    category_ru = data.get("categoryRu", "")
    cover_url = data.get("cover_image_url", "")
    telegraph_url = data.get("telegraph_url", "")

    # Формируем пост, максимально похожий на финальный
    preview_text = (
        f"<b>🔥 {title}</b>\n\n"
        "───────────────\n"
        f"📌 <b>Суть:</b> {description}\n\n"
        f"📂 <i>{category_ru}</i>\n\n"
    )

    if telegraph_url:
        preview_text += f'👉 <a href="{telegraph_url}">Читать статью (Instant View)</a>\n'
    else:
        preview_text += "👉 <a href=\"https://www.solispartners.kz/articles\">Читать на сайте</a>\n"

    preview_text += (
        "\n───────────────\n"
        "⚖️ <b>SOLIS Partners</b> — ваш юридический партнёр"
    )

    # Если есть обложка — отправляем фото с подписью
    if cover_url:
        try:
            await callback.message.answer_photo(
                photo=cover_url,
                caption=preview_text,
            )
        except Exception:
            await callback.message.answer(preview_text)
    else:
        await callback.message.answer(preview_text)

    await callback.message.answer(
        "👆 <i>Так будет выглядеть пост для подписчиков.</i>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К редактированию", callback_data="cm_back_to_edit")],
            ]
        ),
    )


# ── Аудит красоты (AI проверка оформления) ──


@router.callback_query(F.data == "cm_beauty_audit", ArticleForm.confirm)
async def beauty_audit(callback: CallbackQuery, state: FSMContext) -> None:
    """AI проверяет визуальное качество текста."""
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer("🔍 Запускаю аудит...")

    data = await state.get_data()
    content = data.get("content", "")[:2000]

    try:
        from src.bot.utils.ai_client import audit_post_beauty

        result = await audit_post_beauty(content)

        score = result.get("score", 0)
        passed = result.get("passed", True)
        issues = result.get("issues", [])
        suggestion = result.get("suggestion", "")

        # Визуализация оценки
        from src.bot.utils.visual import progress_bar

        status = "✅ ПРОШЁЛ" if passed else "⚠️ НУЖНА ДОРАБОТКА"
        score_bar = progress_bar(score, 50, label="Качество")

        text = (
            f"🔍 <b>Аудит визуального качества</b>\n\n"
            f"Статус: <b>{status}</b>\n"
            f"<code>{score_bar}</code>\n\n"
        )

        if issues:
            text += "📋 <b>Замечания:</b>\n"
            for issue in issues[:5]:
                text += f"  ⚠️ {issue}\n"
            text += "\n"

        if suggestion:
            text += f"💡 <b>Рекомендация:</b>\n<i>{suggestion}</i>"

        await callback.message.answer(text)

    except Exception as e:
        logger.error("Beauty audit error: %s", e)
        await callback.message.answer(f"❌ Ошибка аудита: {e}")


# ── Возврат к редактированию из превью ──


@router.callback_query(F.data == "cm_back_to_edit", ArticleForm.confirm)
async def back_to_edit(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    data = await state.get_data()
    await callback.message.answer(
        _format_preview(data),
        reply_markup=_article_preview_keyboard(data),
    )


@router.callback_query(F.data == "cm_article_confirm", ArticleForm.confirm)
async def article_confirm(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
) -> None:
    if not _is_admin(callback.from_user.id):
        return

    data = await state.get_data()
    await state.clear()
    await callback.answer()

    status_msg = await callback.message.edit_text("⏳ Сохраняю статью...")

    try:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)

        await google.append_article(
            article_id=data.get("article_id", ""),
            title=data.get("title", ""),
            date=now.strftime("%d.%m.%Y"),
            author="Чингис Оралбаев",
            category=data.get("category", "Guide"),
            category_ru=data.get("categoryRu", ""),
            description=data.get("description", ""),
            external_url=data.get("externalUrl", ""),
            content=data.get("content", ""),
            is_gold=data.get("isGoldTag", False),
            telegram_bot_link=data.get("telegramBotLink", ""),
        )

        # Telegraph — автопубликация с AI-обложкой
        telegraph_url = data.get("telegraph_url", "")
        cover_url = data.get("cover_image_url", "")
        if not telegraph_url and data.get("content"):
            try:
                # Генерируем обложку через DALL-E
                if not cover_url:
                    try:
                        from src.bot.utils.ai_client import generate_post_image
                        cover_url = await generate_post_image(data.get("title", ""))
                    except Exception as img_e:
                        logger.warning("Cover generation failed: %s", img_e)
                        cover_url = ""

                from src.bot.utils.telegraph_client import publish_to_telegraph
                telegraph_url = await publish_to_telegraph(
                    title=data.get("title", ""),
                    html_content=data.get("content", ""),
                    cover_image_url=cover_url or "",
                )
            except Exception as te:
                logger.warning("Telegraph auto-publish failed: %s", te)

        await status_msg.edit_text("⏳ Статья сохранена. Синхронизирую сайт...")
        success = await _run_site_sync()

        tg_line = f"\n📱 Telegraph: {telegraph_url}" if telegraph_url else ""

        if success:
            await status_msg.edit_text(
                f"✅ <b>Статья опубликована!</b>\n\n📝 {data.get('title', '')}{tg_line}\n\n"
                "Сайт обновится через 1-2 мин (Vercel deploy).",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="📝 Ещё статью", callback_data="cm_publish"),
                         InlineKeyboardButton(text="🏠 Меню", callback_data="adm_home")],
                    ]
                ),
            )
        else:
            await status_msg.edit_text(
                f"⚠️ Статья сохранена, но синк не удался.{tg_line}\n<code>python sync_articles.py</code>",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="🔄 Повторить", callback_data="cm_sync")],
                        [InlineKeyboardButton(text="🏠 Меню", callback_data="adm_home")],
                    ]
                ),
            )
    except Exception as e:
        logger.error("Ошибка публикации: %s", e)
        await status_msg.edit_text(f"❌ Ошибка: {e}")


# ── Публикация + пост в канал одновременно ──


@router.callback_query(F.data == "cm_article_and_channel", ArticleForm.confirm)
async def article_and_channel(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
    bot: Bot,
) -> None:
    """Публикует статью и генерирует пост в канал."""
    if not _is_admin(callback.from_user.id):
        return

    data = await state.get_data()
    await state.clear()
    await callback.answer()

    status_msg = await callback.message.edit_text("⏳ Публикую статью + генерирую пост...")

    try:
        from datetime import datetime, timezone
        from src.bot.utils.ai_client import ask_content

        now = datetime.now(timezone.utc)

        # 1. Сохраняем статью
        await google.append_article(
            article_id=data.get("article_id", ""),
            title=data.get("title", ""),
            date=now.strftime("%d.%m.%Y"),
            author="Чингис Оралбаев",
            category=data.get("category", "Guide"),
            category_ru=data.get("categoryRu", ""),
            description=data.get("description", ""),
            external_url=data.get("externalUrl", ""),
            content=data.get("content", ""),
            is_gold=data.get("isGoldTag", False),
            telegram_bot_link=data.get("telegramBotLink", ""),
        )

        # 2. Запускаем синк + публикацию в Telegraph параллельно
        asyncio.create_task(_run_site_sync())

        telegraph_url = data.get("telegraph_url", "")
        cover_url = data.get("cover_image_url", "")
        if not telegraph_url and data.get("content"):
            try:
                # Генерируем обложку через DALL-E
                if not cover_url:
                    try:
                        from src.bot.utils.ai_client import generate_post_image
                        cover_url = await generate_post_image(data.get("title", ""))
                    except Exception as img_e:
                        logger.warning("Cover generation failed: %s", img_e)
                        cover_url = ""

                from src.bot.utils.telegraph_client import publish_to_telegraph
                telegraph_url = await publish_to_telegraph(
                    title=data.get("title", ""),
                    html_content=data.get("content", ""),
                    cover_image_url=cover_url or "",
                )
            except Exception as te:
                logger.warning("Telegraph auto-publish failed: %s", te)

        # 3. Генерируем пост для канала
        read_link_hint = (
            "с ссылкой на Telegraph (Instant View — чтение внутри Telegram)"
            if telegraph_url
            else "с ссылкой на сайт"
        )
        announce_prompt = (
            f"Статья: {data.get('title', '')}\n"
            f"Категория: {data.get('categoryRu', '')}\n"
            f"Описание: {data.get('description', '')}\n\n"
            f"Создай анонс для Telegram-канала, {read_link_hint}.\n"
            "НЕ добавляй ссылку — она будет добавлена автоматически."
        )

        channel_post = await ask_content(
            announce_prompt,
            task="channel_post",
            max_tokens=512,
        )

        # Добавляем ссылку — Telegraph (Instant View) или сайт
        if telegraph_url:
            channel_post += f"\n\n📖 Читать статью: {telegraph_url}"
        else:
            site_url = "https://www.solispartners.kz/articles"
            channel_post += f"\n\n📎 Читать на сайте: {site_url}"

        # Показываем превью поста
        await status_msg.edit_text(
            f"✅ Статья опубликована!\n\n"
            f"📢 <b>Пост для канала:</b>\n\n{channel_post}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📢 Опубликовать в канал", callback_data="cm_send_channel")],
                    [InlineKeyboardButton(text="✏️ Отредактировать", callback_data="cm_edit_channel_post")],
                    [InlineKeyboardButton(text="⏭️ Пропустить", callback_data="adm_home")],
                ]
            ),
        )
        # Сохраняем текст поста
        await state.update_data(channel_post=channel_post)

    except Exception as e:
        logger.error("Article+channel error: %s", e)
        await status_msg.edit_text(f"⚠️ Статья сохранена, но пост не сгенерирован: {e}")


# ═══════════════════════════════════════════════════════════════════════
#  📚 ГАЙДЫ (уровень 2)
# ═══════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "adm_guides")
async def menu_guides(callback: CallbackQuery, google: GoogleSheetsClient, cache: TTLCache) -> None:
    if not _is_admin(callback.from_user.id):
        return

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    count = len(catalog) if catalog else 0

    await callback.message.edit_text(
        f"📚 <b>Управление гайдами</b>\n\n📊 В каталоге: <b>{count}</b> гайдов\n\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📤 Загрузить гайд (PDF)", callback_data="cm_upload_guide")],
                [InlineKeyboardButton(text="📋 Каталог гайдов", callback_data="adm_guides_list")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_home")],
            ]
        ),
    )
    await callback.answer()


@router.callback_query(F.data == "adm_guides_list")
async def guides_list(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    if not catalog:
        await callback.message.edit_text(
            "📚 Каталог пуст.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📤 Загрузить", callback_data="cm_upload_guide")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_guides")],
                ]
            ),
        )
        return

    text = "📚 <b>Каталог гайдов:</b>\n\n"
    buttons = []
    for g in catalog:
        gid = g.get("id", "?")
        title = g.get("title", gid)[:35]
        text += f"📄 <b>{title}</b>\n   🆔 <code>{gid}</code>\n\n"

        cb = f"adm_gdel_{gid}"
        if len(cb.encode("utf-8")) > 64:
            cb = cb[:64]
        buttons.append([InlineKeyboardButton(text=f"🗑 Удалить: {title[:25]}", callback_data=cb)])

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_guides")])

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("adm_gdel_"))
async def delete_guide_handler(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    if not _is_admin(callback.from_user.id):
        return
    guide_id = callback.data.removeprefix("adm_gdel_")
    success = await google.delete_guide(guide_id)
    if success:
        cache.invalidate()
        await callback.answer(f"Удалён: {guide_id}")
    else:
        await callback.answer("Не найден")
    await guides_list(callback, google, cache)


# ── Загрузка гайда (с AI-определением названия) ──


class GuideForm(StatesGroup):
    waiting_pdf = State()
    waiting_title = State()
    waiting_description = State()
    confirm = State()


@router.callback_query(F.data == "cm_upload_guide")
async def start_upload_guide(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await state.clear()
    await state.set_state(GuideForm.waiting_pdf)
    await callback.message.edit_text(
        "📄 <b>Загрузка гайда</b>\n\n"
        "Отправьте PDF-файл — AI определит название и предложит описание.",
    )
    await callback.answer()


@router.message(Command("upload_guide"))
async def cmd_upload_guide(message: Message, state: FSMContext) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    await state.clear()
    await state.set_state(GuideForm.waiting_pdf)
    await message.answer("📄 Отправьте PDF-файл гайда:")


@router.message(GuideForm.waiting_pdf)
async def guide_pdf(message: Message, state: FSMContext, bot: Bot) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return

    # Пропуск команд
    if message.text and message.text.strip().startswith("/"):
        await state.clear()
        return

    if not message.document:
        await message.answer("Отправьте файл (документ), а не текст.")
        return

    file_name = message.document.file_name or "guide.pdf"
    if not file_name.lower().endswith(".pdf"):
        await message.answer("Нужен PDF. Отправьте файл .pdf:")
        return

    telegram_file_id = message.document.file_id

    # AI определяет название из имени файла
    clean_name = os.path.splitext(file_name)[0].replace("_", " ").replace("-", " ")

    try:
        from src.bot.utils.ai_client import ask_marketing

        ai_result = await ask_marketing(
            prompt=(
                f"Название файла PDF-гайда: '{clean_name}'. "
                "На основе названия файла предложи:\n"
                "1. Красивое название гайда на русском (1 строка)\n"
                "2. Описание для превью (1-2 предложения)\n\n"
                "Формат ответа строго:\n"
                "НАЗВАНИЕ: ...\nОПИСАНИЕ: ..."
            ),
            max_tokens=256,
            temperature=0.5,
        )

        suggested_title = clean_name
        suggested_desc = ""

        for line in ai_result.split("\n"):
            line = line.strip()
            if line.upper().startswith("НАЗВАНИЕ:"):
                suggested_title = line.split(":", 1)[1].strip()
            elif line.upper().startswith("ОПИСАНИЕ:"):
                suggested_desc = line.split(":", 1)[1].strip()

    except Exception:
        suggested_title = clean_name
        suggested_desc = ""

    await state.update_data(
        telegram_file_id=telegram_file_id,
        original_filename=file_name,
        guide_title=suggested_title,
        guide_description=suggested_desc,
        guide_id=_slugify(suggested_title),
    )

    # Показываем превью с AI-предложением
    text = (
        f"✅ Файл получен: <code>{file_name}</code>\n\n"
        f"📝 AI предлагает:\n"
        f"<b>Название:</b> {suggested_title}\n"
    )
    if suggested_desc:
        text += f"<b>Описание:</b> {suggested_desc}\n"

    text += "\nЧто делаем?"

    await message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Загрузить как есть", callback_data="cm_guide_confirm")],
                [InlineKeyboardButton(text="✏️ Название", callback_data="cm_guide_edit_title")],
                [InlineKeyboardButton(text="✏️ Описание", callback_data="cm_guide_edit_desc")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="adm_guides")],
            ]
        ),
    )
    await state.set_state(GuideForm.confirm)


@router.callback_query(F.data == "cm_guide_edit_title")
async def guide_edit_title(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    await state.set_state(GuideForm.waiting_title)
    await callback.message.answer("✏️ Введите название гайда:")


@router.message(GuideForm.waiting_title)
async def guide_title(message: Message, state: FSMContext) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    title = (message.text or "").strip()
    if title.startswith("/"):
        await state.clear()
        return
    if len(title) < 3:
        await message.answer("Слишком короткое:")
        return
    await state.update_data(guide_title=title, guide_id=_slugify(title))
    await state.set_state(GuideForm.confirm)
    data = await state.get_data()
    await message.answer(
        f"📝 <b>{title}</b>\n📖 {data.get('guide_description', '(нет описания)')}\n\nЗагрузить?",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Загрузить", callback_data="cm_guide_confirm")],
                [InlineKeyboardButton(text="✏️ Описание", callback_data="cm_guide_edit_desc")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="adm_guides")],
            ]
        ),
    )


@router.callback_query(F.data == "cm_guide_edit_desc")
async def guide_edit_desc(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    await state.set_state(GuideForm.waiting_description)
    await callback.message.answer("✏️ Введите описание гайда (1-2 предложения):")


@router.message(GuideForm.waiting_description)
async def guide_description(message: Message, state: FSMContext) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    desc = (message.text or "").strip()
    if desc.startswith("/"):
        await state.clear()
        return
    if len(desc) < 5:
        await message.answer("Слишком короткое:")
        return
    await state.update_data(guide_description=desc)
    await state.set_state(GuideForm.confirm)
    data = await state.get_data()
    await message.answer(
        f"📝 <b>{data.get('guide_title', '')}</b>\n📖 {desc}\n\nЗагрузить?",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Загрузить", callback_data="cm_guide_confirm")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="adm_guides")],
            ]
        ),
    )


@router.callback_query(F.data == "cm_guide_confirm", GuideForm.confirm)
async def guide_confirm(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    if not _is_admin(callback.from_user.id):
        return

    data = await state.get_data()
    await state.clear()
    await callback.answer()

    title = data.get("guide_title", "")
    desc = data.get("guide_description", "")
    guide_id = data.get("guide_id", "")
    telegram_file_id = data.get("telegram_file_id", "")
    filename = data.get("original_filename", "guide.pdf")

    status_msg = await callback.message.edit_text("⏳ Сохраняю гайд...")

    try:
        os.makedirs(GUIDES_DIR, exist_ok=True)
        local_path = os.path.join(GUIDES_DIR, f"{guide_id}.pdf")
        file_obj = await bot.get_file(telegram_file_id)
        await bot.download_file(file_obj.file_path, local_path)

        mapping_path = os.path.join(GUIDES_DIR, "telegram_files.json")
        mapping = {}
        if os.path.isfile(mapping_path):
            with open(mapping_path, "r", encoding="utf-8") as f:
                mapping = _json.load(f)
        mapping[guide_id] = {
            "file_id": telegram_file_id,
            "filename": filename,
            "title": title,
        }
        with open(mapping_path, "w", encoding="utf-8") as f:
            _json.dump(mapping, f, ensure_ascii=False, indent=2)

        await google.append_guide(
            guide_id=guide_id,
            title=title,
            description=desc,
            drive_file_id=f"local:{guide_id}",
        )
        cache.invalidate()

        await status_msg.edit_text(
            f"✅ <b>Гайд загружен!</b>\n\n📄 {title}\n🆔 <code>{guide_id}</code>\n\n"
            "Гайд сразу доступен в боте.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📤 Ещё гайд", callback_data="cm_upload_guide")],
                    [InlineKeyboardButton(text="🏠 Меню", callback_data="adm_home")],
                ]
            ),
        )
    except Exception as e:
        logger.error("Ошибка загрузки гайда: %s", e)
        await status_msg.edit_text(f"❌ Ошибка: {e}")


# ═══════════════════════════════════════════════════════════════════════
#  📢 МАРКЕТИНГ (уровень 2)
# ═══════════════════════════════════════════════════════════════════════


class ChannelPostForm(StatesGroup):
    writing = State()
    confirm = State()


@router.callback_query(F.data == "adm_marketing")
async def menu_marketing(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(
        "📢 <b>Маркетинг</b>\n\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📢 Пост в канал", callback_data="adm_channel_post")],
                [InlineKeyboardButton(text="📝 Статья + пост (комбо)", callback_data="cm_publish")],
                [InlineKeyboardButton(text="📅 Контент-календарь", callback_data="adm_content_cal")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_home")],
            ]
        ),
    )
    await callback.answer()


# ── Пост в канал ──


@router.callback_query(F.data == "adm_channel_post")
async def start_channel_post(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await state.clear()
    await state.set_state(ChannelPostForm.writing)
    await callback.message.edit_text(
        "📢 <b>Пост в канал</b>\n\n"
        "Варианты:\n"
        "• Отправьте готовый текст поста\n"
        "• Или опишите тему — AI сгенерирует пост\n\n"
        "Канал: @SOLISlegal",
    )
    await callback.answer()


@router.message(ChannelPostForm.writing)
async def channel_post_text(message: Message, state: FSMContext, bot: Bot) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    raw = (message.text or "").strip()
    if raw.startswith("/"):
        await state.clear()
        return
    if len(raw) < 10:
        await message.answer("Слишком короткий текст:")
        return

    # Если короткий текст (тема) → AI генерирует
    if len(raw) < 100:
        thinking = await message.answer("🤖 Генерирую пост...")
        try:
            from src.bot.utils.ai_client import ask_content

            post_text = await ask_content(
                f"Тема: {raw}\nНапиши пост для Telegram-канала.",
                task="channel_post",
                max_tokens=512,
            )
            await thinking.delete()
        except Exception as e:
            post_text = raw
            await thinking.edit_text(f"⚠️ AI не смог. Используем ваш текст.")
    else:
        post_text = raw

    await state.update_data(channel_post=post_text)
    await state.set_state(ChannelPostForm.confirm)

    await message.answer(
        f"📢 <b>Превью поста:</b>\n\n{post_text}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📢 Отправить в канал", callback_data="cm_send_channel")],
                [InlineKeyboardButton(text="✏️ Редактировать", callback_data="cm_edit_channel_post")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="adm_marketing")],
            ]
        ),
    )


@router.callback_query(F.data == "cm_send_channel")
async def send_channel_post(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not _is_admin(callback.from_user.id):
        return
    data = await state.get_data()
    post_text = data.get("channel_post", "")
    if not post_text:
        await callback.answer("Нет текста поста")
        return

    await callback.answer()
    try:
        channel = settings.CHANNEL_USERNAME
        cover_url = data.get("cover_image_url", "")

        # Если есть обложка — отправляем фото с подписью
        if cover_url:
            try:
                await bot.send_photo(
                    chat_id=channel,
                    photo=cover_url,
                    caption=post_text,
                )
            except Exception:
                # Fallback: текст без обложки
                await bot.send_message(chat_id=channel, text=post_text)
        else:
            await bot.send_message(chat_id=channel, text=post_text)

        await state.clear()
        await callback.message.edit_text(
            "✅ <b>Пост опубликован в канал!</b>",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📢 Ещё пост", callback_data="adm_channel_post")],
                    [InlineKeyboardButton(text="🏠 Меню", callback_data="adm_home")],
                ]
            ),
        )
    except Exception as e:
        logger.error("Channel post error: %s", e)
        await callback.message.answer(f"❌ Ошибка отправки в канал: {e}")


@router.callback_query(F.data == "cm_edit_channel_post")
async def edit_channel_post(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    await state.set_state(ChannelPostForm.writing)
    await callback.message.answer("✏️ Отправьте отредактированный текст поста:")


# ── Контент-календарь ──


@router.callback_query(F.data == "adm_content_cal")
async def content_calendar(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()

    calendar = await google.get_content_calendar()

    text = "📅 <b>Контент-календарь:</b>\n\n"
    if not calendar:
        text += "(пусто — AI будет предлагать идеи в дайджестах)"
    else:
        for item in calendar[-10:]:
            date = item.get("date", "?")
            ctype = item.get("type", "?")
            title = item.get("title", "?")[:40]
            status = item.get("status", "planned")
            emoji = "✅" if status == "done" else "📝" if status == "in_progress" else "📅"
            text += f"{emoji} {date} | {ctype} | {title}\n"

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text="📊 Открыть в Sheets",
                    url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
                )],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_marketing")],
            ]
        ),
    )


# ═══════════════════════════════════════════════════════════════════════
#  🧠 AI АССИСТЕНТ (уровень 2)
# ═══════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "adm_ai")
async def menu_ai(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(
        "🧠 <b>AI Ассистент</b>\n\nВыберите режим:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💬 Чат с AI-стратегом", callback_data="adm_ai_chat")],
                [InlineKeyboardButton(text="💡 Генерация идей", callback_data="strat_ideas")],
                [InlineKeyboardButton(text="📰 Свежие новости", callback_data="adm_ai_news")],
                [InlineKeyboardButton(text="❓ Auto-FAQ (популярные вопросы)", callback_data="adm_auto_faq")],
                [InlineKeyboardButton(text="🗂 Data Room", callback_data="adm_data_room")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_home")],
            ]
        ),
    )
    await callback.answer()


@router.callback_query(F.data == "adm_ai_chat")
async def ai_chat_redirect(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    await callback.message.answer(
        "Отправьте /chat для начала диалога с AI-стратегом."
    )


# ── Свежие новости ──


@router.callback_query(F.data == "adm_ai_news")
async def ai_news(callback: CallbackQuery, google: GoogleSheetsClient) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer("Загружаю новости...")

    try:
        from src.bot.utils.news_parser import fetch_all_news

        news = await fetch_all_news()
        if not news:
            await callback.message.edit_text(
                "📰 Новых релевантных новостей не найдено.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_ai")]],
                ),
            )
            return

        text = "📰 <b>Свежие новости:</b>\n\n"
        for i, item in enumerate(news[:8], 1):
            title = item.get("title", "?")[:60]
            source = item.get("source", "?")
            text += f"{i}. [{source}] {title}\n"

        if len(text) > 4000:
            text = text[:4000] + "..."

        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🤖 AI-анализ новостей", callback_data="adm_ai_analyze_news")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_ai")],
                ]
            ),
        )

    except Exception as e:
        await callback.message.edit_text(
            f"❌ Ошибка: {e}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_ai")]],
            ),
        )


@router.callback_query(F.data == "adm_ai_analyze_news")
async def ai_analyze_news(callback: CallbackQuery, google: GoogleSheetsClient) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer("AI анализирует...")

    try:
        from src.bot.utils.news_parser import fetch_all_news
        from src.bot.utils.ai_client import ask_marketing

        news = await fetch_all_news()
        news_text = "\n".join(f"- {n.get('title', '')}" for n in news[:10])

        response = await ask_marketing(
            prompt=(
                "Проанализируй свежие новости и предложи:\n"
                "1. Какие новости можно использовать для контента?\n"
                "2. Предложи 2-3 идеи постов/статей на основе них\n"
                "3. Как связать с услугами SOLIS Partners?"
            ),
            context=f"СВЕЖИЕ НОВОСТИ:\n{news_text}",
            max_tokens=1500,
            temperature=0.7,
        )

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📝 Написать статью", callback_data="cm_publish")],
                [InlineKeyboardButton(text="📢 Пост в канал", callback_data="adm_channel_post")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_ai")],
            ]
        )
        text = f"🤖 <b>AI-анализ новостей:</b>\n\n{response}"
        try:
            await callback.message.answer(text, reply_markup=kb)
        except Exception:
            await callback.message.answer(text, reply_markup=kb, parse_mode=None)

    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")


# ── Auto-FAQ ──


@router.callback_query(F.data == "adm_auto_faq")
async def auto_faq(callback: CallbackQuery, google: GoogleSheetsClient) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer("Анализирую...")

    try:
        consult_log = await google.get_consult_log(limit=50)

        if not consult_log:
            await callback.message.edit_text(
                "❓ <b>Auto-FAQ</b>\n\nПока нет данных. Вопросы из /consult будут накапливаться, "
                "и AI определит популярные темы.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_ai")]],
                ),
            )
            return

        from src.bot.utils.ai_client import ask_marketing

        questions = "\n".join(
            f"- {q.get('question', q.get('Вопрос', ''))[:100]}"
            for q in consult_log[-30:]
        )

        response = await ask_marketing(
            prompt=(
                "Проанализируй вопросы пользователей юридического бота:\n"
                "1. Выдели 3-5 самых популярных тем/вопросов\n"
                "2. Для каждой темы предложи: создать ли гайд, статью или пост\n"
                "3. Предложи конкретные заголовки"
            ),
            context=f"ВОПРОСЫ ПОЛЬЗОВАТЕЛЕЙ:\n{questions}",
            max_tokens=1024,
            temperature=0.5,
        )

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📝 Создать контент", callback_data="cm_publish")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_ai")],
            ]
        )
        text = f"❓ <b>Auto-FAQ — популярные темы:</b>\n\n{response}"
        try:
            await callback.message.edit_text(text, reply_markup=kb)
        except Exception:
            await callback.message.answer(text, reply_markup=kb, parse_mode=None)

    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")


# ── Data Room ──


class DataRoomForm(StatesGroup):
    adding = State()


@router.callback_query(F.data == "adm_data_room")
async def data_room_menu(callback: CallbackQuery, google: GoogleSheetsClient) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()

    data = await google.get_data_room()
    text = "🗂 <b>Data Room — знания о компании</b>\n\n"
    if not data:
        text += "(пусто — добавьте информацию о компании для AI-контекста)"
    else:
        for item in data[:15]:
            cat = item.get("category", item.get("Категория", ""))
            title = item.get("title", item.get("Заголовок", ""))[:40]
            text += f"• [{cat}] {title}\n"
        if len(data) > 15:
            text += f"\n... и ещё {len(data) - 15}"

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить запись", callback_data="adm_dr_add")],
                [InlineKeyboardButton(
                    text="📊 Открыть в Sheets",
                    url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
                )],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_ai")],
            ]
        ),
    )


@router.callback_query(F.data == "adm_dr_add")
async def data_room_add(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    await state.set_state(DataRoomForm.adding)
    await callback.message.answer(
        "➕ <b>Добавить в Data Room</b>\n\n"
        "Формат:\n"
        "<code>Категория | Заголовок | Описание</code>\n\n"
        "Категории: Услуги, Кейсы, Команда, КП, Процессы, Прочее\n\n"
        "Пример:\n"
        "<code>Услуги | ESOP для стартапов | Разрабатываем опционные программы...</code>"
    )


@router.message(DataRoomForm.adding)
async def data_room_save(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    text = (message.text or "").strip()
    if text.startswith("/"):
        await state.clear()
        return
    parts = text.split("|")
    if len(parts) < 2:
        await message.answer("Используйте формат: <code>Категория | Заголовок | Описание</code>")
        return

    category = parts[0].strip()
    title = parts[1].strip()
    content = parts[2].strip() if len(parts) > 2 else ""

    await google.append_data_room(category=category, title=title, content=content)
    await state.clear()

    await message.answer(
        f"✅ Добавлено в Data Room:\n[{category}] {title}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Ещё", callback_data="adm_dr_add")],
                [InlineKeyboardButton(text="🏠 Меню", callback_data="adm_home")],
            ]
        ),
    )


# ═══════════════════════════════════════════════════════════════════════
#  📊 АНАЛИТИКА (уровень 2)
# ═══════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "adm_analytics")
async def menu_analytics(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()

    # Собираем данные
    from src.database.crud import get_all_user_ids

    user_ids = await get_all_user_ids()
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    leads = await google.get_recent_leads(limit=200)

    # Лиды за сегодня
    from datetime import datetime, timedelta, timezone as tz

    almaty = tz(timedelta(hours=5))
    today = datetime.now(almaty).strftime("%d.%m.%Y")

    # Считаем по дате в формате DD.MM.YYYY или YYYY-MM-DD
    today_iso = datetime.now(almaty).strftime("%Y-%m-%d")
    today_leads = [
        l for l in leads
        if l.get("timestamp", "").startswith(today) or l.get("timestamp", "").startswith(today_iso)
    ]

    # Топ гайды
    guide_counts: dict[str, int] = {}
    for l in leads:
        g = str(l.get("guide", ""))
        if g:
            guide_counts[g] = guide_counts.get(g, 0) + 1
    top_guides = sorted(guide_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    # Топ источники
    source_counts: dict[str, int] = {}
    for l in leads:
        s = str(l.get("source", "")).strip()
        if s:
            source_counts[s] = source_counts.get(s, 0) + 1
    top_sources = sorted(source_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    text = (
        "📊 <b>Аналитика SOLIS Bot</b>\n\n"
        f"👥 Пользователей: <b>{len(user_ids)}</b>\n"
        f"📚 Гайдов: <b>{len(catalog)}</b>\n"
        f"📋 Всего лидов: <b>{len(leads)}</b>\n"
        f"🔥 Лидов сегодня: <b>{len(today_leads)}</b>\n\n"
    )

    if top_guides:
        text += "📚 <b>Топ гайдов:</b>\n"
        for g, c in top_guides:
            text += f"  • {g}: {c}\n"
        text += "\n"

    if top_sources:
        text += "📍 <b>Источники:</b>\n"
        for s, c in top_sources:
            text += f"  • {s}: {c}\n"
        text += "\n"

    # Последние 3 лида
    if leads:
        text += "👤 <b>Последние лиды:</b>\n"
        for l in leads[-3:]:
            name = l.get("name", "?")
            email = l.get("email", "?")
            guide = l.get("guide", "?")
            text += f"  • {name} ({email}) — {guide}\n"

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text="📊 Полная аналитика в Sheets",
                    url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
                )],
                [InlineKeyboardButton(text="🔄 Обновить аналитику", callback_data="adm_refresh_analytics")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_home")],
            ]
        ),
    )


@router.callback_query(F.data == "adm_refresh_analytics")
async def refresh_analytics(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer("Обновляю...")
    await google.update_analytics()
    await callback.message.answer("✅ Аналитика в Google Sheets обновлена!")


# ═══════════════════════════════════════════════════════════════════════
#  ⚙️ НАСТРОЙКИ (уровень 2)
# ═══════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "adm_settings")
async def menu_settings(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.message.edit_text(
        "⚙️ <b>Настройки</b>\n\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Синхронизировать сайт", callback_data="cm_sync")],
                [InlineKeyboardButton(text="🗑 Сбросить кеш", callback_data="adm_clear_cache")],
                [InlineKeyboardButton(
                    text="📊 Google Sheets",
                    url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
                )],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_home")],
            ]
        ),
    )
    await callback.answer()


@router.callback_query(F.data == "adm_clear_cache")
async def clear_cache(callback: CallbackQuery, cache: TTLCache) -> None:
    if not _is_admin(callback.from_user.id):
        return
    cache.invalidate()
    await callback.answer("Кеш сброшен!")
    await callback.message.answer("✅ Кеш очищен. Данные обновятся при следующем запросе.")


# ═══════════════════════════════════════════════════════════════════════
#  СИНХРОНИЗАЦИЯ САЙТА
# ═══════════════════════════════════════════════════════════════════════


@router.callback_query(F.data == "cm_sync")
async def sync_site_callback(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()
    status_msg = await callback.message.edit_text("⏳ Синхронизирую сайт...")

    success = await _run_site_sync()

    if success:
        await status_msg.edit_text(
            "✅ Сайт синхронизирован! Vercel задеплоит через 1-2 мин.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="adm_home")]],
            ),
        )
    else:
        await status_msg.edit_text(
            "⚠️ Синхронизация не удалась.\n<code>python sync_articles.py</code>",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🏠 Меню", callback_data="adm_home")]],
            ),
        )


@router.message(Command("site_sync"))
async def cmd_site_sync(message: Message) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    status_msg = await message.answer("⏳ Синхронизирую сайт...")
    success = await _run_site_sync()
    if success:
        await status_msg.edit_text("✅ Сайт синхронизирован!")
    else:
        await status_msg.edit_text("⚠️ Ошибка. <code>python sync_articles.py</code>")


# ── Обратная совместимость: старые callback cm_stats ──


@router.callback_query(F.data == "cm_stats")
async def stats_compat(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    await menu_analytics(callback, google, cache)


# ═══════════════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ
# ═══════════════════════════════════════════════════════════════════════


async def _run_site_sync() -> bool:
    """Запускает sync_articles.py через subprocess (venv Python)."""
    import sys as _sys

    script = os.path.normpath(SYNC_SCRIPT)
    if not os.path.isfile(script):
        logger.error("sync_articles.py не найден: %s", script)
        return False

    python_exe = _sys.executable
    project_root = os.path.dirname(script)

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            [python_exe, script],
            capture_output=True,
            text=True,
            timeout=120,
            encoding="utf-8",
            errors="replace",
            cwd=project_root,
        )
        if result.returncode == 0:
            logger.info("Site sync OK: %s", result.stdout[-500:] if result.stdout else "")
            return True
        else:
            logger.error(
                "Site sync FAIL (rc=%d):\nSTDOUT: %s\nSTDERR: %s",
                result.returncode,
                result.stdout[-300:] if result.stdout else "",
                result.stderr[-300:] if result.stderr else "",
            )
            return False
    except Exception as e:
        logger.error("Site sync error: %s", e)
        return False
