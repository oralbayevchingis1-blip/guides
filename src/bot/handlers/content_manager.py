"""Админ-панель — управление гайдами.

/admin → Меню:
    ├── 📚 Гайды → загрузка, каталог, удаление
    └── 📊 Статистика → быстрая ссылка на CRM
"""

import json as _json
import logging
import os
import re

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

from difflib import SequenceMatcher

from src.bot.utils.ai_assistant import _ask_openai, _ask_gemini
from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.config import settings

router = Router()
logger = logging.getLogger(__name__)

GUIDES_DIR = os.path.join("data", "guides")


def _is_admin(user_id: int | None) -> bool:
    return user_id == settings.ADMIN_ID


def _slugify(text: str) -> str:
    """URL-совместимый ID из текста."""
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
#  ГЛАВНОЕ МЕНЮ
# ═══════════════════════════════════════════════════════════════════════


def _main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📚 Гайды", callback_data="adm_guides")],
            [InlineKeyboardButton(text="📊 Открыть CRM",
                url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit")],
        ]
    )


@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return
    await state.clear()
    await message.answer(
        "🏠 <b>Панель управления</b>\n\nВыберите действие:",
        reply_markup=_main_menu_keyboard(),
    )


@router.callback_query(F.data == "adm_home")
async def go_home(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.edit_text(
        "🏠 <b>Панель управления</b>\n\nВыберите действие:",
        reply_markup=_main_menu_keyboard(),
    )
    await callback.answer()


# Обратная совместимость
@router.callback_query(F.data == "cm_back_menu")
async def back_compat(callback: CallbackQuery, state: FSMContext) -> None:
    await go_home(callback, state)


@router.callback_query(F.data == "admin_home")
async def admin_home_compat(callback: CallbackQuery, state: FSMContext) -> None:
    await go_home(callback, state)


# ═══════════════════════════════════════════════════════════════════════
#  📚 ГАЙДЫ
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
    bot: Bot,
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

    bot_info = await bot.get_me()
    bot_username = bot_info.username

    # Получаем счётчики скачиваний одним запросом
    from src.database.crud import count_guide_downloads_bulk
    guide_ids = [str(g.get("id", "")) for g in catalog if g.get("id")]
    dl_counts = await count_guide_downloads_bulk(guide_ids) if guide_ids else {}

    text = "📚 <b>Каталог гайдов:</b>\n\n"
    buttons = []
    for g in catalog:
        gid = g.get("id", "?")
        title = g.get("title", gid)[:35]
        deep_link = f"https://t.me/{bot_username}?start=guide_{gid}"
        dl = dl_counts.get(gid, 0)
        text += (
            f"📄 <b>{title}</b>"
            f" · 📊 {dl} скач.\n"
            f"   🆔 <code>{gid}</code>\n"
            f"   🔗 <code>{deep_link}</code>\n\n"
        )

        cb_promo = f"adm_gpromo_{gid}"
        cb_del = f"adm_gdel_{gid}"
        if len(cb_promo.encode("utf-8")) > 64:
            cb_promo = cb_promo[:64]
        if len(cb_del.encode("utf-8")) > 64:
            cb_del = cb_del[:64]
        buttons.append([
            InlineKeyboardButton(text=f"📣 Промо: {title[:20]}", callback_data=cb_promo),
            InlineKeyboardButton(text="🗑", callback_data=cb_del),
        ])

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="adm_guides")])

    # Telegram ограничивает text до 4096 символов; если слишком длинный — разбиваем
    if len(text) > 4000:
        await callback.message.delete()
        await callback.message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
    else:
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )


@router.callback_query(F.data.startswith("adm_gdel_"))
async def delete_guide_handler(
    callback: CallbackQuery,
    bot: Bot,
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
    await guides_list(callback, bot, google, cache)


# ── Промо-генератор ──


@router.callback_query(F.data.startswith("adm_gpromo_"))
async def guide_promo_handler(
    callback: CallbackQuery,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Генерирует промо-материалы для гайда: пост для канала, блок для статьи."""
    if not _is_admin(callback.from_user.id):
        return

    guide_id = callback.data.removeprefix("adm_gpromo_")
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    guide = None
    for g in catalog:
        if str(g.get("id", "")) == guide_id:
            guide = g
            break

    if not guide:
        await callback.answer("Гайд не найден", show_alert=True)
        return

    await callback.answer()

    bot_info = await bot.get_me()
    bot_username = bot_info.username

    from src.database.crud import count_guide_downloads
    dl_count = await count_guide_downloads(guide_id)

    from src.bot.utils.promo import build_guide_promo
    promo = build_guide_promo(
        guide, bot_username,
        utm_source="channel",
        download_count=dl_count,
    )

    # Сообщение 1: Пост для канала (готов к пересылке)
    await callback.message.answer(
        "📣 <b>Готовый пост для канала:</b>\n"
        "<i>(перешлите в канал или скопируйте)</i>\n\n"
        "─" * 20,
    )
    await callback.message.answer(promo["channel_post"])

    # Сообщение 2: CTA для статьи (Telegraph / сайт)
    await callback.message.answer(
        "📝 <b>CTA-блок для статьи:</b>\n"
        "<i>(вставьте в конец статьи или поста)</i>\n\n"
        "─" * 20 + "\n\n"
        + promo["telegraph_cta"],
    )

    # Сообщение 3: Deep links для разных каналов
    links_text = (
        "🔗 <b>Deep links с UTM:</b>\n\n"
        f"📱 Канал:\n<code>{_make_deep_link(bot_username, guide_id, 'channel')}</code>\n\n"
        f"📧 Email:\n<code>{_make_deep_link(bot_username, guide_id, 'email')}</code>\n\n"
        f"💼 LinkedIn:\n<code>{_make_deep_link(bot_username, guide_id, 'linkedin')}</code>\n\n"
        f"📘 Facebook:\n<code>{_make_deep_link(bot_username, guide_id, 'facebook')}</code>\n\n"
        f"📸 Instagram:\n<code>{_make_deep_link(bot_username, guide_id, 'instagram')}</code>\n\n"
        f"🌐 Сайт:\n<code>{_make_deep_link(bot_username, guide_id, 'website')}</code>\n\n"
        f"📋 Короткий CTA:\n<code>{promo['short_cta']}</code>"
    )
    await callback.message.answer(
        links_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К каталогу", callback_data="adm_guides_list")],
        ]),
    )


def _make_deep_link(bot_username: str, guide_id: str, source: str) -> str:
    return f"https://t.me/{bot_username}?start=guide_{guide_id}--{source}"


# ── Загрузка гайда ──


class GuideForm(StatesGroup):
    waiting_pdf = State()
    waiting_title = State()
    waiting_description = State()
    confirm = State()


def _find_duplicates(
    filename: str, title: str, catalog: list[dict], threshold: float = 0.55,
) -> list[dict]:
    """Ищет похожие гайды в каталоге по названию файла или title."""
    results = []
    fn_lower = filename.lower().replace(".pdf", "").replace("_", " ").replace("-", " ")
    t_lower = title.lower()

    for guide in catalog:
        existing_title = (guide.get("title") or "").lower()
        existing_id = (guide.get("id") or "").lower().replace("-", " ").replace("_", " ")

        score = max(
            SequenceMatcher(None, t_lower, existing_title).ratio(),
            SequenceMatcher(None, fn_lower, existing_title).ratio(),
            SequenceMatcher(None, fn_lower, existing_id).ratio(),
        )
        if score >= threshold:
            results.append({"guide": guide, "score": score})

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:3]


_SUGGEST_PROMPT = """\
Ты — редактор юридической фирмы SOLIS Partners (Казахстан).

Дано название файла PDF-гайда: "{filename}"

Задача:
1. Придумай короткое, цепляющее НАЗВАНИЕ для кнопки в Telegram-боте (до 30 символов).
   Оно должно быть понятным и вызывать желание скачать.
2. Напиши ОПИСАНИЕ (1-2 предложения, до 120 символов) — что внутри гайда, какая польза.

Отвечай СТРОГО в формате:
НАЗВАНИЕ: <название>
ОПИСАНИЕ: <описание>

Не добавляй ничего лишнего.
"""


async def _suggest_title_desc(filename: str) -> tuple[str, str]:
    """Просит ИИ предложить название и описание для гайда."""
    prompt = _SUGGEST_PROMPT.format(filename=filename)

    answer = await _ask_openai(prompt)
    if not answer:
        answer = await _ask_gemini(prompt)
    if not answer:
        return "", ""

    title = ""
    desc = ""
    for line in answer.strip().splitlines():
        line = line.strip()
        if line.upper().startswith("НАЗВАНИЕ:"):
            title = line.split(":", 1)[1].strip().strip('"').strip("«»")
        elif line.upper().startswith("ОПИСАНИЕ:"):
            desc = line.split(":", 1)[1].strip().strip('"').strip("«»")
    return title, desc


@router.callback_query(F.data == "cm_upload_guide")
async def start_upload_guide(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await state.clear()
    await state.set_state(GuideForm.waiting_pdf)
    await callback.message.edit_text(
        "📄 <b>Загрузка гайда</b>\n\nОтправьте PDF-файл.",
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
async def guide_pdf(
    message: Message, state: FSMContext, bot: Bot,
    google: GoogleSheetsClient, cache: TTLCache,
) -> None:
    if not _is_admin(message.from_user and message.from_user.id):
        return

    if message.text and message.text.strip().startswith("/"):
        await state.clear()
        return

    if not message.document:
        await message.answer("Отправьте файл (документ), а не текст.")
        return

    file_name = message.document.file_name or "guide.pdf"
    if not file_name.lower().endswith(".pdf"):
        await message.answer("Нужен PDF. Отправьте .pdf файл:")
        return

    telegram_file_id = message.document.file_id
    clean_name = os.path.splitext(file_name)[0].replace("_", " ").replace("-", " ")

    # ── Проверка дубликатов ────────────────────────────────────────────
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    duplicates = _find_duplicates(file_name, clean_name, catalog)

    dup_warning = ""
    if duplicates:
        dup_lines = []
        for d in duplicates:
            g = d["guide"]
            pct = int(d["score"] * 100)
            dup_lines.append(
                f"  — <b>{g.get('title', '?')}</b> ({pct}% совпадение)"
            )
        dup_warning = (
            "\n⚠️ <b>Возможные дубликаты:</b>\n"
            + "\n".join(dup_lines)
            + "\n\nЕсли это тот же гайд — отмените загрузку.\n"
        )

    # ── ИИ-предложение названия и описания ─────────────────────────────
    status_msg = await message.answer("🔹 Файл получен. Анализирую...")

    suggested_title, suggested_desc = await _suggest_title_desc(file_name)

    # Выбираем лучшее название
    final_title = suggested_title or clean_name
    final_desc = suggested_desc or ""

    await state.update_data(
        telegram_file_id=telegram_file_id,
        original_filename=file_name,
        guide_title=final_title,
        guide_description=final_desc,
        guide_id=_slugify(final_title),
    )

    # ── Формируем карточку подтверждения ───────────────────────────────
    card = f"🔹 <b>Новый гайд</b>\n\n"
    card += f"📎 Файл: <code>{file_name}</code>\n"
    card += f"\n🔹 <b>Название:</b> {final_title}\n"
    if final_desc:
        card += f"🔹 <b>Описание:</b> {final_desc}\n"
    else:
        card += f"🔹 <b>Описание:</b> <i>(не задано)</i>\n"

    if suggested_title:
        card += f"\n💡 <i>Название и описание предложены ИИ-ассистентом</i>\n"

    card += dup_warning
    card += "\nЧто делаем?"

    await status_msg.edit_text(
        card,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔹 Загрузить", callback_data="cm_guide_confirm")],
                [InlineKeyboardButton(text="✏️ Изменить название", callback_data="cm_guide_edit_title")],
                [InlineKeyboardButton(text="✏️ Изменить описание", callback_data="cm_guide_edit_desc")],
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
            f"🔹 <b>Гайд загружен!</b>\n\n"
            f"{title}\n"
            f"<code>{guide_id}</code>\n\n"
            "Гайд сразу доступен в боте.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(
                        text="📢 Анонсировать в канале",
                        callback_data=f"cm_announce_{guide_id}",
                    )],
                    [InlineKeyboardButton(text="🔹 Ещё гайд", callback_data="cm_upload_guide")],
                    [InlineKeyboardButton(text="🔹 Меню", callback_data="adm_home")],
                ]
            ),
        )
    except Exception as e:
        logger.error("Ошибка загрузки гайда: %s", e)
        await status_msg.edit_text(f"❌ Ошибка: {e}")


# ═══════════════════════════════════════════════════════════════════════
#  📢 КАНАЛ — анонсы и дайджесты
# ═══════════════════════════════════════════════════════════════════════


@router.callback_query(F.data.startswith("cm_announce_"))
async def announce_guide_to_channel(
    callback: CallbackQuery,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Публикует анонс гайда в канал."""
    if not _is_admin(callback.from_user.id):
        return

    guide_id = callback.data.removeprefix("cm_announce_")
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    guide = next((g for g in catalog if g.get("id") == guide_id), None)

    if not guide:
        await callback.answer("Гайд не найден в каталоге", show_alert=True)
        return

    await callback.answer()

    from src.bot.utils.channel_publisher import post_new_guide
    ok = await post_new_guide(bot, guide)

    if ok:
        await callback.message.answer(
            f"📢 Анонс <b>{guide.get('title', guide_id)}</b> "
            f"опубликован в {settings.CHANNEL_USERNAME}!"
        )
    else:
        await callback.message.answer(
            "❌ Не удалось опубликовать. Проверьте, что бот — "
            f"администратор канала {settings.CHANNEL_USERNAME}."
        )


@router.message(Command("channel_post"))
async def cmd_channel_post(
    message: Message,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Публикует дайджест или анонс конкретного гайда в канал.

    /channel_post — дайджест из 3 гайдов
    /channel_post guide_id — анонс конкретного гайда
    """
    if not _is_admin(message.from_user and message.from_user.id):
        return

    args = (message.text or "").split(maxsplit=1)
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    if len(args) > 1:
        # Конкретный гайд
        guide_id = args[1].strip()
        guide = next((g for g in catalog if g.get("id") == guide_id), None)
        if not guide:
            await message.answer(f"Гайд <code>{guide_id}</code> не найден.")
            return

        from src.bot.utils.channel_publisher import post_new_guide
        ok = await post_new_guide(bot, guide)
        status = "опубликован" if ok else "ошибка"
        await message.answer(f"📢 Анонс: {status}")
    else:
        # Дайджест
        from src.bot.utils.channel_publisher import post_weekly_digest
        ok = await post_weekly_digest(bot, catalog, top_n=3)
        status = "опубликован" if ok else "ошибка"
        await message.answer(f"📢 Дайджест в канал: {status}")


@router.message(Command("channel_digest"))
async def cmd_channel_digest(
    message: Message,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Публикует полный обзор всех гайдов в канал."""
    if not _is_admin(message.from_user and message.from_user.id):
        return

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    if not catalog:
        await message.answer("Каталог пуст.")
        return

    info = await bot.get_me()
    bot_username = info.username

    lines = [
        "🔹 <b>Бесплатные PDF-гайды от SOLIS Partners</b>\n",
        "Мы подготовили серию практических гайдов на основе "
        "реальных кейсов. В каждом — пошаговые инструкции, "
        "чек-листы и примеры документов.\n",
    ]

    for guide in catalog:
        title = guide.get("title", "?")
        desc = guide.get("description", "")
        short = f"\n  <i>{desc[:70]}</i>" if desc else ""
        lines.append(f"— <b>{title}</b>{short}\n")

    lines.append(
        f"\nВсего {len(catalog)} гайдов. "
        "Скачивайте бесплатно в нашем боте 👇"
    )

    text = "\n".join(lines)
    start_link = f"https://t.me/{bot_username}?start=catalog--channel"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔹 Открыть бота", url=start_link)],
    ])

    try:
        await bot.send_message(
            chat_id=settings.CHANNEL_USERNAME,
            text=text,
            reply_markup=kb,
        )
        await message.answer(f"📢 Полный каталог опубликован в {settings.CHANNEL_USERNAME}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
