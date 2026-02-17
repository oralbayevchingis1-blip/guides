"""Обработчик воронки: выбор гайда -> выдача PDF -> сбор контактов -> согласие."""

import asyncio
import logging
import os
import re

from aiogram import Bot, F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery, FSInputFile, InlineKeyboardButton,
    InlineKeyboardMarkup, Message,
)

from src.bot.keyboards.inline import after_guide_keyboard, categories_keyboard, consent_keyboard, guides_menu_keyboard, main_menu_keyboard, paginated_guides_keyboard, subscription_keyboard, _slugify_cat
from src.bot.utils.cache import TTLCache
from src.bot.utils.compliance import log_consent
from src.bot.utils.disclaimer import add_disclaimer
from src.bot.utils.google_drive import download_guide_pdf
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.monitoring import metrics
from src.bot.utils.scheduler import schedule_followup_series
from src.bot.utils.subscription_check import check_subscription
from src.bot.utils.throttle import critical_limiter
from src.config import settings
from src.constants import get_text
from src.bot.utils.smart_recommendations import smart_recommender
from src.database.crud import count_user_downloads, get_lead_by_user_id, save_lead, track, update_lead_sphere

router = Router()
logger = logging.getLogger(__name__)

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# Домены, которые НЕ являются настоящими email-адресами
_BLOCKED_DOMAINS = {
    "example.com", "example.org", "example.net",
    "test.com", "test.org", "test.net",
    "sample.com", "fake.com", "email.com",
    "domain.com", "yourmail.com", "mail.example",
    "company.kz",
}

# Шаблоны, которые мы сами показываем как примеры — блокируем
_BLOCKED_EMAILS = {
    "name@example.com", "user@example.com", "test@test.com",
    "your.email@gmail.com", "email@email.com",
    "ivan@example.com", "ivanov@example.com",
    "name@company.kz",
}


def _is_fake_email(email: str) -> bool:
    """Проверяет, не является ли email фейковым/тестовым."""
    email_lower = email.lower().strip()
    if email_lower in _BLOCKED_EMAILS:
        return True
    domain = email_lower.split("@")[-1] if "@" in email_lower else ""
    if domain in _BLOCKED_DOMAINS:
        return True
    return False


# ──────────────────────── FSM States ────────────────────────────────────


class LeadForm(StatesGroup):
    """Состояния формы сбора лидов."""

    waiting_for_email = State()
    waiting_for_name = State()
    consent_given = State()
    waiting_for_business_sphere = State()
    waiting_for_profile = State()  # универсальное состояние для profiling 2.0


# ──────────────────────── Вспомогательные ───────────────────────────────


def _find_guide(catalog: list[dict], guide_id: str) -> dict | None:
    """Ищет гайд в каталоге по id."""
    for guide in catalog:
        if str(guide.get("id", "")) == guide_id:
            return guide
    return None


def _esc_html(text: str) -> str:
    """Экранирует спецсимволы HTML."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── Маппинг сфер → категории гайдов ─────────────────────────────────

# ── Кейсы / social proof по сферам ───────────────────────────────────

SPHERE_CASES: dict[str, str] = {
    "it": (
        "Недавно мы помогли IT-стартапу сэкономить 5 млн тенге "
        "на налогах с помощью льготы МФЦА."
    ),
    "финтех": (
        "Один из наших клиентов-финтехов получил лицензию МФЦА "
        "за 3 месяца вместо обычных 6 — благодаря правильной структуре."
    ),
    "строительство": (
        "Мы помогли строительной компании снизить налоговую "
        "нагрузку на 30% через корректную структуру подрядчиков."
    ),
    "ритейл": (
        "Сеть из 40+ магазинов сократила налоговые риски "
        "после нашего аудита — без единого штрафа за 2 года."
    ),
    "производство": (
        "Производственный холдинг оптимизировал трудовые "
        "договоры — текучесть снизилась на 25%."
    ),
    "инвестиции": (
        "Мы сопровождали M&A-сделку на $12M — от due diligence "
        "до закрытия за 4 месяца."
    ),
    "медицина": (
        "Частная клиника прошла лицензирование без замечаний "
        "после нашего юридического сопровождения."
    ),
    "консалтинг": (
        "Консалтинговая фирма выстроила договорную базу "
        "с нуля — 50+ шаблонов под ключ."
    ),
}

# Дефолтный social proof (если сфера не известна)
DEFAULT_SOCIAL_PROOF = (
    "На основе реальных кейсов наших клиентов — "
    "эту схему уже применили 150+ компаний в Казахстане."
)

# Social proof с привязкой к сфере
SPHERE_SOCIAL_PROOF: dict[str, str] = {
    "it": "Эту схему уже применили 80+ IT-компаний в Казахстане.",
    "финтех": "Эти рекомендации используют 40+ финтех-компаний и фондов.",
    "строительство": "Более 50 строительных компаний уже применяют эти подходы.",
    "ритейл": "Эту схему используют 60+ ритейл-компаний в Казахстане.",
    "производство": "Более 45 производственных компаний применяют эти рекомендации.",
    "инвестиции": "Эти подходы проверены на 30+ инвестиционных сделках.",
    "медицина": "Более 25 медицинских организаций прошли лицензирование с нами.",
    "консалтинг": "Эти шаблоны используют 35+ консалтинговых компаний.",
}

DEFAULT_CASE_TEASER = (
    "Кстати, недавно мы помогли бизнесу из Казахстана "
    "решить похожий вопрос — если интересно, пришлём краткое "
    "описание кейса."
)

# ── Счётчики и urgency ─────────────────────────────────────────────────

# Лимит бесплатных консультаций в месяц (для создания дефицита).
# Бот отображает «осталось N слотов», где N = max - записано в этом месяце.
MONTHLY_CONSULT_SLOTS = 10


def _humanize_count(n: int) -> str:
    """Форматирует число для social proof: 23 → '23', 150 → '150+'."""
    if n >= 1000:
        return f"{n // 100 * 100}+"
    if n >= 50:
        return f"{n // 10 * 10}+"
    return str(n)


async def _get_guide_download_line(guide_id: str) -> str:
    """Возвращает строку вида 'Уже 120 предпринимателей использовали...'.

    Если скачиваний мало (< 3) — возвращает пустую строку.
    """
    from src.database.crud import count_guide_downloads

    count = await count_guide_downloads(guide_id)
    if count < 3:
        return ""
    return f"📊 Уже {_humanize_count(count)} предпринимателей использовали эту информацию."


async def _get_consult_scarcity_line() -> str:
    """Возвращает строку с дефицитом: «Осталось N бесплатных слотов».

    Если все слоты заняты, предлагает запись в лист ожидания.
    """
    from src.database.crud import count_consultations_this_month

    booked = await count_consultations_this_month()
    remaining = max(0, MONTHLY_CONSULT_SLOTS - booked)

    if remaining == 0:
        return (
            "⏰ В этом месяце все бесплатные слоты заняты, "
            "но вы можете записаться — мы постараемся найти время."
        )
    if remaining <= 3:
        return (
            f"🔥 В этом месяце у наших юристов осталось всего "
            f"<b>{remaining}</b> слот(-а) для бесплатных консультаций — "
            f"успейте записаться."
        )
    return (
        f"В этом месяце осталось <b>{remaining}</b> бесплатных слотов "
        f"для консультаций."
    )


def _get_freshness_line(guide_info: dict, download_count: int) -> str:
    """Возвращает строку «свежести» гайда.

    Если у гайда есть поле ``is_new`` или ``new`` = true — отдаёт «только вышел».
    Иначе — отдаёт счётчик скачиваний.
    """
    is_new = str(guide_info.get("is_new", guide_info.get("new", ""))).strip().lower()

    if is_new in ("true", "1", "yes", "да"):
        if download_count > 0:
            return (
                f"🆕 Этот гайд только что вышел, и уже "
                f"{_humanize_count(download_count)} человек скачали. "
                f"Получите актуальную информацию первыми."
            )
        return "🆕 Этот гайд совсем новый — будьте среди первых, кто его получит."

    if download_count >= 10:
        return f"📊 Уже {_humanize_count(download_count)} предпринимателей использовали эту информацию."

    return ""


def _get_social_proof(guide_info: dict, sphere: str = "") -> str:
    """Возвращает строку social proof для гайда.

    Приоритет:
    1. Поле social_proof из каталога (Google Sheets)
    2. Proof по сфере («Эту схему применили 80+ IT-компаний»)
    3. Дефолтная фраза
    """
    custom = str(guide_info.get("social_proof", "")).strip()
    if custom:
        return custom

    if sphere:
        norm = _normalize_sphere(sphere)
        sphere_proof = SPHERE_SOCIAL_PROOF.get(norm)
        if sphere_proof:
            return sphere_proof

    return DEFAULT_SOCIAL_PROOF


def _get_case_teaser(sphere: str = "") -> str:
    """Возвращает короткий тизер кейса для post-download."""
    if sphere:
        norm = _normalize_sphere(sphere)
        case = SPHERE_CASES.get(norm)
        if case:
            return (
                f"{case}\n\n"
                "Если интересно — могу прислать краткое описание кейса."
            )
    return DEFAULT_CASE_TEASER


def _has_case_for_sphere(sphere: str) -> bool:
    """Проверяет, есть ли кейс для данной сферы."""
    if not sphere:
        return False
    return _normalize_sphere(sphere) in SPHERE_CASES


SPHERE_CATEGORIES: dict[str, list[str]] = {
    "it": ["it", "ит", "технолог", "ip", "ai", "ии"],
    "финтех": ["инвестиции", "финтех", "investment", "tax", "налог"],
    "строительство": ["труд", "labor", "корпоратив", "corporate"],
    "ритейл": ["налог", "tax", "труд", "labor"],
    "производство": ["труд", "labor", "корпоратив", "corporate", "налог"],
    "инвестиции": ["инвестиции", "investment", "m&a", "слиян", "мфца", "aifc"],
    "медицина": ["труд", "labor", "корпоратив", "ip"],
    "консалтинг": ["корпоратив", "corporate", "налог", "tax"],
    "образование": ["труд", "labor", "ip", "интеллектуальная"],
}


def _normalize_sphere(sphere: str) -> str:
    """Нормализует введённую сферу к ключу из SPHERE_CATEGORIES."""
    low = sphere.lower().strip()
    for key in SPHERE_CATEGORIES:
        if key in low or low in key:
            return key
    return low


def _find_guide_by_sphere(
    catalog: list[dict],
    sphere: str | None,
    exclude_ids: set[str] | None = None,
    downloaded: set[str] | None = None,
) -> dict | None:
    """Подбирает гайд по сфере бизнеса, исключая уже скачанные."""
    if not sphere:
        return None

    norm = _normalize_sphere(sphere)
    target_cats = SPHERE_CATEGORIES.get(norm, [])
    if not target_cats:
        return None

    exclude = (exclude_ids or set()) | (downloaded or set())

    for guide in catalog:
        gid = str(guide.get("id", ""))
        if gid in exclude:
            continue
        cat = guide.get("category", "").lower()
        for tag in target_cats:
            if tag in cat:
                return guide
    return None


async def _get_downloaded_set(user_id: int) -> set[str]:
    """Возвращает множество guide_id, скачанных пользователем."""
    from src.database.crud import get_user_downloaded_guides
    guides = await get_user_downloaded_guides(user_id)
    return set(guides)


def _sphere_keyboard() -> InlineKeyboardMarkup:
    """Inline-клавиатура выбора сферы бизнеса."""
    spheres = [
        ("💻 IT / Технологии", "IT"),
        ("🏗 Строительство", "Строительство"),
        ("🛒 Ритейл / Торговля", "Ритейл"),
        ("💰 Инвестиции / Финансы", "Инвестиции"),
        ("🏭 Производство", "Производство"),
        ("🏥 Медицина", "Медицина"),
        ("📊 Консалтинг", "Консалтинг"),
    ]
    rows = []
    for i in range(0, len(spheres), 2):
        row = []
        for label, value in spheres[i:i+2]:
            row.append(InlineKeyboardButton(
                text=label,
                callback_data=f"sphere_{value}",
            ))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⏭ Пропустить", callback_data="sphere_skip")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ──────────────────────── Превью гайда (шаг 1) ──────────────────────────


@router.callback_query(F.data.startswith("guide_"))
async def show_guide_preview(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Показывает карточку-превью гайда: название, описание, 'что внутри'."""
    guide_id = callback.data.removeprefix("guide_")
    asyncio.create_task(track(callback.from_user.id, "view_guide", guide_id=guide_id))

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)

    guide_info = _find_guide(catalog, guide_id)

    if guide_info is None:
        await callback.answer(get_text(texts, "guide_not_found"), show_alert=True)
        return

    await state.update_data(selected_guide=guide_id)
    await callback.answer()

    guide_title = guide_info.get("title", guide_id)
    guide_desc = guide_info.get("description", "")
    preview = guide_info.get("preview", "") or guide_info.get("preview_text", "")
    pages = str(guide_info.get("pages", "")).strip()
    category = guide_info.get("category", "")

    # Определяем сферу для персонализации social proof
    user_id = callback.from_user.id
    lead = await get_lead_by_user_id(user_id)
    sphere = getattr(lead, "business_sphere", None) or "" if lead else ""

    # Счётчик скачиваний из БД
    from src.database.crud import count_guide_downloads
    dl_count = await count_guide_downloads(guide_id)

    # ── Формируем карточку ────────────────────────────────────────────
    card_parts = [f"🔹 <b>{_esc_html(guide_title)}</b>"]

    if guide_desc:
        card_parts.append(f"\n{_esc_html(guide_desc)}")

    # Метаданные: категория, объём
    meta_items = []
    if category:
        meta_items.append(_esc_html(category))
    if pages:
        meta_items.append(f"{_esc_html(pages)} стр.")
    meta_items.append("PDF")
    if meta_items:
        card_parts.append("\n" + "  ·  ".join(meta_items))

    # Что внутри
    if preview:
        card_parts.append(f"\n🔹 <b>Что внутри:</b>\n{_esc_html(preview)}")

    # Freshness / download counter (динамический)
    freshness = _get_freshness_line(guide_info, dl_count)
    if freshness:
        card_parts.append(f"\n{freshness}")

    # Social proof (статический / сфера)
    proof = _get_social_proof(guide_info, sphere)
    card_parts.append(f"\n<i>{_esc_html(proof)}</i>")

    card_text = "\n".join(card_parts)

    dl_data = f"download_{guide_id}"
    while len(dl_data.encode("utf-8")) > 64:
        dl_data = dl_data[:-1]

    # Кнопка «Назад» ведёт к категории, из которой пришли, или к списку категорий
    fsm_data = await state.get_data()
    current_cat = fsm_data.get("current_category")
    if current_cat:
        back_cb = f"cat_{current_cat}"
        back_text = "⬅️ Назад к категории"
    else:
        back_cb = "show_categories"
        back_text = "⬅️ Назад к темам"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔹 Скачать гайд", callback_data=dl_data)],
        [InlineKeyboardButton(text=back_text, callback_data=back_cb)],
    ])

    await callback.message.answer(card_text, reply_markup=kb)


# ──────────────────────── Скачивание гайда (шаг 2) ───────────────────────


@router.callback_query(F.data.startswith("download_"))
async def process_guide_download(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Отправляет PDF после превью и начинает сбор данных.

    Value-first flow: барьеры (подписка + email) проверяются
    именно здесь, а не при /start.
    """
    if not critical_limiter.allow(callback.from_user.id, "download"):
        await callback.answer("⏳ Подождите минуту перед следующим скачиванием.", show_alert=True)
        return

    guide_id = callback.data.removeprefix("download_")
    user_id = callback.from_user.id
    fsm_data = await state.get_data()
    _src = fsm_data.get("traffic_source", "")

    asyncio.create_task(track(user_id, "click_download", guide_id=guide_id, source=_src or None))

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)

    guide_info = _find_guide(catalog, guide_id)

    if guide_info is None:
        await callback.answer(get_text(texts, "guide_not_found"), show_alert=True)
        return

    await state.update_data(selected_guide=guide_id, pending_guide=guide_id)

    # ── Барьер 1: подписка на канал ───────────────────────────────────
    is_subscribed = await check_subscription(user_id, bot)
    if not is_subscribed:
        asyncio.create_task(track(user_id, "sub_prompt", guide_id=guide_id))
        guide_title = guide_info.get("title", guide_id)
        channel = settings.CHANNEL_USERNAME
        await callback.answer()
        await callback.message.answer(
            f"📚 Вы на шаг от получения гайда «{guide_title}»!\n\n"
            f"В нашем канале {channel} мы публикуем обновления "
            "законов, судебную практику и анонсы новых гайдов.\n\n"
            "Подпишитесь — и будете в курсе изменений, "
            "которые касаются вашего бизнеса 👇",
            reply_markup=subscription_keyboard(),
        )
        return

    # ── Барьер 2: email (регистрация) ─────────────────────────────────
    existing_lead = await get_lead_by_user_id(user_id)
    if not existing_lead:
        asyncio.create_task(track(user_id, "email_prompt", guide_id=guide_id))
        await callback.answer()
        await callback.message.answer(
            "📚 <b>Почти готово!</b>\n\n"
            "Укажите email — на него придёт:\n"
            "• ссылка на гайд (чтобы не потерять)\n"
            "• уведомления о новых материалах по вашей теме\n\n"
            "Спама не будет, отписаться — 1 клик в любом письме.\n\n"
            "💡 Например: <code>name@company.kz</code>"
        )
        await state.set_state(LeadForm.waiting_for_email)
        return

    # ── Оба барьера пройдены — доставляем PDF ─────────────────────────
    await callback.answer()

    # Получаем PDF
    file_id = guide_info.get("drive_file_id", "")
    local_path = None
    telegram_file_id = None

    if file_id.startswith("local:"):
        local_guide_id = file_id.removeprefix("local:")
        local_candidate = os.path.join("data", "guides", f"{local_guide_id}.pdf")
        if os.path.isfile(local_candidate):
            local_path = local_candidate
        else:
            mapping_path = os.path.join("data", "guides", "telegram_files.json")
            if os.path.isfile(mapping_path):
                import json as _json
                with open(mapping_path, "r", encoding="utf-8") as f:
                    mapping = _json.load(f)
                entry = mapping.get(local_guide_id, {})
                telegram_file_id = entry.get("file_id")
    elif file_id:
        local_path = await download_guide_pdf(file_id)

    guide_title = guide_info.get("title", guide_id)
    guide_desc = guide_info.get("description", "")
    caption = (
        f"📚 <b>{_esc_html(guide_title)}</b>\n\n"
        f"{_esc_html(guide_desc)}\n\n"
        "Сохраните файл — он пригодится при принятии решений."
    )
    if len(caption) > 1024:
        caption = caption[:1020] + "..."

    pdf_sent = False
    if telegram_file_id:
        await callback.message.answer_document(document=telegram_file_id, caption=caption)
        metrics.inc("pdf_delivered")
        asyncio.create_task(track(user_id, "pdf_delivered", guide_id=guide_id, source=_src or None))
        pdf_sent = True
    elif local_path:
        document = FSInputFile(local_path)
        await callback.message.answer_document(document=document, caption=caption)
        metrics.inc("pdf_delivered")
        asyncio.create_task(track(user_id, "pdf_delivered", guide_id=guide_id, source=_src or None))
        pdf_sent = True
    else:
        await callback.message.answer(
            get_text(texts, "guide_pdf_unavailable", title=guide_title, description=guide_desc),
        )
        metrics.inc_error("pdf_unavailable")
        logger.warning("PDF не доступен для гайда '%s' (drive_file_id='%s')", guide_id, file_id)

    # existing_lead уже проверен выше (барьер 2) — перечитываем на случай race condition
    existing_lead = await get_lead_by_user_id(user_id)

    if existing_lead:
        username = callback.from_user.username or ""
        data = await state.get_data()
        traffic_source = data.get("traffic_source", "")

        asyncio.create_task(
            google.append_lead(
                user_id=user_id,
                username=username,
                name=existing_lead.name,
                email=existing_lead.email,
                guide=guide_id,
                source=traffic_source,
                sphere=getattr(existing_lead, "business_sphere", "") or "",
            )
        )

        asyncio.create_task(schedule_followup_series(user_id, guide_id))

        logger.info("Пользователь user_id=%s скачал '%s'", user_id, guide_id)

        # Progressive profiling 2.0: задаём по одному вопросу за визит
        download_count = await count_user_downloads(user_id)
        has_sphere = bool(getattr(existing_lead, "business_sphere", None))

        from src.bot.utils.profiling import get_next_question, build_question_keyboard
        next_q = await get_next_question(user_id, download_count)
        if next_q:
            await callback.message.answer(
                f"<b>{existing_lead.name}</b>, вы скачали уже "
                f"{download_count} гайда — отлично!\n\n"
                f"{next_q.prompt}",
                reply_markup=build_question_keyboard(next_q),
            )
            await state.update_data(profiling_user_id=user_id, profiling_field=next_q.field)
            await state.set_state(LeadForm.waiting_for_profile)
            return

        # ── Единое сообщение «Что дальше» ─────────────────────────────
        sphere = getattr(existing_lead, "business_sphere", None) or ""
        name = existing_lead.name

        # Подбираем следующий гайд (collab → сфера → Sheets → любой)
        downloaded_set = await _get_downloaded_set(user_id)
        exclude = downloaded_set | {guide_id}
        rec_source = ""

        # 1. Коллаборативная фильтрация: «часто скачивают вместе»
        next_gid = await smart_recommender.get_recommendation(guide_id, exclude=exclude)
        next_guide = _find_guide(catalog, next_gid) if next_gid else None
        if next_guide:
            rec_source = "collab"

        # 2. По сфере бизнеса (приоритет выше статического маппинга)
        if not next_guide and has_sphere:
            next_guide = _find_guide_by_sphere(
                catalog, existing_lead.business_sphere, exclude_ids=exclude,
                downloaded=downloaded_set,
            )
            if next_guide:
                next_gid = next_guide.get("id", "")
                rec_source = "sphere"

        # 3. Статический маппинг из листа «Рекомендации»
        recommendations = await cache.get_or_fetch("recommendations", google.get_recommendations)
        rec = recommendations.get(guide_id, {})
        next_article = rec.get("next_article_link", "")
        if not next_guide:
            sheet_gid = rec.get("next_guide_id", "")
            next_guide = _find_guide(catalog, sheet_gid) if sheet_gid else None
            if next_guide:
                next_gid = sheet_gid
                rec_source = "sheets"

        # 4. Любой не скачанный
        if not next_guide:
            for g in catalog:
                gid = str(g.get("id", ""))
                if gid and gid not in exclude:
                    next_guide = g
                    next_gid = gid
                    rec_source = "fallback"
                    break

        # Формируем текст
        case_text = _get_case_teaser(sphere)
        parts = [f"✅ <b>{_esc_html(name)}</b>, гайд у вас — сохраните!"]
        parts.append(f"\n💼 {case_text}")

        if next_guide:
            next_title = next_guide.get("title", next_gid)
            if rec_source == "sphere" and sphere:
                parts.append(
                    f"\n📚 <b>Для {_esc_html(sphere)}-бизнеса рекомендуем:</b> "
                    f"«{_esc_html(next_title)}»"
                )
            elif has_sphere and sphere:
                parts.append(
                    f"\n📚 <b>Рекомендуем далее:</b> «{_esc_html(next_title)}» "
                    f"(для сферы «{_esc_html(sphere)}»)"
                )
            else:
                parts.append(
                    f"\n📚 <b>Рекомендуем далее:</b> «{_esc_html(next_title)}»"
                )

        # Scarcity консультаций
        scarcity = await _get_consult_scarcity_line()
        if scarcity:
            parts.append(f"\n{scarcity}")

        whats_next_text = "\n".join(parts)

        # Формируем кнопки
        buttons = []

        # Кнопка «Да, пришли кейс» — если есть кейс по сфере
        if _has_case_for_sphere(sphere):
            buttons.append([InlineKeyboardButton(
                text="💼 Да, пришли кейс",
                callback_data=f"send_case_{_normalize_sphere(sphere)}",
            )])

        if next_guide:
            cb = f"guide_{next_gid}"
            while len(cb.encode("utf-8")) > 64:
                cb = cb[:-1]
            buttons.append([InlineKeyboardButton(
                text=f"📥 {next_guide.get('title', 'Следующий гайд')[:40]}",
                callback_data=cb,
            )])

        if next_article:
            buttons.append([InlineKeyboardButton(
                text="📰 Читать кейс по теме",
                url=next_article,
            )])

        buttons.append([InlineKeyboardButton(
            text="🔹 Все темы",
            callback_data="show_categories",
        )])
        buttons.append([InlineKeyboardButton(
            text="🔹 Бесплатная консультация",
            callback_data="book_consultation",
        )])

        if user_id:
            buttons.append([InlineKeyboardButton(
                text="🔗 Отправить другу",
                callback_data=f"share_bot_{user_id}",
            )])

        if pdf_sent:
            await callback.message.answer(
                whats_next_text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            )

        await state.clear()
        return

    # Этот код не должен достигаться — барьер 2 выше уже
    # перенаправляет незарегистрированных пользователей на сбор email.
    logger.warning("Unexpected: download path reached without lead for user %s", user_id)


# ──────────────────── Прогрессивное профилирование ─────────────────────


async def _save_sphere(
    user_id: int,
    sphere: str,
    google: GoogleSheetsClient,
) -> None:
    """Сохраняет сферу бизнеса в SQLite и Google Sheets."""
    await update_lead_sphere(user_id, sphere)
    asyncio.create_task(google.update_lead_sphere(user_id, sphere))
    metrics.inc("sphere_collected")
    logger.info("Sphere saved: user=%s sphere='%s'", user_id, sphere[:50])


@router.callback_query(F.data.startswith("sphere_"), LeadForm.waiting_for_business_sphere)
async def process_sphere_button(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Обработка выбора сферы через inline-кнопку."""
    value = callback.data.removeprefix("sphere_")
    data = await state.get_data()
    user_id = data.get("profiling_user_id", callback.from_user.id)

    await callback.answer()

    if value == "skip":
        await callback.message.edit_text(
            "Хорошо, пропускаем. Вы всегда сможете уточнить позже.",
        )
        await callback.message.answer(
            "📚 Что дальше?",
            reply_markup=after_guide_keyboard(user_id),
        )
    else:
        await _save_sphere(user_id, value, google)

        # Рекомендуем гайд по сфере
        catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
        downloaded = await _get_downloaded_set(user_id)
        rec_guide = _find_guide_by_sphere(catalog, value, downloaded=downloaded)

        rec_text = ""
        kb = after_guide_keyboard(user_id)
        if rec_guide:
            rec_title = rec_guide.get("title", "")
            rec_id = rec_guide.get("id", "")
            rec_text = f"\n\n💡 Для сферы «{_esc_html(value)}» рекомендуем:\n📚 <b>{_esc_html(rec_title)}</b>"
            dl_data = f"guide_{rec_id}"
            while len(dl_data.encode("utf-8")) > 64:
                dl_data = dl_data[:-1]
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"📥 {rec_title}"[:55], callback_data=dl_data)],
                [InlineKeyboardButton(text="🔹 Все темы", callback_data="show_categories")],
                [InlineKeyboardButton(text="🔹 Консультация", callback_data="book_consultation")],
            ])

        await callback.message.edit_text(
            f"Отлично, запомнили: <b>{_esc_html(value)}</b>. "
            f"Буду подбирать материалы для вашей сферы.{rec_text}",
        )
        await callback.message.answer("👇", reply_markup=kb)

    await state.clear()


@router.message(LeadForm.waiting_for_business_sphere)
async def process_business_sphere(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Сохраняет бизнес-сферу (текстовый ввод)."""
    text = (message.text or "").strip()

    if text.startswith("/"):
        await state.clear()
        return

    data = await state.get_data()
    user_id = data.get("profiling_user_id", message.from_user.id)

    if text == "-" or len(text) < 2:
        await message.answer(
            "Хорошо, пропускаем. Вы всегда сможете уточнить позже.",
            reply_markup=after_guide_keyboard(user_id),
        )
        await state.clear()
        return

    sphere = text[:100]
    await _save_sphere(user_id, sphere, google)

    # Рекомендуем гайд по сфере
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    downloaded = await _get_downloaded_set(user_id)
    rec_guide = _find_guide_by_sphere(catalog, sphere, downloaded=downloaded)

    rec_text = ""
    kb = after_guide_keyboard(user_id)
    if rec_guide:
        rec_title = rec_guide.get("title", "")
        rec_id = rec_guide.get("id", "")
        rec_text = f"\n\n💡 Для вашей сферы рекомендуем:\n📚 <b>{_esc_html(rec_title)}</b>"
        dl_data = f"guide_{rec_id}"
        while len(dl_data.encode("utf-8")) > 64:
            dl_data = dl_data[:-1]
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"📥 {rec_title}"[:55], callback_data=dl_data)],
            [InlineKeyboardButton(text="🔹 Все темы", callback_data="show_categories")],
            [InlineKeyboardButton(text="🔹 Консультация", callback_data="book_consultation")],
        ])

    await message.answer(
        f"Спасибо! Запомнили: <b>{_esc_html(sphere)}</b>. "
        f"Буду подбирать материалы для вашей сферы.{rec_text}",
        reply_markup=kb,
    )
    await state.clear()


# ──────────── Прогрессивное профилирование 2.0 (универсальное) ──────────


@router.callback_query(F.data.startswith("profile_"), LeadForm.waiting_for_profile)
async def process_profile_button(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
) -> None:
    """Обработка выбора варианта в профильном вопросе."""
    from src.database.crud import update_user_profile

    raw = callback.data.removeprefix("profile_")
    data = await state.get_data()
    user_id = data.get("profiling_user_id", callback.from_user.id)
    field = data.get("profiling_field", "")

    # Разбираем callback: profile_{field}_{value}
    # field может содержать '_', поэтому берём из FSM data
    value = raw.removeprefix(f"{field}_") if raw.startswith(f"{field}_") else raw

    await callback.answer()

    if value == "skip":
        # Проверяем, обязательное ли поле
        from src.bot.utils.profiling import PROFILE_QUESTIONS
        is_required = any(
            q.field == field and q.required for q in PROFILE_QUESTIONS
        )
        if is_required:
            await callback.message.answer(
                "Пожалуйста, выберите сферу — это поможет нам "
                "подобрать наиболее полезные материалы для вас.",
            )
            return

        await callback.message.edit_text(
            "Хорошо, пропускаем. Вы всегда сможете уточнить позже.",
        )
        await callback.message.answer("📚 Что дальше?", reply_markup=after_guide_keyboard(user_id))
        await state.clear()
        return

    # Сохраняем в User
    await update_user_profile(user_id, **{field: value})
    metrics.inc(f"profile_{field}_collected")

    # Для business_sphere — также обновляем Lead и Sheets
    if field == "business_sphere":
        await _save_sphere(user_id, value, google)

    await callback.message.edit_text(
        f"Отлично, запомнили! Спасибо.",
    )
    await callback.message.answer("📚 Что дальше?", reply_markup=after_guide_keyboard(user_id))
    await state.clear()


@router.message(LeadForm.waiting_for_profile)
async def process_profile_text(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
) -> None:
    """Текстовый ввод для профильного вопроса."""
    from src.database.crud import update_user_profile

    text = (message.text or "").strip()
    if text.startswith("/"):
        await state.clear()
        return

    data = await state.get_data()
    user_id = data.get("profiling_user_id", message.from_user.id)
    field = data.get("profiling_field", "")

    if text == "-" or len(text) < 2:
        await message.answer(
            "Хорошо, пропускаем. Вы всегда сможете уточнить позже.",
            reply_markup=after_guide_keyboard(user_id),
        )
        await state.clear()
        return

    value = text[:100]
    await update_user_profile(user_id, **{field: value})
    metrics.inc(f"profile_{field}_collected")

    if field == "business_sphere":
        await _save_sphere(user_id, value, google)

    await message.answer(
        f"Спасибо! Запомнили: <b>{_esc_html(value)}</b>.",
        reply_markup=after_guide_keyboard(user_id),
    )
    await state.clear()


# ──────────────────────── Сбор email ────────────────────────────────────


@router.message(LeadForm.waiting_for_email)
async def process_email(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Валидация и сохранение email."""
    if not critical_limiter.allow(message.from_user.id, "email"):
        await message.answer("⏳ Слишком много попыток. Подождите минуту.")
        return

    texts = await cache.get_or_fetch("texts", google.get_bot_texts)

    if not message.text:
        await message.answer(
            "Пожалуйста, введите email <b>текстом</b>.\n\n"
            "Пример: <code>name@company.kz</code>"
        )
        return

    email = message.text.strip()

    if email.startswith("/"):
        await state.clear()
        return

    if not EMAIL_REGEX.match(email):
        metrics.inc("email_invalid")
        await message.answer(
            "Пожалуйста, введите корректный email.\n"
            "Пример: <code>name@company.kz</code>"
        )
        return

    if _is_fake_email(email):
        metrics.inc("email_fake_blocked")
        await message.answer(
            "Похоже, это тестовый адрес.\n\n"
            "Укажите <b>ваш настоящий рабочий email</b> — "
            "на него придёт ссылка на PDF.\n\n"
            "Пример: <code>name@company.kz</code>"
        )
        return

    metrics.inc("email_collected")
    asyncio.create_task(track(message.from_user.id, "email_submitted"))
    await state.update_data(email=email)
    await message.answer(get_text(texts, "email_saved"))
    await state.set_state(LeadForm.waiting_for_name)


# ──────────────────────── Сбор имени ────────────────────────────────────


@router.message(LeadForm.waiting_for_name)
async def process_name(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Сохранение имени и запрос согласия."""
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)

    if not message.text:
        await message.answer(
            "Пожалуйста, введите имя <b>текстом</b>.\n\n"
            'Например: <b>Айдар Муратович</b>'
        )
        return

    name = message.text.strip()

    if name.startswith("/"):
        await state.clear()
        return

    if len(name) < 2 or name.isdigit() or not any(c.isalpha() for c in name):
        await message.answer(
            "Имя должно содержать хотя бы одну букву.\n"
            'Укажите, как к вам обращаться (например, <b>Айдар Муратович</b>).'
        )
        return

    await state.update_data(name=name)
    await message.answer(
        get_text(texts, "consent_text", privacy_url=settings.PRIVACY_POLICY_URL),
        reply_markup=consent_keyboard(),
        disable_web_page_preview=True,
    )
    await state.set_state(LeadForm.consent_given)


# ──────────────────────── Согласие ──────────────────────────────────────


@router.callback_query(F.data == "give_consent", LeadForm.consent_given)
async def process_consent(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Пользователь дал согласие — сохраняем лид."""
    data = await state.get_data()
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)

    user_id = callback.from_user.id
    username = callback.from_user.username or ""
    email = data.get("email", "")
    name = data.get("name", "")
    selected_guide = data.get("selected_guide", "")
    traffic_source = data.get("traffic_source", "")

    # Воронка: согласие
    asyncio.create_task(track(user_id, "consent_given", guide_id=selected_guide, source=traffic_source or None))

    # 1. SQLite
    await save_lead(
        user_id=user_id,
        email=email,
        name=name,
        selected_guide=selected_guide,
        traffic_source=traffic_source or None,
    )
    metrics.inc("leads_saved")

    # 2. Google Sheets (sphere пока пустая — заполнится через profiling)
    asyncio.create_task(
        google.append_lead(
            user_id=user_id,
            username=username,
            name=name,
            email=email,
            guide=selected_guide,
            source=traffic_source,
        )
    )

    # 3. Compliance
    await log_consent(user_id=user_id, consent_type="personal_data_processing")

    logger.info(
        "Новый лид: user_id=%s, email=%s, name=%s, guide=%s",
        user_id, email, name, selected_guide,
    )

    # 4. Follow-up серия (задачи сохраняются в БД — переживают рестарт)
    if selected_guide:
        asyncio.create_task(schedule_followup_series(user_id, selected_guide))

    # 5. Благодарим + показываем постоянное меню
    await callback.message.edit_text(
        get_text(texts, "consent_given", name=name, email=email),
    )
    await callback.message.answer("⚙️", reply_markup=main_menu_keyboard())

    # 5a. Если есть pending_guide — автоматически выдаём гайд
    pending_guide = data.get("pending_guide")
    if pending_guide:
        catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
        pg_info = _find_guide(catalog, pending_guide)
        if pg_info:
            pg_id = pg_info.get("id", pending_guide)
            dl_data = f"download_{pg_id}"
            while len(dl_data.encode("utf-8")) > 64:
                dl_data = dl_data[:-1]
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"📥 Получить: {pg_info['title']}",
                    callback_data=dl_data,
                )],
                [InlineKeyboardButton(
                    text="🔹 Все темы", callback_data="show_categories",
                )],
            ])
            await callback.message.answer(
                f"📚 <b>{pg_info['title']}</b>\n\n"
                f"{pg_info.get('description', '')}\n\n"
                "Нажмите кнопку ниже, чтобы получить гайд:",
                reply_markup=kb,
            )
        else:
            await callback.message.answer(
                "📚 Хотите посмотреть другие полезные материалы?",
                reply_markup=after_guide_keyboard(user_id),
            )
    else:
        # Если пользователь прошёл регистрацию без выбора гайда — показываем каталог
        if not selected_guide:
            catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
            await callback.message.answer(
                "🎉 <b>Отлично!</b> Теперь выберите тему, которая вам интересна:",
                reply_markup=categories_keyboard(catalog),
            )
        else:
            await callback.message.answer(
                "📚 Хотите посмотреть другие полезные материалы?",
                reply_markup=after_guide_keyboard(user_id),
            )

    # 6. Уведомляем админа
    asyncio.create_task(
        notify_admin(
            bot,
            user_id=user_id,
            username=username,
            name=name,
            email=email,
            guide=selected_guide,
            source=traffic_source,
        )
    )

    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "decline_consent", LeadForm.consent_given)
async def process_decline(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Пользователь отказался от обработки данных."""
    texts = await cache.get_or_fetch("texts", google.get_bot_texts)
    await callback.message.edit_text(get_text(texts, "consent_declined"))
    await state.clear()
    await callback.answer()


# ──────────────────────── Кнопка «Все гайды» / Категории ─────────────────


@router.callback_query(F.data == "show_categories")
async def show_categories(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Показывает список категорий."""
    asyncio.create_task(track(callback.from_user.id, "view_categories"))

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    if not catalog:
        await callback.answer("Каталог пуст.", show_alert=True)
        return

    await callback.message.answer(
        "📚 <b>Выберите тему:</b>",
        reply_markup=categories_keyboard(catalog),
    )
    await callback.answer()


@router.callback_query(F.data == "show_all_guides")
async def show_all_guides(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Показывает все гайды — первая страница с пагинацией (по 3 гайда)."""
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    if not catalog:
        await callback.answer("Каталог пуст. Попробуйте позже.", show_alert=True)
        return

    await callback.message.answer(
        "📚 <b>Все гайды:</b>\n\n<i>Листайте кнопками ◀️ / ▶️</i>",
        reply_markup=paginated_guides_keyboard(catalog, page=0, prefix="gpage"),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("gpage_"))
async def navigate_all_guides(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Переключает страницу в списке всех гайдов."""
    page = int(callback.data.removeprefix("gpage_"))
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    if not catalog:
        await callback.answer("Каталог пуст.", show_alert=True)
        return

    try:
        await callback.message.edit_reply_markup(
            reply_markup=paginated_guides_keyboard(catalog, page=page, prefix="gpage"),
        )
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data == "noop")
async def noop_handler(callback: CallbackQuery) -> None:
    """Заглушка для кнопки-счётчика страниц."""
    await callback.answer()


@router.callback_query(F.data.startswith("cat_"))
async def show_category_guides(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Показывает гайды внутри выбранной категории (с пагинацией)."""
    cat_slug = callback.data.removeprefix("cat_")
    asyncio.create_task(track(callback.from_user.id, "view_category", meta=cat_slug))

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    filtered = [
        g for g in catalog
        if _slugify_cat(g.get("category", "")) == cat_slug
    ]

    if not filtered:
        await callback.answer("В этой категории пока нет гайдов.", show_alert=True)
        return

    await callback.answer()

    cat_name = filtered[0].get("category", "Гайды")
    await state.update_data(current_category=cat_slug)

    prefix = f"cpage_{cat_slug}"
    await callback.message.answer(
        f"📂 <b>{cat_name}</b>\n\nВыберите гайд:\n"
        f"<i>Листайте кнопками ◀️ / ▶️</i>",
        reply_markup=paginated_guides_keyboard(
            filtered, page=0,
            prefix=prefix,
            back_cb="show_categories",
            back_text="⬅️ Назад к темам",
        ),
    )


@router.callback_query(F.data.startswith("cpage_"))
async def navigate_category_guides(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Переключает страницу внутри категории."""
    raw = callback.data.removeprefix("cpage_")
    parts = raw.rsplit("_", 1)
    if len(parts) != 2:
        await callback.answer()
        return

    cat_slug, page_str = parts
    page = int(page_str)

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    filtered = [
        g for g in catalog
        if _slugify_cat(g.get("category", "")) == cat_slug
    ]

    if not filtered:
        await callback.answer("Нет гайдов.", show_alert=True)
        return

    prefix = f"cpage_{cat_slug}"
    try:
        await callback.message.edit_reply_markup(
            reply_markup=paginated_guides_keyboard(
                filtered, page=page,
                prefix=prefix,
                back_cb="show_categories",
                back_text="⬅️ Назад к темам",
            ),
        )
    except Exception:
        pass
    await callback.answer()


# ──────────────────────── Кнопка «Поделиться» ─────────────────────────────


# ──────────────────────── Кейс по сфере ────────────────────────────────

# Подробные кейсы по сферам (отправляются по кнопке «Да, пришли кейс»)
SPHERE_CASE_DETAILS: dict[str, str] = {
    "it": (
        "💼 <b>Кейс: IT-стартап и льгота МФЦА</b>\n\n"
        "IT-компания из Алматы (20 сотрудников, SaaS-продукт) "
        "планировала расширение и искала пути оптимизации налогов.\n\n"
        "<b>Проблема:</b> Стандартная ставка КПН 20% и социальный налог "
        "съедали значительную часть прибыли.\n\n"
        "<b>Что сделали:</b>\n"
        "— Зарегистрировали компанию как участника МФЦА\n"
        "— Оформили освобождение от КПН на 50 лет\n"
        "— Перевели ключевых сотрудников на контракты МФЦА\n\n"
        "<b>Результат:</b> Экономия 5 млн тенге в первый год, "
        "упрощённый найм иностранных специалистов.\n\n"
        "📞 Хотите узнать, подходит ли МФЦА для вашего бизнеса?"
    ),
    "финтех": (
        "💼 <b>Кейс: Финтех-компания и лицензия МФЦА</b>\n\n"
        "Платёжный сервис из Казахстана получал отказы от НБ РК "
        "и обратился к нам за лицензированием через МФЦА.\n\n"
        "<b>Проблема:</b> Длительная процедура (обычно 6-9 месяцев), "
        "неясные требования регулятора.\n\n"
        "<b>Что сделали:</b>\n"
        "— Подготовили полный пакет документов\n"
        "— Провели pre-consultation с AFSA\n"
        "— Выстроили комплаенс-систему под требования\n\n"
        "<b>Результат:</b> Лицензия получена за 3 месяца.\n\n"
        "📞 Планируете лицензирование? Обсудим вашу ситуацию."
    ),
    "строительство": (
        "💼 <b>Кейс: Строительная компания и налоговая оптимизация</b>\n\n"
        "Строительный холдинг с 5 проектами искал способ "
        "снизить налоговую нагрузку без рисков.\n\n"
        "<b>Проблема:</b> Высокая налоговая нагрузка через прямые контракты, "
        "риски при работе с субподрядчиками.\n\n"
        "<b>Что сделали:</b>\n"
        "— Провели аудит договорной базы\n"
        "— Реструктурировали цепочку подрядчиков\n"
        "— Внедрили типовые договоры с защитой от налоговых рисков\n\n"
        "<b>Результат:</b> Снижение нагрузки на 30%, ноль претензий от НК.\n\n"
        "📞 Хотите аудит вашей структуры?"
    ),
    "ритейл": (
        "💼 <b>Кейс: Розничная сеть и налоговая безопасность</b>\n\n"
        "Сеть из 40+ точек продаж прошла выездную проверку "
        "без единого штрафа — после нашего аудита.\n\n"
        "<b>Что сделали:</b>\n"
        "— Аудит всех договоров с поставщиками\n"
        "— Приведение кассовой дисциплины в соответствие\n"
        "— Обучение бухгалтерии по новым требованиям\n\n"
        "<b>Результат:</b> 0 штрафов за 2 года, экономия 8 млн тенге.\n\n"
        "📞 Готовитесь к проверке? Поможем подготовиться."
    ),
    "производство": (
        "💼 <b>Кейс: Производственный холдинг и трудовое право</b>\n\n"
        "Компания с 200+ сотрудниками столкнулась с высокой "
        "текучестью и трудовыми спорами.\n\n"
        "<b>Что сделали:</b>\n"
        "— Полный аудит трудовых договоров\n"
        "— Разработка системы KPI с юридической защитой\n"
        "— Типовые документы при увольнении\n\n"
        "<b>Результат:</b> Текучесть снизилась на 25%, "
        "ноль проигранных трудовых споров.\n\n"
        "📞 Есть вопросы по трудовому праву? Обсудим."
    ),
    "инвестиции": (
        "💼 <b>Кейс: M&A-сделка на $12M</b>\n\n"
        "Инвестиционный фонд приобретал долю в казахстанской "
        "компании и нуждался в полном юридическом сопровождении.\n\n"
        "<b>Что сделали:</b>\n"
        "— Due diligence за 6 недель\n"
        "— Структурирование сделки (holdco + opco)\n"
        "— Переговоры и закрытие\n\n"
        "<b>Результат:</b> Сделка закрыта за 4 месяца, "
        "все риски застрахованы.\n\n"
        "📞 Планируете сделку? Обсудим структуру."
    ),
    "медицина": (
        "💼 <b>Кейс: Частная клиника и лицензирование</b>\n\n"
        "Клиника из Астаны проходила лицензирование "
        "и столкнулась с замечаниями КМФК.\n\n"
        "<b>Что сделали:</b>\n"
        "— Подготовка полного пакета документов\n"
        "— Устранение замечаний за 2 недели\n"
        "— Сопровождение до получения лицензии\n\n"
        "<b>Результат:</b> Лицензия получена без повторной проверки.\n\n"
        "📞 Нужна помощь с лицензированием?"
    ),
    "консалтинг": (
        "💼 <b>Кейс: Консалтинговая фирма и договорная база</b>\n\n"
        "Фирма с 15 консультантами работала без стандартных "
        "договоров — каждый проект с нуля.\n\n"
        "<b>Что сделали:</b>\n"
        "— Разработали 50+ шаблонов (NDA, SLA, MSA)\n"
        "— Автоматизировали генерацию через бота\n"
        "— Обучение команды\n\n"
        "<b>Результат:</b> Время на оформление проекта "
        "сократилось с 3 дней до 2 часов.\n\n"
        "📞 Нужны шаблоны под ваш бизнес?"
    ),
}


@router.callback_query(F.data.startswith("send_case_"))
async def send_case_detail(
    callback: CallbackQuery,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Отправляет подробный кейс по сфере + кнопки дальнейших действий."""
    sphere_key = callback.data.removeprefix("send_case_")
    await callback.answer()

    case_text = SPHERE_CASE_DETAILS.get(sphere_key)
    if not case_text:
        case_text = (
            "💼 <b>Кейс из практики SOLIS Partners</b>\n\n"
            "Мы помогли компании из Казахстана оптимизировать "
            "юридические процессы и снизить риски.\n\n"
            "📞 Хотите обсудить вашу ситуацию?"
        )

    asyncio.create_task(track(callback.from_user.id, "view_case", meta=sphere_key))

    buttons = [
        [InlineKeyboardButton(
            text="📞 Записаться на консультацию",
            callback_data="book_consultation",
        )],
        [InlineKeyboardButton(
            text="📚 Другие гайды",
            callback_data="show_categories",
        )],
    ]

    await callback.message.answer(
        case_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("share_bot_"))
async def share_bot(callback: CallbackQuery, bot: Bot) -> None:
    """Генерирует реферальную ссылку с UTM-меткой для отслеживания."""
    user_id = callback.data.removeprefix("share_bot_")
    bot_info = await bot.get_me()
    share_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}--referral"

    share_text = (
        "🔗 <b>Поделитесь с коллегами!</b>\n\n"
        "Бесплатные PDF-гайды от SOLIS Partners: налоговая "
        "оптимизация, IT-право, инвестиции и M&A в Казахстане.\n\n"
        "Перешлите это сообщение или скопируйте ссылку:\n\n"
        f"<code>{share_link}</code>"
    )

    await callback.message.answer(
        share_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="📤 Переслать другу",
                switch_inline_query=f"Бесплатные юридические гайды для бизнеса в Казахстане — забирай: {share_link}",
            )],
            [InlineKeyboardButton(text="🔹 Назад к темам", callback_data="show_categories")],
        ]),
    )
    await callback.answer()


# ──────────────────────── Подписка на тему (категорию) ────────────────────


@router.callback_query(F.data.startswith("topic_sub_"))
async def subscribe_to_category(callback: CallbackQuery) -> None:
    """Подписывает пользователя на обновления по категории гайдов."""
    from src.database.crud import subscribe_to_topic, get_user_topic_subscriptions

    cat_slug = callback.data.removeprefix("topic_sub_")
    user_id = callback.from_user.id

    created = await subscribe_to_topic(user_id, cat_slug)
    if created:
        await callback.answer("📩 Вы подписались! Мы пришлём уведомление о новых гайдах по этой теме.", show_alert=True)
        logger.info("Topic subscription: user=%s cat='%s'", user_id, cat_slug)
    else:
        await callback.answer("Вы уже подписаны на эту тему.", show_alert=True)


@router.callback_query(F.data.startswith("topic_unsub_"))
async def unsubscribe_from_category(callback: CallbackQuery) -> None:
    """Отписывает пользователя от категории."""
    from src.database.crud import unsubscribe_from_topic

    cat_slug = callback.data.removeprefix("topic_unsub_")
    user_id = callback.from_user.id

    removed = await unsubscribe_from_topic(user_id, cat_slug)
    if removed:
        await callback.answer("Вы отписались от этой темы.", show_alert=True)
    else:
        await callback.answer("Вы не были подписаны.", show_alert=True)


@router.callback_query(F.data == "my_subscriptions")
async def show_my_subscriptions(callback: CallbackQuery) -> None:
    """Показывает текущие подписки пользователя на темы."""
    from src.database.crud import get_user_topic_subscriptions

    user_id = callback.from_user.id
    subs = await get_user_topic_subscriptions(user_id)

    if not subs:
        await callback.answer("У вас пока нет подписок на темы.", show_alert=True)
        return

    await callback.answer()
    buttons: list[list[InlineKeyboardButton]] = []
    for cat_slug in subs:
        buttons.append([
            InlineKeyboardButton(
                text=f"❌ Отписаться от «{cat_slug}»",
                callback_data=f"topic_unsub_{cat_slug}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="🔹 К темам", callback_data="show_categories")])

    await callback.message.answer(
        "📩 <b>Ваши подписки на темы:</b>\n\n"
        "Мы пришлём уведомление, когда появятся новые гайды "
        "по подписанным категориям.\n\n"
        "Нажмите, чтобы отписаться:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


# ──────────────────────── Уведомление админа ─────────────────────────────


async def notify_admin(
    bot: Bot,
    *,
    user_id: int,
    username: str,
    name: str,
    email: str,
    guide: str,
    source: str = "",
) -> None:
    """Отправка уведомления администратору о новом лиде."""
    from datetime import datetime, timezone

    try:
        source_line = f"📍 Источник: {_esc_html(source)}\n" if source else ""
        username_display = f"@{username}" if username else "нет"

        # Подтягиваем сферу, если она указана
        sphere_line = ""
        try:
            lead = await get_lead_by_user_id(user_id)
            if lead and getattr(lead, "business_sphere", None):
                sphere_line = f"🏢 Сфера: {_esc_html(lead.business_sphere)}\n"
        except Exception:
            pass

        now = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")

        text = (
            "🆕 <b>Новый лид!</b>\n\n"
            f"👤 Имя: {_esc_html(name)}\n"
            f"📧 Email: {_esc_html(email)}\n"
            f"📚 Гайд: {_esc_html(guide)}\n"
            f"💬 Telegram: {username_display}\n"
            f"{sphere_line}"
            f"{source_line}"
            f"🕐 Время: {now}\n"
            f"🆔 User ID: <code>{user_id}</code>"
        )

        crm_url = (
            f"https://docs.google.com/spreadsheets/d/"
            f"{settings.GOOGLE_SPREADSHEET_ID}/edit#gid=0"
        )
        buttons = [
            [InlineKeyboardButton(text="📊 Открыть CRM", url=crm_url)],
        ]
        if username:
            buttons.append(
                [InlineKeyboardButton(text="💬 Написать в Telegram", url=f"https://t.me/{username}")],
            )

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        await bot.send_message(chat_id=settings.ADMIN_ID, text=text, reply_markup=keyboard)
    except Exception as e:
        logger.error("Не удалось уведомить администратора: %s", e)
