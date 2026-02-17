"""Обработчик /start — точка входа в воронку.

Value-first подход:
    /start показывает ценность (категории гайдов) сразу.
    Барьеры (подписка на канал + email) проверяются только
    при нажатии Скачать в download-хендлере.

UTM-трекинг через deep link:
    /start guide_ID--src_EMAIL--med_NEWSLETTER--cmp_FEB2026
    Разделитель ``--``, префиксы: src_ (source), med_ (medium), cmp_ (campaign).
"""

import asyncio
import logging
from typing import NamedTuple

from aiogram import Bot, F, Router
from aiogram.filters import CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.bot.keyboards.inline import categories_keyboard, guides_menu_keyboard, library_keyboard, main_menu_keyboard, subscription_keyboard
from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.subscription_check import check_subscription
from src.constants import get_text
from src.database.crud import (
    cancel_tasks_for_user,
    delete_leads_for_user,
    delete_user,
    get_lead_by_user_id,
    get_or_create_user,
    get_user_downloaded_guides,
    track,
)

router = Router()
logger = logging.getLogger(__name__)


# ── UTM parsing ────────────────────────────────────────────────────────


class UTMData(NamedTuple):
    """Структурированные UTM-параметры из deep link."""
    source: str    # utm_source (email, facebook, linkedin, channel)
    medium: str    # utm_medium (newsletter, cpc, organic, post)
    campaign: str  # utm_campaign (feb2026, investment_webinar)
    raw: str       # исходная строка целиком


def parse_utm(raw_args: str) -> UTMData:
    """Извлекает UTM-метки из deep-link параметра.

    Поддерживает два формата:

    Полный:
        ``payload--src_SOURCE--med_MEDIUM--cmp_CAMPAIGN``

    Короткий (только source):
        ``payload--SOURCE``
        Если сегмент после ``--`` не содержит префикса ``src_/med_/cmp_``,
        он интерпретируется как source.

    Любой сегмент может отсутствовать. Разделитель ``--``.

    Examples:
        >>> parse_utm("guide_TOO--src_email--med_newsletter--cmp_feb2026")
        UTMData(source='email', medium='newsletter', campaign='feb2026', ...)
        >>> parse_utm("guide_invest--linkedin")
        UTMData(source='linkedin', medium='', campaign='', ...)
        >>> parse_utm("guide_TOO")
        UTMData(source='', medium='', campaign='', raw='guide_TOO')
    """
    source = medium = campaign = ""
    segments = raw_args.split("--")
    for seg_raw in segments[1:]:
        seg = seg_raw.strip()
        if seg.startswith("src_"):
            source = seg[4:]
        elif seg.startswith("med_"):
            medium = seg[4:]
        elif seg.startswith("cmp_"):
            campaign = seg[4:]
        elif not source and seg:
            source = seg
    return UTMData(source=source, medium=medium, campaign=campaign, raw=raw_args)


def strip_utm(raw_args: str) -> str:
    """Возвращает payload без UTM-сегментов.

    >>> strip_utm("guide_TOO--src_email--med_newsletter")
    'guide_TOO'
    """
    parts = [p.strip() for p in raw_args.split("--")
             if not p.strip().startswith(("src_", "med_", "cmp_"))]
    return parts[0] if parts else raw_args


def _utm_json(utm: UTMData) -> str | None:
    """Сериализует UTM-данные в JSON-строку для поля meta (аналитика).

    Возвращает None, если UTM-данных нет.
    """
    if not utm.source and not utm.medium and not utm.campaign:
        return None
    import json
    d: dict[str, str] = {}
    if utm.source:
        d["src"] = utm.source
    if utm.medium:
        d["med"] = utm.medium
    if utm.campaign:
        d["cmp"] = utm.campaign
    return json.dumps(d, ensure_ascii=False)


def format_utm_source(utm: UTMData) -> str:
    """Форматирует UTM для хранения в поле traffic_source.

    Результат: ``payload | src=X med=Y cmp=Z`` или просто payload.
    """
    parts = []
    if utm.source:
        parts.append(f"src={utm.source}")
    if utm.medium:
        parts.append(f"med={utm.medium}")
    if utm.campaign:
        parts.append(f"cmp={utm.campaign}")
    if parts:
        base = strip_utm(utm.raw)
        return f"{base} | {' '.join(parts)}"
    return utm.raw


@router.message(CommandStart())
async def cmd_start(
    message: Message,
    bot: Bot,
    state: FSMContext,
    command: CommandObject,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Обработка команды /start.

    Value-first: сначала ценность (категории / превью гайда),
    барьеры (подписка + email) — только при скачивании.
    """
    user = message.from_user
    if user is None:
        return

    raw_args = command.args or ""

    # ── UTM-трекинг ───────────────────────────────────────────────────
    utm = parse_utm(raw_args)
    clean_args = strip_utm(raw_args)
    source_str = format_utm_source(utm)

    if utm.source:
        logger.info(
            "UTM detected: user=%s src=%s med=%s cmp=%s",
            user.id, utm.source, utm.medium, utm.campaign,
        )

    # ── Воронка: событие bot_start ────────────────────────────────────
    asyncio.create_task(track(
        user.id, "bot_start",
        source=source_str or None,
        meta=_utm_json(utm) or (clean_args[:100] if clean_args else None),
    ))

    # ── Deep Link: гайд (guide_*) — показываем карточку сразу ────────
    if clean_args.startswith("guide_"):
        guide_slug = clean_args.removeprefix("guide_")
        logger.info("Guide deep link: user=%s, guide=%s, utm_src=%s", user.id, guide_slug, utm.source)

        await get_or_create_user(
            user_id=user.id, username=user.username, full_name=user.full_name,
            traffic_source=source_str if source_str else None,
        )
        await state.clear()
        await state.update_data(traffic_source=source_str)

        asyncio.create_task(track(
            user.id, "deeplink_guide",
            guide_id=guide_slug,
            source=source_str or None,
            meta=_utm_json(utm),
        ))

        catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
        matched_guide = None
        for guide in catalog:
            if str(guide.get("id", "")) == guide_slug:
                matched_guide = guide
                break

        if not matched_guide:
            guide_name_lower = guide_slug.replace("-", " ").replace("_", " ").lower()
            for guide in catalog:
                if (
                    guide_name_lower in guide.get("title", "").lower()
                    or guide_name_lower in guide.get("category", "").lower()
                    or guide.get("id", "").lower() == guide_name_lower
                ):
                    matched_guide = guide
                    break

        if matched_guide:
            guide_id = matched_guide.get("id", "")
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"📥 Получить: {matched_guide['title']}",
                    callback_data=f"download_{guide_id}",
                )],
                [InlineKeyboardButton(
                    text="🔹 Все темы", callback_data="show_categories",
                )],
            ])
            await message.answer(
                f"📚 <b>{matched_guide['title']}</b>\n\n"
                f"{matched_guide.get('description', '')}\n\n"
                "Нажмите кнопку ниже, чтобы получить гайд:",
                reply_markup=kb,
            )
        else:
            await message.answer(
                "Привет! Я помогу разобраться в юридических "
                "вопросах бизнеса в Казахстане.\n\n"
                "Выберите тему:",
                reply_markup=categories_keyboard(catalog),
            )
        return

    # ── Deep Link: статья (article_*) → Instant View ───────────────
    if clean_args.startswith("article_"):
        article_slug = clean_args.removeprefix("article_")
        logger.info("Article deep link: user=%s, article=%s, utm_src=%s", user.id, article_slug, utm.source)

        await get_or_create_user(
            user_id=user.id, username=user.username, full_name=user.full_name,
            traffic_source=source_str if source_str else None,
        )
        await state.clear()
        await state.update_data(traffic_source=source_str)

        asyncio.create_task(track(
            user.id, "deeplink_article",
            source=source_str or None,
            meta=_utm_json(utm) or article_slug[:100],
        ))

        article = await google.get_article_by_id(article_slug)
        if article:
            title = article.get("title", article_slug)
            desc = article.get("description", "")
            telegraph_url = article.get("telegraph_url", "")
            external_url = article.get("external_url", article.get("url", ""))

            text_parts = [f"📰 <b>{title}</b>"]
            if desc:
                text_parts.append(f"\n{desc}")

            buttons = []
            if telegraph_url:
                buttons.append([InlineKeyboardButton(
                    text="📖 Читать статью",
                    url=telegraph_url,
                )])
            elif external_url:
                buttons.append([InlineKeyboardButton(
                    text="📖 Читать статью",
                    url=external_url,
                )])
            buttons.append([InlineKeyboardButton(
                text="🔹 Скачать гайд по теме",
                callback_data="show_categories",
            )])

            await message.answer(
                "\n".join(text_parts),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            )
        else:
            catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
            await message.answer(
                "📰 К сожалению, статья не найдена.\n\n"
                "📚 Но у нас есть полезные гайды для вашего бизнеса:",
                reply_markup=categories_keyboard(catalog),
            )
        return

    # ── Deep Link: консультация (consult) ──────────────────────────
    if clean_args == "consult":
        logger.info("Consult deep link: user=%s, utm_src=%s", user.id, utm.source)

        await get_or_create_user(
            user_id=user.id, username=user.username, full_name=user.full_name,
            traffic_source=source_str if source_str else None,
        )
        await state.clear()
        await state.update_data(traffic_source=source_str)

        asyncio.create_task(track(
            user.id, "deeplink_consult",
            source=source_str or None,
            meta=_utm_json(utm),
        ))

        await message.answer("⚙️", reply_markup=main_menu_keyboard())
        await message.answer(
            "📞 <b>Бесплатная консультация</b>\n\n"
            "Обсудим вашу ситуацию с практикующим юристом.\n"
            "Нажмите кнопку ниже, чтобы записаться:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="📞 Записаться на консультацию",
                    callback_data="book_consultation",
                )],
                [InlineKeyboardButton(
                    text="📚 Посмотреть гайды",
                    callback_data="show_categories",
                )],
            ]),
        )
        return

    # ── Deep Link: реферал (ref_{user_id}) ──────────────────────────
    if clean_args.startswith("ref_"):
        referrer_id = clean_args.removeprefix("ref_")
        logger.info("Referral deep link: user=%s, referrer=%s, utm_src=%s", user.id, referrer_id, utm.source)

        await get_or_create_user(
            user_id=user.id, username=user.username, full_name=user.full_name,
            traffic_source=source_str or f"ref_{referrer_id}",
        )
        await state.clear()
        await state.update_data(traffic_source=source_str or f"ref_{referrer_id}")

        asyncio.create_task(track(
            user.id, "deeplink_referral",
            source=source_str or f"ref_{referrer_id}",
            meta=_utm_json(utm),
        ))

    # ── Стандартный /start flow ───────────────────────────────────────
    # Value-first: сразу показываем категории гайдов.
    # Барьеры (подписка + email) — только при нажатии «Скачать».

    source = source_str
    if raw_args:
        logger.info("Deep-link: '%s' (user_id=%s, utm=%s)", raw_args, user.id, source_str)

    await get_or_create_user(
        user_id=user.id,
        username=user.username,
        full_name=user.full_name,
        traffic_source=source_str if source_str else None,
    )

    logger.info("Команда /start от user_id=%s, src=%s", user.id, source_str)

    await state.clear()

    if source:
        await state.update_data(traffic_source=source)

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    # Персонализация для возвращающегося пользователя
    try:
        existing_lead = await get_lead_by_user_id(user.id)
    except Exception:
        existing_lead = None

    await message.answer("⚙️", reply_markup=main_menu_keyboard())

    if existing_lead:
        # ── Возвращающийся пользователь ───────────────────────────
        try:
            name = existing_lead.name
            if utm.source == "email":
                welcome_text = (
                    f"👋 <b>{name}</b>, рады видеть вас из рассылки!\n\n"
                    "Вот свежие гайды, которые могут быть полезны:"
                )
            elif utm.source in ("facebook", "instagram", "fb", "ig"):
                welcome_text = (
                    f"👋 <b>{name}</b>, добро пожаловать из соцсетей!\n\n"
                    "Здесь — бесплатные PDF-гайды. Выберите тему:"
                )
            elif utm.source == "linkedin":
                welcome_text = (
                    f"👋 <b>{name}</b>, рады видеть вас из LinkedIn!\n\n"
                    "Практические гайды для бизнеса в Казахстане:"
                )
            else:
                welcome_text = (
                    f"👋 С возвращением, <b>{name}</b>!\n\n"
                    "Выберите тему — я пришлю PDF с пошаговыми "
                    "инструкциями и чек-листами:"
                )
        except Exception:
            welcome_text = "👋 С возвращением! Выберите тему:"

        try:
            await message.answer(
                welcome_text,
                reply_markup=categories_keyboard(catalog),
            )
        except Exception as e:
            logger.error("Ошибка отображения каталога: %s", e)
            await message.answer(
                "Выберите тему — я пришлю PDF-гайд.\n\n"
                "⚠️ Ошибка загрузки каталога. Попробуйте /start снова.",
            )
    else:
        # ── Новый пользователь — сразу выгода, потом барьеры ─────
        guide_count = len(catalog)

        welcome_text = (
            "Привет! Я помогу вам разобраться "
            "в юридических вопросах бизнеса в Казахстане.\n\n"
            f"У нас <b>{guide_count} бесплатных PDF-гайдов</b> "
            "с чек-листами и примерами документов "
            "от практикующих юристов "
            "<b>SOLIS Partners</b>.\n\n"
            "Выберите тему — и получите гайд "
            "прямо сюда, в чат 👇"
        )

        try:
            await message.answer(
                welcome_text,
                reply_markup=categories_keyboard(catalog),
            )
        except Exception as e:
            logger.error("Ошибка отображения каталога: %s", e)
            await message.answer(
                "Выберите тему — я пришлю PDF-гайд.\n\n"
                "⚠️ Ошибка загрузки каталога. Попробуйте /start снова.",
            )


@router.message(F.text == "📚 Гайды")
async def reply_guides_button(
    message: Message,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Обработка кнопки 'Гайды' из постоянного меню."""
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    if not catalog:
        await message.answer("Каталог пуст. Попробуйте позже.")
        return

    await message.answer(
        "📚 <b>Выберите тему:</b>",
        reply_markup=categories_keyboard(catalog),
    )


@router.message(F.text == "🆕 Я новый пользователь")
async def restart_as_new_user(
    message: Message,
    state: FSMContext,
    cache: TTLCache,
) -> None:
    """Сбрасывает персональные данные/состояние и перезапускает флоу как для нового."""
    user = message.from_user
    if user is None:
        return

    user_id = user.id
    leads_deleted = await delete_leads_for_user(user_id)
    user_deleted = await delete_user(user_id)
    tasks_cancelled = await cancel_tasks_for_user(user_id)
    await state.clear()
    cache.invalidate()

    logger.info(
        "User self-reset: user_id=%s, leads=%d, user=%s, tasks=%d",
        user_id, leads_deleted, user_deleted, tasks_cancelled,
    )

    await message.answer(
        "🧪 <b>Сброс выполнен!</b>\n\n"
        f"• Лидов удалено: {leads_deleted}\n"
        f"• Профиль удалён: {'да' if user_deleted else 'нет'}\n"
        f"• Follow-up задач отменено: {tasks_cancelled}\n"
        "• FSM очищен\n"
        "• Кеш сброшен\n\n"
        "Теперь нажмите /start — бот начнёт флоу как для нового пользователя."
    )


@router.message(F.text == "📂 Мои гайды")
async def reply_library_button(
    message: Message,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Обработка кнопки 'Мои гайды' — показывает скачанные гайды."""
    user_id = message.from_user.id
    downloaded_ids = await get_user_downloaded_guides(user_id)

    if not downloaded_ids:
        await message.answer(
            "📂 <b>Ваша библиотека пуста</b>\n\n"
            "Вы ещё не скачивали гайды. Нажмите «📚 Гайды» чтобы выбрать первый!",
        )
        return

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    downloaded_guides = [g for g in catalog if g.get("id") in downloaded_ids]

    if not downloaded_guides:
        await message.answer(
            "📂 <b>Ваша библиотека пуста</b>\n\n"
            "Не удалось найти гайды. Попробуйте «📚 Гайды».",
        )
        return

    await message.answer(
        f"📂 <b>Ваша библиотека</b> ({len(downloaded_guides)} шт.)\n\n"
        "Нажмите на гайд, чтобы скачать повторно:",
        reply_markup=library_keyboard(downloaded_guides),
    )


@router.message(F.text == "/library")
async def cmd_library(
    message: Message,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Команда /library — алиас для кнопки 'Мои гайды'."""
    await reply_library_button(message, google, cache)


@router.message(F.text == "📩 Подписки")
async def reply_subscriptions_button(message: Message, cache: TTLCache, google: GoogleSheetsClient) -> None:
    """Обработка кнопки 'Подписки' — показывает текущие подписки на темы."""
    from src.database.crud import get_user_topic_subscriptions

    user_id = message.from_user.id
    subs = await get_user_topic_subscriptions(user_id)

    if not subs:
        catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
        seen: dict[str, str] = {}
        for g in catalog:
            cat = g.get("category", "").strip()
            if cat and cat not in seen:
                from src.bot.keyboards.inline import _slugify_cat
                seen[cat] = _slugify_cat(cat)

        buttons = []
        for cat_name, slug in seen.items():
            buttons.append([InlineKeyboardButton(
                text=f"📩 {cat_name}",
                callback_data=f"topic_sub_{slug}",
            )])

        await message.answer(
            "📩 <b>Подписки на темы</b>\n\n"
            "У вас пока нет подписок. Выберите темы, по которым "
            "хотите получать уведомления о новых гайдах:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None,
        )
        return

    buttons = []
    for cat_slug in subs:
        buttons.append([InlineKeyboardButton(
            text=f"❌ Отписаться от «{cat_slug}»",
            callback_data=f"topic_unsub_{cat_slug}",
        )])

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    seen: dict[str, str] = {}
    for g in catalog:
        cat = g.get("category", "").strip()
        if cat and cat not in seen:
            from src.bot.keyboards.inline import _slugify_cat
            seen[cat] = _slugify_cat(cat)

    for cat_name, slug in seen.items():
        if slug not in subs:
            buttons.append([InlineKeyboardButton(
                text=f"📩 Подписаться: {cat_name}",
                callback_data=f"topic_sub_{slug}",
            )])

    await message.answer(
        f"📩 <b>Ваши подписки</b> ({len(subs)} шт.)\n\n"
        "Вы получаете уведомления о новых гайдах по этим темам.\n"
        "Нажмите, чтобы отписаться. Ниже — доступные для подписки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
