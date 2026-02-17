"""Email-ретаргетинг — персонализированные рассылки по сегментам.

Команды:
    /email_campaign — интерактивный конструктор кампании

Сегменты строятся по скачанным гайдам → интересы (теги).
В письме: персонализированная рекомендация + UTM deep link.
"""

import asyncio
import html
import logging
from collections import defaultdict
from datetime import datetime, timezone

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


# ── Маппинг guide_id → тематические теги ────────────────────────────────

GUIDE_INTEREST_MAP: dict[str, list[str]] = {
    "too": ["corporate", "registration", "business"],
    "ip": ["startup", "registration", "business"],
    "mfca": ["aifc", "international", "finance"],
    "aifc": ["aifc", "international", "finance"],
    "esop": ["startup", "corporate", "finance"],
    "taxes": ["tax", "finance", "business"],
    "labor": ["labor", "hr", "business"],
    "it_law": ["it", "tech", "ip"],
    "ma": ["corporate", "finance", "m&a"],
    "invest": ["investment", "finance", "international"],
}

ALL_SEGMENTS = sorted({tag for tags in GUIDE_INTEREST_MAP.values() for tag in tags})

# ── Обратный маппинг: тег → guide_ids ────────────────────────────────────

TAG_TO_GUIDES: dict[str, list[str]] = defaultdict(list)
for _gid, _tags in GUIDE_INTEREST_MAP.items():
    for _tag in _tags:
        TAG_TO_GUIDES[_tag].append(_gid)


def _is_admin(uid: int | None) -> bool:
    return uid == settings.ADMIN_ID


def _esc(text: str) -> str:
    return html.escape(str(text))


# ═══════════════════════════════════════════════════════════════════════════
#  Сегментация
# ═══════════════════════════════════════════════════════════════════════════


def _get_user_interests(leads: list[dict], user_id: int) -> set[str]:
    """Определяет интересы пользователя по скачанным гайдам."""
    interests: set[str] = set()
    for lead in leads:
        if str(lead.get("user_id", "")) == str(user_id):
            guide = str(lead.get("guide", lead.get("selected_guide", ""))).lower()
            for key, tags in GUIDE_INTEREST_MAP.items():
                if key in guide:
                    interests.update(tags)
    return interests


def _get_user_guides(leads: list[dict], user_id: int) -> set[str]:
    """Возвращает set guide_id, скачанных пользователем."""
    guides: set[str] = set()
    for lead in leads:
        if str(lead.get("user_id", "")) == str(user_id):
            g = str(lead.get("guide", lead.get("selected_guide", ""))).strip()
            if g:
                guides.add(g)
    return guides


def _build_audience(
    leads: list[dict],
    target_tags: list[str] | None = None,
    warmth_filter: str | None = None,
) -> list[dict]:
    """Строит сегментированную аудиторию.

    Returns:
        Список уникальных записей: {user_id, email, name, interests, warmth, guides, sphere}
    """
    target_set = {t.lower() for t in target_tags} if target_tags else None

    seen_emails: set[str] = set()
    audience: list[dict] = []

    # Группируем по user_id
    user_data: dict[int, dict] = {}
    for lead in leads:
        uid_str = str(lead.get("user_id", "")).strip()
        if not uid_str:
            continue
        uid = int(uid_str)
        email = str(lead.get("email", "")).strip().lower()
        name = str(lead.get("name", "")).strip()
        warmth = str(lead.get("warmth", "Cold")).strip()
        sphere = str(lead.get("sphere_tag", "")).strip()

        if uid not in user_data:
            user_data[uid] = {
                "user_id": uid,
                "email": email,
                "name": name,
                "warmth": warmth,
                "sphere": sphere,
                "guides": set(),
                "interests": set(),
            }
        elif email and not user_data[uid]["email"]:
            user_data[uid]["email"] = email
        if sphere and not user_data[uid].get("sphere"):
            user_data[uid]["sphere"] = sphere

        guide = str(lead.get("guide", lead.get("selected_guide", ""))).strip()
        if guide:
            user_data[uid]["guides"].add(guide)
            for key, tags in GUIDE_INTEREST_MAP.items():
                if key in guide.lower():
                    user_data[uid]["interests"].update(tags)

    for ud in user_data.values():
        email = ud["email"]
        if not email or email in seen_emails:
            continue

        if warmth_filter and ud["warmth"].lower() != warmth_filter.lower():
            continue

        if target_set and not (ud["interests"] & target_set):
            continue

        seen_emails.add(email)
        audience.append({
            "user_id": ud["user_id"],
            "email": email,
            "name": ud["name"],
            "warmth": ud["warmth"],
            "sphere": ud.get("sphere", ""),
            "guides": ud["guides"],
            "interests": ud["interests"],
        })

    return audience


def _pick_best_guide_for_user(
    user: dict,
    catalog: list[dict],
) -> dict | None:
    """Подбирает лучший гайд для пользователя на основе его интересов.

    Логика: находим гайд из тех же тематических тегов, который
    пользователь ещё НЕ скачивал. Приоритет — максимум совпадений.
    """
    downloaded = user.get("guides", set())
    user_interests = user.get("interests", set())

    if not user_interests:
        # Нет интересов — берём любой не скачанный
        for g in catalog:
            gid = str(g.get("id", ""))
            if gid and gid not in downloaded:
                return g
        return None

    # Считаем score для каждого гайда
    scored: list[tuple[int, dict]] = []
    for g in catalog:
        gid = str(g.get("id", ""))
        if not gid or gid in downloaded:
            continue

        guide_tags: set[str] = set()
        for key, tags in GUIDE_INTEREST_MAP.items():
            if key in gid.lower():
                guide_tags.update(tags)

        overlap = len(user_interests & guide_tags)
        if overlap > 0:
            scored.append((overlap, g))

    if scored:
        scored.sort(key=lambda x: -x[0])
        return scored[0][1]

    # Fallback: любой не скачанный
    for g in catalog:
        gid = str(g.get("id", ""))
        if gid and gid not in downloaded:
            return g
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  Email-шаблон для ретаргетинга
# ═══════════════════════════════════════════════════════════════════════════


# ── Крючки по сфере для email ──────────────────────────────────────────────

_SPHERE_EMAIL_HOOKS: dict[str, str] = {
    "it": "Мы видим, что IT-компаниям сейчас особенно важно разбираться в этой теме.",
    "производство": "Для производственных компаний это особенно актуально в 2025 году.",
    "ритейл": "В ритейле эти вопросы стоят особенно остро — и вот почему.",
    "инвестиции": "Если вы инвестор или работаете с инвесторами — это must-read.",
    "финтех": "Для финтех-проектов важно учитывать нюансы, описанные в гайде.",
    "стартап": "Стартапам критически важно разобраться в этом до привлечения инвестиций.",
}


def build_retarget_email(
    name: str,
    guide: dict,
    bot_username: str,
    *,
    campaign_id: str = "",
    sphere: str = "",
) -> tuple[str, str]:
    """Генерирует HTML-письмо с рекомендацией гайда.

    Args:
        sphere: Сфера бизнеса пользователя (для персонализации).

    Returns:
        (subject, html_body)
    """
    guide_title = guide.get("title", "")
    guide_desc = guide.get("description", "")
    guide_id = guide.get("id", "")
    preview = guide.get("preview_text", "") or guide.get("preview", "")
    highlights = guide.get("highlights", "")
    pages = str(guide.get("pages", "")).strip()
    download_count = guide.get("download_count", "")

    utm = f"--src_email--cmp_{campaign_id}" if campaign_id else "--src_email"
    deep_link = f"https://t.me/{bot_username}?start=guide_{guide_id}{utm}"

    # Highlights → bullets
    bullets_html = ""
    if highlights:
        items = [h.strip() for h in highlights.replace("\n", ";").split(";") if h.strip()]
        if items:
            bullets_html = "<ul style='margin:12px 0;padding-left:20px;'>"
            for item in items[:5]:
                bullets_html += f"<li style='margin:4px 0;'>{_esc(item)}</li>"
            bullets_html += "</ul>"

    meta_parts = []
    if pages:
        meta_parts.append(f"{_esc(pages)} страниц")
    meta_parts.extend(["PDF", "бесплатно"])
    meta_line = " · ".join(meta_parts)

    # Social proof
    social_proof = ""
    if download_count:
        social_proof = (
            f'<p style="margin:8px 0 16px;color:#16a34a;font-size:13px;">'
            f"📊 Уже <b>{_esc(str(download_count))}</b> предпринимателей скачали этот гайд"
            f"</p>"
        )

    # Персонализация по сфере
    sphere_hook = ""
    if sphere:
        sphere_lower = sphere.lower()
        for key, hook_text in _SPHERE_EMAIL_HOOKS.items():
            if key in sphere_lower:
                sphere_hook = (
                    f'<p style="color:#555;margin:0 0 12px;">'
                    f"💡 <i>{_esc(hook_text)}</i></p>"
                )
                break

    subject = f"{name}, новый гайд для вас: «{guide_title}»"

    html_body = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#fff;">
        <div style="background:#1a237e;padding:20px;text-align:center;">
            <h1 style="color:#c9a227;margin:0;font-size:22px;">SOLIS Partners</h1>
            <p style="color:#fff;margin:5px 0 0;font-size:13px;">Юридическая фирма нового поколения</p>
        </div>

        <div style="padding:30px;">
            <p style="font-size:16px;color:#333;">Здравствуйте, <b>{_esc(name)}</b>!</p>

            <p style="color:#555;">На основе ваших интересов мы подобрали новый материал,
            который может быть полезен для вашего бизнеса:</p>

            {sphere_hook}

            <div style="background:#f8f9fa;border-left:4px solid #2563eb;padding:20px;
                        margin:20px 0;border-radius:8px;">
                <h2 style="margin:0 0 8px;font-size:18px;color:#1a237e;">
                    📚 {_esc(guide_title)}</h2>
                {'<p style="margin:0 0 12px;color:#555;">' + _esc(guide_desc) + '</p>' if guide_desc else ''}
                {bullets_html}
                {'<p style="margin:0 0 12px;color:#555;"><b>Что внутри:</b> ' + _esc(preview) + '</p>' if preview and not bullets_html else ''}
                {social_proof}
                <p style="margin:0 0 16px;font-size:13px;color:#888;">📎 {meta_line}</p>
                <a href="{_esc(deep_link)}"
                   style="display:inline-block;background:#2563eb;color:#fff;
                          padding:12px 28px;border-radius:6px;text-decoration:none;
                          font-weight:bold;font-size:15px;">
                    📥 Скачать бесплатно
                </a>
            </div>

            <p style="color:#888;font-size:13px;margin-top:24px;">
                Гайд откроется в Telegram-боте SOLIS Partners — там же можно
                задать вопрос AI-юристу или записаться на бесплатную консультацию.
            </p>
        </div>

        <div style="padding:15px;text-align:center;background:#f5f5f5;
                    border-top:1px solid #eee;">
            <p style="color:#999;font-size:12px;margin:0;">
                © SOLIS Partners ·
                <a href="https://solispartners.kz" style="color:#999;">solispartners.kz</a>
            </p>
            <p style="color:#bbb;font-size:11px;margin:5px 0 0;">
                Вы получили это письмо, потому что скачивали гайды через нашего бота.
                <a href="https://t.me/{_esc(bot_username)}?start=unsubscribe"
                   style="color:#bbb;">Отписаться</a>
            </p>
        </div>
    </div>
    """

    return subject, html_body


# ═══════════════════════════════════════════════════════════════════════════
#  /email_campaign — конструктор кампании
# ═══════════════════════════════════════════════════════════════════════════


class CampaignStates(StatesGroup):
    choose_segment = State()
    choose_guide = State()
    confirm = State()


@router.message(Command("email_campaign"))
async def cmd_email_campaign(
    message: Message,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Запуск интерактивного конструктора email-кампании."""
    if not _is_admin(message.from_user and message.from_user.id):
        return

    from src.bot.utils.email_sender import is_email_configured
    if not is_email_configured():
        await message.answer(
            "❌ Email не настроен.\n\n"
            "Добавьте в <code>.env</code>:\n"
            "<code>RESEND_API_KEY=re_...</code>\n"
            "или SMTP-параметры (SMTP_HOST, SMTP_USER, SMTP_PASSWORD)."
        )
        return

    await state.clear()

    # Загружаем лидов и считаем сегменты
    leads = await google.get_recent_leads(limit=5000)
    audience_all = _build_audience(leads)

    if not audience_all:
        await message.answer("📧 Нет лидов с email для рассылки.")
        return

    # Считаем размер каждого сегмента
    segment_counts: dict[str, int] = {}
    for tag in ALL_SEGMENTS:
        seg = _build_audience(leads, target_tags=[tag])
        if seg:
            segment_counts[tag] = len(seg)

    lines = [
        f"📧 <b>Email-кампания</b>\n",
        f"👥 Всего лидов с email: <b>{len(audience_all)}</b>\n",
        "🎯 <b>Выберите сегмент:</b>",
    ]

    buttons = []
    for tag in sorted(segment_counts, key=lambda t: -segment_counts[t]):
        cnt = segment_counts[tag]
        cb = f"ecamp_seg_{tag}"
        buttons.append([InlineKeyboardButton(
            text=f"#{tag} ({cnt} чел.)",
            callback_data=cb,
        )])

    buttons.append([InlineKeyboardButton(
        text=f"📨 Все ({len(audience_all)} чел.)",
        callback_data="ecamp_seg_all",
    )])
    buttons.append([InlineKeyboardButton(
        text="🔥 Только Hot",
        callback_data="ecamp_seg_hot",
    )])
    buttons.append([InlineKeyboardButton(
        text="❌ Отмена",
        callback_data="ecamp_cancel",
    )])

    await message.answer(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await state.set_state(CampaignStates.choose_segment)


@router.callback_query(F.data.startswith("ecamp_seg_"), CampaignStates.choose_segment)
async def campaign_segment_chosen(
    callback: CallbackQuery,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Сегмент выбран → показываем выбор гайда для рекомендации."""
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()

    segment = callback.data.removeprefix("ecamp_seg_")

    # Считаем аудиторию
    leads = await google.get_recent_leads(limit=5000)
    if segment == "all":
        audience = _build_audience(leads)
        seg_label = "Все"
    elif segment == "hot":
        audience = _build_audience(leads, warmth_filter="Hot")
        seg_label = "Hot"
    else:
        audience = _build_audience(leads, target_tags=[segment])
        seg_label = f"#{segment}"

    if not audience:
        await callback.message.edit_text(
            f"📧 Сегмент «{seg_label}» пуст — нет лидов с email."
        )
        await state.clear()
        return

    await state.update_data(
        campaign_segment=segment,
        campaign_seg_label=seg_label,
        campaign_audience_count=len(audience),
    )

    # Показываем гайды для рекомендации
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    # Считаем, скольким подойдёт авто-рекомендация
    auto_count = sum(
        1 for u in audience if _pick_best_guide_for_user(u, catalog)
    )

    buttons = [
        [InlineKeyboardButton(
            text=f"🤖 Авто — каждому свой ({auto_count} чел.)",
            callback_data="ecamp_guide_AUTO",
        )],
    ]

    for g in catalog:
        gid = g.get("id", "")
        title = g.get("title", gid)[:35]
        cb = f"ecamp_guide_{gid}"
        if len(cb.encode("utf-8")) > 64:
            cb = cb[:64]
        buttons.append([InlineKeyboardButton(text=f"📚 {title}", callback_data=cb)])

    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="ecamp_cancel")])

    await callback.message.edit_text(
        f"📧 <b>Сегмент:</b> {seg_label} ({len(audience)} чел.)\n\n"
        "📚 <b>Какой гайд рекомендовать в письме?</b>\n\n"
        "<i>🤖 Авто = каждый получит персональный гайд "
        "на основе своих скачиваний.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await state.set_state(CampaignStates.choose_guide)


@router.callback_query(F.data.startswith("ecamp_guide_"), CampaignStates.choose_guide)
async def campaign_guide_chosen(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Гайд выбран → показываем превью письма и подтверждение."""
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()

    guide_id = callback.data.removeprefix("ecamp_guide_")
    is_auto = guide_id == "AUTO"

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    data = await state.get_data()
    seg_label = data.get("campaign_seg_label", "?")
    audience_count = data.get("campaign_audience_count", 0)

    # Генерируем campaign_id
    ts = datetime.now(timezone.utc).strftime("%d%m")
    campaign_id = f"retarget_auto_{ts}" if is_auto else f"retarget_{guide_id}_{ts}"

    await state.update_data(
        campaign_guide_id=guide_id,
        campaign_id=campaign_id,
        campaign_auto=is_auto,
    )

    bot_info = await bot.get_me()

    if is_auto:
        # Показываем распределение рекомендаций
        leads = await google.get_recent_leads(limit=5000)
        segment = data.get("campaign_segment", "all")
        if segment == "all":
            audience = _build_audience(leads)
        elif segment == "hot":
            audience = _build_audience(leads, warmth_filter="Hot")
        else:
            audience = _build_audience(leads, target_tags=[segment])

        guide_dist: dict[str, int] = {}
        no_guide = 0
        for u in audience:
            best = _pick_best_guide_for_user(u, catalog)
            if best:
                title = best.get("title", best.get("id", "?"))[:25]
                guide_dist[title] = guide_dist.get(title, 0) + 1
            else:
                no_guide += 1

        dist_lines = []
        for title, cnt in sorted(guide_dist.items(), key=lambda x: -x[1])[:8]:
            dist_lines.append(f"  📚 {_esc(title)} → <b>{cnt}</b> чел.")

        await callback.message.edit_text(
            f"📧 <b>Авто-рекомендация</b>\n\n"
            f"🎯 Сегмент: <b>{seg_label}</b>\n"
            f"👥 Получателей: <b>{audience_count}</b>\n"
            f"🤖 Режим: <b>персональный гайд для каждого</b>\n"
            f"🆔 Campaign: <code>{campaign_id}</code>\n\n"
            f"📊 <b>Распределение:</b>\n"
            + "\n".join(dist_lines)
            + (f"\n  ⏭ Без рекомендации (всё скачали): {no_guide}" if no_guide else "")
            + f"\n\nUTM: <code>src_email / cmp_{campaign_id}</code>\n\n"
            "Отправить?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Отправить", callback_data="ecamp_send"),
                    InlineKeyboardButton(text="❌ Отмена", callback_data="ecamp_cancel"),
                ],
                [InlineKeyboardButton(
                    text="👁 Тестовое письмо (себе)",
                    callback_data="ecamp_test",
                )],
            ]),
        )
    else:
        guide = None
        for g in catalog:
            if str(g.get("id", "")) == guide_id:
                guide = g
                break

        if not guide:
            await callback.message.edit_text("❌ Гайд не найден.")
            await state.clear()
            return

        # Превью письма
        subject, _ = build_retarget_email(
            name="Айдар",
            guide=guide,
            bot_username=bot_info.username,
            campaign_id=campaign_id,
        )

        guide_title = guide.get("title", guide_id)

        await callback.message.edit_text(
            f"📧 <b>Превью email-кампании</b>\n\n"
            f"🎯 Сегмент: <b>{seg_label}</b>\n"
            f"👥 Получателей: <b>{audience_count}</b>\n"
            f"📚 Гайд: <b>{_esc(guide_title)}</b>\n"
            f"📝 Тема: <i>{_esc(subject)}</i>\n"
            f"🆔 Campaign: <code>{campaign_id}</code>\n\n"
            f"UTM в ссылке: <code>src_email, cmp_{campaign_id}</code>\n\n"
            "Отправить?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Отправить", callback_data="ecamp_send"),
                    InlineKeyboardButton(text="❌ Отмена", callback_data="ecamp_cancel"),
                ],
                [InlineKeyboardButton(
                    text="👁 Тестовое письмо (себе)",
                    callback_data="ecamp_test",
                )],
            ]),
        )

    await state.set_state(CampaignStates.confirm)


@router.callback_query(F.data == "ecamp_test", CampaignStates.confirm)
async def campaign_test_email(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Отправляет тестовое письмо админу."""
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer("Отправляю тестовое письмо...")

    data = await state.get_data()
    guide_id = data.get("campaign_guide_id", "")
    campaign_id = data.get("campaign_id", "")
    is_auto = data.get("campaign_auto", False)

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    bot_info = await bot.get_me()

    # Получаем email админа из лидов
    from src.database.crud import get_lead_by_user_id
    admin_lead = await get_lead_by_user_id(settings.ADMIN_ID)
    if not admin_lead or not admin_lead.email:
        await callback.message.answer(
            "❌ Не найден email админа в БД. "
            "Сначала пройдите lead form в боте."
        )
        return

    if is_auto:
        # Для авто-режима подбираем лучший гайд для админа
        leads = await google.get_recent_leads(limit=5000)
        admin_audience = _build_audience(leads)
        admin_user = next(
            (u for u in admin_audience if u["user_id"] == settings.ADMIN_ID),
            {"guides": set(), "interests": set()},
        )
        guide = _pick_best_guide_for_user(admin_user, catalog)
        if not guide:
            guide = catalog[0] if catalog else None
    else:
        guide = next((g for g in catalog if str(g.get("id", "")) == guide_id), None)

    if not guide:
        await callback.message.answer("❌ Гайд не найден.")
        return

    subject, html_body = build_retarget_email(
        name=admin_lead.name or "Admin",
        guide=guide,
        bot_username=bot_info.username,
        campaign_id=f"test_{campaign_id}",
    )

    from src.bot.utils.email_sender import send_email
    ok = await send_email(admin_lead.email, f"[TEST] {subject}", html_body)

    guide_title = guide.get("title", "?")
    if ok:
        await callback.message.answer(
            f"✅ Тестовое письмо отправлено на <code>{admin_lead.email}</code>\n"
            f"📚 Гайд в письме: <b>{_esc(guide_title)}</b>"
            + (" (авто-подбор)" if is_auto else "")
        )
    else:
        await callback.message.answer("❌ Ошибка отправки. Проверьте логи.")


@router.callback_query(F.data == "ecamp_send", CampaignStates.confirm)
async def campaign_send(
    callback: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Отправка email-кампании всем пользователям сегмента."""
    if not _is_admin(callback.from_user.id):
        return
    await callback.answer()

    data = await state.get_data()
    segment = data.get("campaign_segment", "all")
    guide_id = data.get("campaign_guide_id", "")
    campaign_id = data.get("campaign_id", "")
    is_auto = data.get("campaign_auto", False)
    await state.clear()

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    if not is_auto:
        guide_fixed = next(
            (g for g in catalog if str(g.get("id", "")) == guide_id), None,
        )
        if not guide_fixed:
            await callback.message.edit_text("❌ Гайд не найден.")
            return
    else:
        guide_fixed = None

    # Строим аудиторию
    leads = await google.get_recent_leads(limit=5000)
    if segment == "all":
        audience = _build_audience(leads)
    elif segment == "hot":
        audience = _build_audience(leads, warmth_filter="Hot")
    else:
        audience = _build_audience(leads, target_tags=[segment])

    if not audience:
        await callback.message.edit_text("📧 Аудитория пуста.")
        return

    bot_info = await bot.get_me()
    total = len(audience)
    sent = 0
    failed = 0
    skipped = 0

    status_msg = await callback.message.edit_text(
        f"⏳ Email-кампания запущена: 0/{total}..."
    )

    from src.bot.utils.email_sender import send_email

    for i, user in enumerate(audience, 1):
        email = user["email"]
        name = user.get("name") or "Коллега"

        if is_auto:
            guide = _pick_best_guide_for_user(user, catalog)
            if not guide:
                skipped += 1
                continue
        else:
            guide = guide_fixed
            if guide_id in user.get("guides", set()):
                skipped += 1
                continue

        subject, html_body = build_retarget_email(
            name=name,
            guide=guide,
            bot_username=bot_info.username,
            campaign_id=campaign_id,
            sphere=user.get("sphere", ""),
        )

        try:
            ok = await send_email(email, subject, html_body)
            if ok:
                sent += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            logger.warning("Email campaign send failed for %s: %s", email[:20], e)

        # Прогресс каждые 5 писем
        if i % 5 == 0 or i == len(audience):
            try:
                await status_msg.edit_text(
                    f"⏳ Email-кампания: {i}/{total}\n"
                    f"✅ Отправлено: {sent}\n"
                    f"⏭ Пропущено: {skipped}\n"
                    f"❌ Ошибок: {failed}"
                )
            except Exception:
                pass

        # Rate limit (Resend: 10/sec, SMTP: varies)
        await asyncio.sleep(0.15)

    # Лог кампании
    mode = "auto" if is_auto else guide_id
    campaign_log = {
        "campaign_id": campaign_id,
        "segment": segment,
        "mode": mode,
        "total": total,
        "sent": sent,
        "skipped": skipped,
        "failed": failed,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info("Email campaign completed: %s", campaign_log)

    # Пишем лог в Sheets
    try:
        await google.log_email_campaign(
            campaign_id=campaign_id,
            segment=segment,
            guide_id=mode,
            total=total,
            sent=sent,
            failed=failed,
        )
    except Exception as e:
        logger.warning("Failed to log campaign to Sheets: %s", e)

    await status_msg.edit_text(
        f"✅ <b>Email-кампания завершена!</b>\n\n"
        f"🆔 {campaign_id}\n"
        f"🤖 Режим: <b>{'авто-подбор' if is_auto else guide_id}</b>\n"
        f"📊 Всего: {total}\n"
        f"✅ Отправлено: {sent}\n"
        f"⏭ Пропущено: {skipped}\n"
        f"❌ Ошибок: {failed}\n\n"
        f"UTM: <code>src_email / cmp_{campaign_id}</code>\n"
        "Отслеживайте переходы через /sources"
    )


@router.callback_query(F.data == "ecamp_cancel")
async def campaign_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.edit_text("❌ Кампания отменена.")
    await callback.answer()


# ═══════════════════════════════════════════════════════════════════════════
#  Автоматическая рассылка — еженедельный дайджест новых гайдов
# ═══════════════════════════════════════════════════════════════════════════

_last_known_guide_ids: set[str] = set()


async def auto_email_retarget(
    *,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Автоматическая email-рассылка: каждому — персональный гайд.

    Запускается по расписанию (еженедельно). Отправляет
    персонализированные рекомендации тем пользователям, которые
    давно не скачивали гайды или есть не скачанные новинки.
    """
    from src.bot.utils.email_sender import is_email_configured, send_email

    if not is_email_configured():
        logger.info("Auto email retarget skipped — email not configured")
        return

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    leads = await google.get_recent_leads(limit=5000)
    audience = _build_audience(leads)

    if not audience:
        logger.info("Auto email retarget: no audience with emails")
        return

    bot_info = await bot.get_me()
    ts = datetime.now(timezone.utc).strftime("%d%m")
    campaign_id = f"auto_weekly_{ts}"

    sent = 0
    skipped = 0

    for user in audience:
        guide = _pick_best_guide_for_user(user, catalog)
        if not guide:
            skipped += 1
            continue

        name = user.get("name") or "Коллега"
        subject, html_body = build_retarget_email(
            name=name,
            guide=guide,
            bot_username=bot_info.username,
            campaign_id=campaign_id,
            sphere=user.get("sphere", ""),
        )

        try:
            ok = await send_email(user["email"], subject, html_body)
            if ok:
                sent += 1
        except Exception as e:
            logger.warning("Auto retarget failed for %s: %s", user["email"][:20], e)

        await asyncio.sleep(0.15)

    logger.info(
        "Auto email retarget done: campaign=%s sent=%d skipped=%d",
        campaign_id, sent, skipped,
    )

    # Уведомляем админа
    if sent > 0:
        try:
            await bot.send_message(
                settings.ADMIN_ID,
                f"📧 <b>Авто-рассылка завершена</b>\n\n"
                f"🆔 {campaign_id}\n"
                f"✅ Отправлено: {sent}\n"
                f"⏭ Пропущено (всё скачали): {skipped}\n\n"
                f"UTM: <code>src_email / cmp_{campaign_id}</code>",
            )
        except Exception:
            pass

    # Логируем в Sheets
    try:
        await google.log_email_campaign(
            campaign_id=campaign_id,
            segment="auto_all",
            guide_id="auto_personalized",
            total=len(audience),
            sent=sent,
            failed=skipped,
        )
    except Exception as e:
        logger.warning("Failed to log auto campaign to Sheets: %s", e)
