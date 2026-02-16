"""Админ-команды бота (только для ADMIN_ID)."""

import io
import logging
from datetime import datetime, timezone, timedelta

from aiogram import Bot, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import BufferedInputFile, Message, InlineKeyboardMarkup, InlineKeyboardButton

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_drive import clear_pdf_cache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.monitoring import metrics
from src.bot.utils.throttle import critical_limiter, throttle_mw
from src.config import settings
from src.bot.utils.smart_recommendations import smart_recommender
from src.database.crud import (
    cancel_tasks_for_user,
    count_pending_tasks,
    delete_leads_for_user,
    delete_user,
    get_active_users_count,
    get_consultations_count,
    get_funnel_by_source,
    get_funnel_stats,
    get_new_leads_count,
    get_new_users_count,
    get_top_guides_period,
    get_total_users_count,
    get_traffic_source_stats,
)

router = Router()
logger = logging.getLogger(__name__)


@router.message(Command("refresh"))
async def cmd_refresh(message: Message, cache: TTLCache) -> None:
    """Сброс кеша — бот подтянет свежие данные из Google Sheets."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    cache.invalidate()
    pdf_count = clear_pdf_cache()

    logger.info("Кеш сброшен администратором (user_id=%s), PDF: %d", message.from_user.id, pdf_count)
    await message.answer(
        f"✅ Кеш сброшен.\n"
        f"• Тексты и каталог обновятся при следующем запросе\n"
        f"• PDF-кеш очищен ({pdf_count} файлов)"
    )


@router.message(Command("test_flow"))
async def cmd_test_flow(message: Message, state: FSMContext, cache: TTLCache) -> None:
    """Сброс себя до 'нового пользователя' и перезапуск /start флоу."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    user_id = message.from_user.id

    # Удаляем лиды (бот будет думать, что email не собран)
    leads_deleted = await delete_leads_for_user(user_id)
    # Удаляем запись пользователя (бот создаст заново)
    user_deleted = await delete_user(user_id)
    # Отменяем pending follow-up задачи
    tasks_cancelled = await cancel_tasks_for_user(user_id)
    # Чистим FSM
    await state.clear()
    # Сбрасываем кеш
    cache.invalidate()

    logger.info(
        "Test flow reset: user_id=%s, leads=%d, user=%s, tasks=%d",
        user_id, leads_deleted, user_deleted, tasks_cancelled,
    )

    await message.answer(
        "🧪 <b>Тестовый сброс выполнен!</b>\n\n"
        f"• Лидов удалено: {leads_deleted}\n"
        f"• Профиль удалён: {'да' if user_deleted else 'нет'}\n"
        f"• Follow-up задач отменено: {tasks_cancelled}\n"
        f"• FSM очищен\n"
        f"• Кеш сброшен\n\n"
        "Теперь нажмите /start — бот будет вести себя как с новым пользователем.\n"
        "Пройдёте весь флоу: подписка → email → имя → согласие → гайд."
    )


@router.message(Command("report"))
async def cmd_report(
    message: Message,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Простой отчёт: лиды за сегодня, пользователи в БД."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    await message.answer("📊 Собираю данные...")

    try:
        now = datetime.now(timezone.utc)
        today_str = now.strftime("%d.%m.%Y")

        # Лиды за сегодня
        leads = await google.get_recent_leads(limit=100)
        today_leads = [
            l for l in leads
            if l.get("timestamp", "").startswith(today_str)
            or l.get("timestamp", "")[:10] == now.strftime("%Y-%m-%d")
        ]

        # Пользователи в БД
        try:
            from src.database.models import async_session, User
            from sqlalchemy import select, func as sa_func

            async with async_session() as session:
                total_users = (await session.execute(
                    select(sa_func.count(User.id))
                )).scalar() or 0

                active_24h = (await session.execute(
                    select(sa_func.count(User.id)).where(
                        User.last_activity >= now - timedelta(hours=24)
                    )
                )).scalar() or 0
        except Exception:
            total_users = 0
            active_24h = 0

        report = (
            f"📊 <b>Отчёт за {today_str}</b>\n"
            f"{'─' * 28}\n\n"
            f"👥 <b>Пользователи:</b>\n"
            f"  • Всего в базе: <b>{total_users}</b>\n"
            f"  • Активных за 24ч: <b>{active_24h}</b>\n\n"
            f"🔥 <b>Лиды сегодня:</b> <b>{len(today_leads)}</b>\n"
        )

        # Последние лиды
        if today_leads:
            report += "\n📝 <b>Последние:</b>\n"
            for lead in today_leads[:5]:
                name = lead.get("name", lead.get("Имя", "?"))
                guide = lead.get("guide", lead.get("Гайд", "?"))
                report += f"  • {name} → {guide}\n"

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text="📊 Открыть CRM",
                    url=f"https://docs.google.com/spreadsheets/d/{settings.GOOGLE_SPREADSHEET_ID}/edit",
                )],
            ]
        )

        await message.answer(report, reply_markup=keyboard)
        logger.info("Report generated for admin")

    except Exception as e:
        logger.error("Report error: %s", e)
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("health"))
async def cmd_health(
    message: Message,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Дашборд здоровья бота: метрики, ошибки, статус сервисов."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    # Метрики
    m = metrics.get_all()
    err_rate = metrics.error_rate(300)

    # Pending задачи
    try:
        pending = await count_pending_tasks()
    except Exception:
        pending = -1

    # Google Sheets пинг
    sheets_ok = True
    try:
        await google.get_bot_texts()
    except Exception:
        sheets_ok = False

    # БД пользователей
    try:
        from src.database.models import async_session, User
        from sqlalchemy import select, func as sa_func
        async with async_session() as session:
            total_users = (await session.execute(
                select(sa_func.count(User.id))
            )).scalar() or 0
    except Exception:
        total_users = "?"

    status_emoji = "🟢" if err_rate < 2.0 and sheets_ok else "🟡" if err_rate < 5.0 else "🔴"
    sheets_status = "🟢 OK" if sheets_ok else "🔴 FAIL"

    recent_errs = metrics.recent_errors(300)
    err_lines = ""
    if recent_errs:
        top = sorted(recent_errs.items(), key=lambda x: -x[1])[:5]
        err_lines = "\n".join(f"    • {name}: {cnt}" for name, cnt in top)
    else:
        err_lines = "    нет ошибок"

    text = (
        f"{status_emoji} <b>Здоровье бота</b>\n"
        f"{'─' * 28}\n\n"
        f"⏱ Uptime: <b>{metrics.uptime_str()}</b>\n"
        f"📅 Запущен: {metrics.started_at_str()}\n\n"
        f"<b>📊 Ключевые метрики:</b>\n"
        f"  /start: <b>{m.get('cmd.start', 0)}</b>\n"
        f"  Загрузок: <b>{m.get('downloads_initiated', 0)}</b>\n"
        f"  Подписок: <b>{m.get('subscription_checks', 0)}</b>\n"
        f"  Согласий: <b>{m.get('consents_given', 0)}</b>\n"
        f"  Консультаций: <b>{m.get('consultations_booked', 0)}</b>\n"
        f"  Всего updates: <b>{m.get('updates_total', 0)}</b>\n\n"
        f"<b>⚠️ Ошибки (5 мин):</b>\n"
        f"  Rate: <b>{err_rate:.1f}/мин</b>\n"
        f"{err_lines}\n\n"
        f"<b>🛡 Rate limiting:</b>\n"
        f"  Throttled (общий): <b>{throttle_mw.total_throttled}</b>\n"
        f"  Passed: <b>{throttle_mw.total_passed}</b>\n"
        f"  Critical blocked: <b>{critical_limiter.total_blocked}</b>\n"
        f"  Throttle events: <b>{m.get('throttled_total', 0)}</b>\n\n"
        f"<b>🔧 Сервисы:</b>\n"
        f"  Google Sheets: {sheets_status}\n"
        f"  Sheets API calls: {m.get('sheets.success', 0)} ok / {m.get('error.sheets_api', 0)} err\n"
        f"  Pending tasks: <b>{pending}</b>\n"
        f"  Users in DB: <b>{total_users}</b>\n"
    )

    # Секция: структура Sheets
    schema_emoji = "🟢" if google.schema_ok else "🔴"
    cached_count = len(google._cached_headers)
    text += (
        f"\n<b>📋 Sheets Schema:</b>\n"
        f"  Статус: {schema_emoji} {'OK' if google.schema_ok else 'DRIFT'}\n"
        f"  Листов проверено: <b>{cached_count}</b>\n"
    )
    if google.schema_warnings:
        for w in google.schema_warnings[:8]:
            text += f"  {w}\n"

    text += (
        "\n💡 /funnel — аналитика воронки"
        "\n💡 /sources — каналы трафика"
        "\n💡 /recommendations — умные рекомендации"
        "\n💡 /profiles — профили пользователей"
        "\n💡 /questions — вопросы юристу"
        "\n💡 /digest — ежедневный дайджест"
    )

    await message.answer(text)


@router.message(Command("export_audience"))
async def cmd_export_audience(
    message: Message,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Экспорт аудиторий для ретаргетинга (Facebook/Instagram Custom Audiences).

    Формирует CSV-файлы с email-адресами, сегментированными по категориям
    скачанных гайдов. Готовы к загрузке в Facebook Ads Manager.

    Использование: /export_audience [категория]
    Без аргумента — экспортирует все сегменты.
    """
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    await message.answer("📊 Собираю аудитории для ретаргетинга...")

    try:
        from src.database.models import async_session, Lead
        from sqlalchemy import select

        # Загружаем каталог для маппинга guide_id → category
        catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
        guide_to_cat: dict[str, str] = {}
        for g in catalog:
            gid = str(g.get("id", ""))
            cat = g.get("category", "Без категории").strip()
            if gid:
                guide_to_cat[gid] = cat

        # Загружаем все лиды из БД
        async with async_session() as session:
            stmt = select(
                Lead.email, Lead.selected_guide, Lead.name, Lead.user_id,
            ).where(Lead.selected_guide != "__consultation__")
            result = await session.execute(stmt)
            rows = result.all()

        if not rows:
            await message.answer("📭 В базе нет лидов для экспорта.")
            return

        # Группируем по категориям
        segments: dict[str, set[str]] = {}
        all_emails: set[str] = set()

        for email, guide_id, name, uid in rows:
            if not email or "@" not in email:
                continue
            cat = guide_to_cat.get(guide_id, "Без категории")
            segments.setdefault(cat, set()).add(email.lower())
            all_emails.add(email.lower())

        # Определяем, какой аргумент передан
        args = (message.text or "").replace("/export_audience", "").strip()

        if args:
            # Экспорт одного сегмента
            matched_cat = None
            for cat_name in segments:
                if args.lower() in cat_name.lower():
                    matched_cat = cat_name
                    break

            if not matched_cat:
                cats_list = "\n".join(f"  • {c} ({len(e)} чел.)" for c, e in segments.items())
                await message.answer(
                    f"❌ Категория «{args}» не найдена.\n\n"
                    f"Доступные сегменты:\n{cats_list}"
                )
                return

            emails = segments[matched_cat]
            csv = _build_csv(emails)
            filename = f"audience_{matched_cat.replace(' ', '_').lower()}.csv"
            doc = BufferedInputFile(csv.encode("utf-8"), filename=filename)
            await message.answer_document(
                document=doc,
                caption=(
                    f"📊 <b>Сегмент: {matched_cat}</b>\n"
                    f"👥 Email-адресов: <b>{len(emails)}</b>\n\n"
                    "Загрузите в Facebook Ads → Audiences → Create Custom Audience → "
                    "Customer list."
                ),
            )
        else:
            # Экспорт всех сегментов
            summary_parts = [f"📊 <b>Аудитории для ретаргетинга</b>\n"]
            summary_parts.append(f"Всего уникальных email: <b>{len(all_emails)}</b>\n")

            for cat_name, emails in sorted(segments.items(), key=lambda x: -len(x[1])):
                summary_parts.append(f"  • {cat_name}: <b>{len(emails)}</b>")

            await message.answer("\n".join(summary_parts))

            # Общий CSV со всеми email
            csv_all = _build_csv(all_emails)
            doc_all = BufferedInputFile(csv_all.encode("utf-8"), filename="audience_all.csv")
            await message.answer_document(
                document=doc_all,
                caption=(
                    f"📎 <b>Все email</b> ({len(all_emails)} шт.)\n\n"
                    "Для посегментного экспорта:\n"
                    "<code>/export_audience название_категории</code>"
                ),
            )

        logger.info("Audience export: %d emails, %d segments", len(all_emails), len(segments))

    except Exception as e:
        logger.error("Export audience error: %s", e, exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("sources"))
async def cmd_sources(message: Message) -> None:
    """Статистика по источникам трафика (UTM / deep links)."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    try:
        stats = await get_traffic_source_stats()
        if not stats:
            await message.answer("📊 Данных по источникам пока нет.")
            return

        total = sum(count for _, count in stats)
        lines = ["📊 <b>Источники трафика</b>\n"]

        for source, count in stats[:20]:
            pct = count / total * 100
            bar = "█" * max(1, round(pct / 5))
            src_display = source if len(source) <= 35 else source[:32] + "…"
            lines.append(f"<code>{src_display:35s}</code> {bar} {count} ({pct:.1f}%)")

        lines.append(f"\n<b>Итого:</b> {total} пользователей с меткой")
        lines.append(
            "\n💡 <i>Формат deep link:</i>\n"
            "<code>?start=guide_ID--src_SOURCE--med_MEDIUM--cmp_CAMPAIGN</code>\n"
            "Или короткий: <code>?start=guide_ID--SOURCE</code>"
        )

        await message.answer("\n".join(lines))
    except Exception as e:
        logger.error("Sources stats error: %s", e, exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


FUNNEL_LABELS = {
    "bot_start": "▶ Старт бота",
    "view_categories": "📂 Категории",
    "view_category": "📂 Категория",
    "view_guide": "📚 Карточка гайда",
    "click_download": "📥 Нажал «Скачать»",
    "sub_prompt": "🔔 Барьер: подписка",
    "sub_confirmed": "✅ Подписался",
    "email_prompt": "📧 Барьер: email",
    "email_submitted": "📧 Ввёл email",
    "consent_given": "✅ Дал согласие",
    "pdf_delivered": "📄 Получил PDF",
    "consultation": "📞 Консультация",
}


@router.message(Command("funnel"))
async def cmd_funnel(message: Message) -> None:
    """Аналитика воронки с конверсией между шагами.

    Использование:
        /funnel           — за последние 24 часа
        /funnel 7d        — за 7 дней
        /funnel 30d       — за 30 дней
        /funnel 24h src   — за 24ч, разбивка по источникам
    """
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    args = (message.text or "").split()[1:]  # /funnel 7d src
    hours = 24
    show_sources = False

    for arg in args:
        arg_lower = arg.lower()
        if arg_lower.endswith("d") and arg_lower[:-1].isdigit():
            hours = int(arg_lower[:-1]) * 24
        elif arg_lower.endswith("h") and arg_lower[:-1].isdigit():
            hours = int(arg_lower[:-1])
        elif arg_lower in ("src", "sources", "source"):
            show_sources = True

    try:
        stats = await get_funnel_stats(hours=hours)

        if not stats:
            await message.answer(
                f"📊 Нет данных воронки за последние {_format_period(hours)}.\n\n"
                "Данные появятся, когда пользователи будут проходить воронку."
            )
            return

        period = _format_period(hours)
        lines = [f"📊 <b>Воронка ({period})</b>\n"]

        # Находим максимум для шкалы
        max_users = max(u for _, u, _ in stats) if stats else 1

        prev_users = None
        for step, users, events in stats:
            label = FUNNEL_LABELS.get(step, step)
            bar_len = max(1, round(users / max_users * 12))
            bar = "█" * bar_len + "░" * (12 - bar_len)

            conv = ""
            if prev_users and prev_users > 0:
                rate = users / prev_users * 100
                if rate < 50:
                    conv = f"  ⚠️ {rate:.0f}%"
                else:
                    conv = f"  → {rate:.0f}%"

            lines.append(f"{bar} <b>{users}</b> {label}{conv}")
            prev_users = users

        # Итоговая конверсия
        first = stats[0][1] if stats else 0
        last_delivery = next((u for s, u, _ in stats if s == "pdf_delivered"), 0)
        if first > 0 and last_delivery > 0:
            total_conv = last_delivery / first * 100
            lines.append(f"\n🎯 <b>Конверсия старт→PDF: {total_conv:.1f}%</b>")

        # Bottleneck
        worst_step = None
        worst_rate = 100.0
        prev_u = None
        for step, users, _ in stats:
            if prev_u and prev_u > 0:
                rate = users / prev_u * 100
                if rate < worst_rate:
                    worst_rate = rate
                    worst_step = step
            prev_u = users

        if worst_step and worst_rate < 80:
            label = FUNNEL_LABELS.get(worst_step, worst_step)
            lines.append(f"\n🔻 <b>Узкое место:</b> {label} ({worst_rate:.0f}%)")

        lines.append(
            f"\n💡 <code>/funnel 7d</code> — за неделю\n"
            f"<code>/funnel 30d src</code> — за месяц + разбивка по источникам"
        )

        await message.answer("\n".join(lines))

        # Разбивка по источникам
        if show_sources:
            by_source = await get_funnel_by_source(hours=hours)
            if by_source:
                src_lines = [f"\n📊 <b>Воронка по источникам ({period})</b>\n"]
                for source, steps in sorted(by_source.items(), key=lambda x: -sum(x[1].values())):
                    starts = steps.get("bot_start", 0)
                    pdfs = steps.get("pdf_delivered", 0)
                    conv = f"{pdfs/starts*100:.0f}%" if starts > 0 else "—"
                    src_short = source[:30] if len(source) <= 30 else source[:27] + "…"
                    src_lines.append(
                        f"<code>{src_short:30s}</code> "
                        f"▶{starts} → 📄{pdfs} ({conv})"
                    )
                await message.answer("\n".join(src_lines))

    except Exception as e:
        logger.error("Funnel stats error: %s", e, exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


def _format_period(hours: int) -> str:
    if hours < 24:
        return f"{hours}ч"
    days = hours // 24
    if days == 1:
        return "24ч"
    return f"{days}д"


@router.message(Command("promo"))
async def cmd_promo(
    message: Message,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Генерация промо-материалов: /promo <guide_id>.

    Без аргумента — показывает список гайдов.
    """
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    args = (message.text or "").split(maxsplit=1)
    guide_id = args[1].strip() if len(args) > 1 else ""

    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)

    if not guide_id:
        if not catalog:
            await message.answer("📚 Каталог пуст.")
            return

        lines = ["📣 <b>Выберите гайд для промо:</b>\n"]
        buttons = []
        for g in catalog:
            gid = g.get("id", "?")
            title = g.get("title", gid)[:35]
            lines.append(f"  📄 <code>{gid}</code> — {title}")
            cb = f"adm_gpromo_{gid}"
            if len(cb.encode("utf-8")) > 64:
                cb = cb[:64]
            buttons.append([InlineKeyboardButton(
                text=f"📣 {title[:30]}",
                callback_data=cb,
            )])
        lines.append("\nИли: <code>/promo guide_id</code>")
        await message.answer(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
        return

    guide = None
    for g in catalog:
        if str(g.get("id", "")) == guide_id:
            guide = g
            break

    if not guide:
        await message.answer(f"❌ Гайд <code>{guide_id}</code> не найден в каталоге.")
        return

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

    await message.answer(
        "📣 <b>Пост для канала:</b>\n\n" + "─" * 20,
    )
    await message.answer(promo["channel_post"])

    await message.answer(
        "📝 <b>CTA-блок для статьи:</b>\n\n" + "─" * 20 + "\n\n"
        + promo["telegraph_cta"],
    )

    from src.bot.handlers.content_manager import _make_deep_link
    links = (
        "🔗 <b>Deep links с UTM:</b>\n\n"
        f"📱 Канал:\n<code>{_make_deep_link(bot_username, guide_id, 'channel')}</code>\n\n"
        f"📧 Email:\n<code>{_make_deep_link(bot_username, guide_id, 'email')}</code>\n\n"
        f"💼 LinkedIn:\n<code>{_make_deep_link(bot_username, guide_id, 'linkedin')}</code>\n\n"
        f"📘 Facebook:\n<code>{_make_deep_link(bot_username, guide_id, 'facebook')}</code>\n\n"
        f"🌐 Сайт:\n<code>{_make_deep_link(bot_username, guide_id, 'website')}</code>\n\n"
        f"📋 Короткий CTA:\n<code>{promo['short_cta']}</code>"
    )
    await message.answer(links)


@router.message(Command("digest"))
async def cmd_digest(message: Message, bot: Bot) -> None:
    """Принудительная отправка дайджеста: /digest или /digest week."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    args = (message.text or "").split()[1:]
    is_weekly = "week" in [a.lower() for a in args]

    try:
        from src.bot.utils.digest import build_daily_digest, build_weekly_digest

        await message.answer("📊 Собираю дайджест...")

        text = await build_weekly_digest() if is_weekly else await build_daily_digest()

        chat_id = settings.TEAM_CHAT_ID or settings.ADMIN_ID
        await bot.send_message(chat_id=chat_id, text=text)

        if chat_id != message.from_user.id:
            await message.answer(f"✅ Дайджест отправлен в чат {chat_id}")
        else:
            logger.info("Digest sent manually to admin")
    except Exception as e:
        logger.error("Digest error: %s", e, exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("profiles"))
async def cmd_profiles(message: Message) -> None:
    """Статистика заполненности профилей пользователей."""
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    try:
        from src.database.crud import get_profile_stats
        stats = await get_profile_stats()

        total = stats["total"]
        if not total:
            await message.answer("📊 В базе нет пользователей.")
            return

        def pct(n: int) -> str:
            return f"{n / total * 100:.0f}%" if total else "0%"

        text = (
            f"👤 <b>Профили пользователей</b>\n"
            f"{'─' * 28}\n\n"
            f"👥 Всего пользователей: <b>{total}</b>\n\n"
            f"🏢 Сфера бизнеса: <b>{stats['with_sphere']}</b> ({pct(stats['with_sphere'])})\n"
            f"👥 Размер команды: <b>{stats['with_size']}</b> ({pct(stats['with_size'])})\n"
            f"📈 Стадия бизнеса: <b>{stats['with_stage']}</b> ({pct(stats['with_stage'])})\n\n"
            f"🔥 <b>Полный профиль:</b> <b>{stats['full_profile']}</b> ({pct(stats['full_profile'])})\n\n"
            f"💡 Полный профиль = все 3 поля заполнены (Hot+ лид)"
        )
        await message.answer(text)
    except Exception as e:
        logger.error("Profiles stats error: %s", e, exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("recommendations"))
async def cmd_recommendations(
    message: Message,
    cache: TTLCache,
    google: GoogleSheetsClient,
) -> None:
    """Умные рекомендации: top co-download пар + сравнение с Sheets.

    /recommendations         — топ пар «часто скачивают вместе»
    /recommendations sync    — обновить лист «Рекомендации» в Sheets
    """
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    args = (message.text or "").split()[1:]
    do_sync = "sync" in [a.lower() for a in args]

    try:
        top_pairs = await smart_recommender.get_top_pairs(limit=15)

        if not top_pairs:
            await message.answer(
                "📊 Недостаточно данных для коллаборативной фильтрации.\n\n"
                "Нужно минимум 2 пользователя, скачавших одинаковые пары гайдов."
            )
            return

        catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
        titles: dict[str, str] = {}
        for g in catalog:
            gid = str(g.get("id", ""))
            titles[gid] = g.get("title", gid)[:30]

        lines = ["🧠 <b>Умные рекомендации — «часто скачивают вместе»</b>\n"]
        for a, b, shared in top_pairs:
            t_a = titles.get(a, a)[:25]
            t_b = titles.get(b, b)[:25]
            lines.append(f"  {t_a} ↔ {t_b}  <b>{shared}</b> чел.")

        # Сравнение с Sheets
        recommendations = await cache.get_or_fetch("recommendations", google.get_recommendations)
        lines.append(f"\n📋 <b>Маппинг в Sheets:</b> {len(recommendations)} записей")

        mismatches = 0
        for gid in titles:
            smart_rec = await smart_recommender.get_recommendation(gid, exclude=set())
            sheet_rec = recommendations.get(gid, {}).get("next_guide_id", "")
            if smart_rec and sheet_rec and smart_rec != sheet_rec:
                mismatches += 1
                lines.append(
                    f"  ⚡ {titles.get(gid, gid)[:20]}: "
                    f"Smart→<code>{smart_rec}</code> vs Sheet→<code>{sheet_rec}</code>"
                )

        if not mismatches:
            lines.append("  ✅ Совпадения или нет конфликтов")

        lines.append("\n<code>/recommendations sync</code> — обновить Sheets")
        await message.answer("\n".join(lines))

        if do_sync:
            await _sync_recommendations_to_sheets(message, google, cache)

    except Exception as e:
        logger.error("Recommendations error: %s", e, exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


async def _sync_recommendations_to_sheets(
    message: Message,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Обновляет маппинг в Sheets на основе коллаборативной фильтрации."""
    catalog = await cache.get_or_fetch("catalog", google.get_guides_catalog)
    guide_ids = [str(g.get("id", "")) for g in catalog if g.get("id")]

    mapping: dict[str, str] = {}
    for gid in guide_ids:
        rec = await smart_recommender.get_recommendation(gid, exclude=set())
        if rec:
            mapping[gid] = rec

    if not mapping:
        await message.answer("⚠️ Недостаточно данных для обновления.")
        return

    try:
        await google.update_recommendations_sheet(mapping)
        await message.answer(
            f"✅ Лист «Рекомендации» обновлён: {len(mapping)} записей."
        )
        cache.invalidate()
    except Exception as e:
        await message.answer(f"❌ Ошибка записи в Sheets: {e}")


def _build_csv(emails: set[str]) -> str:
    """Формирует CSV для Facebook Custom Audience (колонка email)."""
    buf = io.StringIO()
    buf.write("email\n")
    for email in sorted(emails):
        buf.write(f"{email}\n")
    return buf.getvalue()
