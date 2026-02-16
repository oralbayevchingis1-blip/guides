"""Ежедневный дайджест ключевых метрик для product owner'а.

Собирает данные из воронки, лидов, пользователей и отправляет
в Telegram-чат команды по расписанию.
"""

import logging
from datetime import datetime, timedelta, timezone

from src.config import settings

logger = logging.getLogger(__name__)

FUNNEL_ORDER = [
    ("bot_start", "▶ Старт"),
    ("view_guide", "📚 Просмотр"),
    ("click_download", "📥 Скачивание"),
    ("email_submitted", "📧 Email"),
    ("pdf_delivered", "📄 PDF"),
    ("consultation", "📞 Консультация"),
]


async def build_daily_digest(hours: int = 24) -> str:
    """Формирует HTML-текст ежедневного дайджеста."""
    from src.database.crud import (
        get_active_users_count,
        get_consultations_count,
        get_funnel_stats,
        get_funnel_by_source,
        get_new_leads_count,
        get_new_users_count,
        get_top_guides_period,
        get_total_users_count,
    )

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%d.%m.%Y")

    # Текущий период
    new_users = await get_new_users_count(hours)
    active_users = await get_active_users_count(hours)
    total_users = await get_total_users_count()
    new_leads = await get_new_leads_count(hours)
    consultations = await get_consultations_count(hours)
    funnel = await get_funnel_stats(hours)
    top_guides = await get_top_guides_period(hours, limit=5)

    # Предыдущий период (для сравнения)
    prev_users = await get_new_users_count(hours * 2) - new_users
    prev_leads = await get_new_leads_count(hours * 2) - new_leads

    def _delta(current: int, previous: int) -> str:
        if previous <= 0:
            return ""
        diff = current - previous
        pct = diff / previous * 100 if previous else 0
        arrow = "📈" if diff > 0 else "📉" if diff < 0 else "➡️"
        return f" {arrow} {pct:+.0f}%"

    # Воронка
    funnel_map = {step: users for step, users, _ in funnel}
    funnel_lines = []
    prev_count = None
    for step_key, label in FUNNEL_ORDER:
        count = funnel_map.get(step_key, 0)
        if count == 0 and prev_count == 0:
            continue
        conv = ""
        if prev_count and prev_count > 0:
            rate = count / prev_count * 100
            conv = f"  ({rate:.0f}%)"
        funnel_lines.append(f"  {label:15s} → <b>{count}</b>{conv}")
        prev_count = count

    # Итоговая конверсия
    starts = funnel_map.get("bot_start", 0)
    pdfs = funnel_map.get("pdf_delivered", 0)
    total_conv = f"{pdfs / starts * 100:.1f}%" if starts > 0 else "—"

    # Топ гайдов
    guide_lines = []
    for i, (gid, cnt) in enumerate(top_guides, 1):
        guide_lines.append(f"  {i}. {gid} — <b>{cnt}</b>")

    # Источники (top 5)
    by_source = await get_funnel_by_source(hours=hours)
    source_lines = []
    if by_source:
        sorted_sources = sorted(
            by_source.items(),
            key=lambda x: x[1].get("bot_start", 0),
            reverse=True,
        )[:5]
        for src, steps in sorted_sources:
            src_starts = steps.get("bot_start", 0)
            src_name = src[:25] if len(src) <= 25 else src[:22] + "…"
            source_lines.append(f"  {src_name} — <b>{src_starts}</b> чел.")

    # Узкие места
    bottleneck = ""
    worst_rate = 100.0
    worst_label = ""
    prev_u = None
    for step_key, label in FUNNEL_ORDER:
        count = funnel_map.get(step_key, 0)
        if prev_u and prev_u > 0:
            rate = count / prev_u * 100
            if rate < worst_rate:
                worst_rate = rate
                worst_label = label
        prev_u = count

    if worst_label and worst_rate < 70:
        bottleneck = f"\n⚠️ <b>Узкое место:</b> {worst_label} ({worst_rate:.0f}%)"

    period_label = "24ч" if hours == 24 else f"{hours // 24}д" if hours >= 24 else f"{hours}ч"

    text = (
        f"📊 <b>Дайджест за {date_str}</b> ({period_label})\n"
        f"{'━' * 28}\n\n"
        f"👥 <b>Пользователи</b>\n"
        f"  Новых: <b>{new_users}</b>{_delta(new_users, prev_users)}\n"
        f"  Активных: <b>{active_users}</b>\n"
        f"  Всего: <b>{total_users}</b>\n\n"
        f"🔥 <b>Воронка</b>\n"
    )
    text += "\n".join(funnel_lines) if funnel_lines else "  нет данных"
    text += f"\n\n🎯 Конверсия старт→PDF: <b>{total_conv}</b>"

    text += f"\n\n📧 <b>Лиды:</b> <b>{new_leads}</b>{_delta(new_leads, prev_leads)}"
    text += f"\n📞 <b>Консультации:</b> <b>{consultations}</b>"

    if guide_lines:
        text += "\n\n📈 <b>Топ гайдов</b>\n" + "\n".join(guide_lines)

    if source_lines:
        text += "\n\n📍 <b>Топ источников</b>\n" + "\n".join(source_lines)

    if bottleneck:
        text += bottleneck

    text += "\n\n💡 /funnel 7d — подробная воронка за неделю"

    return text


async def build_weekly_digest() -> str:
    """Недельный дайджест (7 дней)."""
    return await build_daily_digest(hours=168)


# ── Планирование дайджеста ───────────────────────────────────────────


async def ensure_digest_scheduled() -> None:
    """Создаёт scheduled_task для ежедневного дайджеста, если его нет."""
    if not settings.DIGEST_ENABLED:
        logger.info("Digest disabled — skipping schedule")
        return

    from src.database.crud import create_scheduled_task
    from src.database.models import async_session, ScheduledTask
    from sqlalchemy import select

    # Проверяем, есть ли уже pending digest
    async with async_session() as session:
        stmt = select(ScheduledTask).where(
            ScheduledTask.task_type == "daily_digest",
            ScheduledTask.status == "pending",
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()

    if existing:
        logger.info("Digest already scheduled (task #%s)", existing.id)
        return

    # Создаём задачу на следующий digest hour
    now = datetime.now(timezone.utc)
    target = now.replace(hour=settings.DIGEST_HOUR, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)

    await create_scheduled_task(
        task_type="daily_digest",
        user_id=settings.ADMIN_ID,
        run_at=target,
        payload={"hours": 24},
    )
    logger.info("Daily digest scheduled for %s", target.isoformat())


async def schedule_next_digest() -> None:
    """Планирует следующий ежедневный дайджест (self-rescheduling)."""
    if not settings.DIGEST_ENABLED:
        return

    from src.database.crud import create_scheduled_task

    now = datetime.now(timezone.utc)
    target = now.replace(hour=settings.DIGEST_HOUR, minute=0, second=0, microsecond=0)
    target += timedelta(days=1)

    # Воскресенье — недельный
    is_sunday = target.weekday() == 6
    payload_hours = 168 if is_sunday else 24

    await create_scheduled_task(
        task_type="daily_digest",
        user_id=settings.ADMIN_ID,
        run_at=target,
        payload={"hours": payload_hours},
    )
    logger.info("Next digest scheduled: %s (hours=%d)", target.isoformat(), payload_hours)
