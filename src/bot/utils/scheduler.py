"""APScheduler с SQLAlchemyJobStore для persistent follow-up сообщений.

Задачи не теряются при перезагрузке бота.
"""

import logging
from datetime import datetime, timedelta, timezone

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from src.config import settings

logger = logging.getLogger(__name__)

# Глобальный экземпляр планировщика
_scheduler: AsyncIOScheduler | None = None

# SQLAlchemyJobStore требует синхронный URL (без +aiosqlite)
_SYNC_DB_URL = settings.DATABASE_URL.replace("+aiosqlite", "").replace("sqlite:///", "sqlite:///")


def get_scheduler() -> AsyncIOScheduler:
    """Возвращает (создаёт при первом вызове) планировщик с persistent storage."""
    global _scheduler
    if _scheduler is None:
        jobstores = {
            "default": SQLAlchemyJobStore(url=_SYNC_DB_URL),
        }
        _scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            timezone="UTC",
        )
    return _scheduler


def schedule_followup_series(
    user_id: int,
    guide_id: str,
    send_func,
) -> None:
    """Планирует серию follow-up сообщений после скачивания гайда.

    Args:
        user_id: Telegram ID пользователя.
        guide_id: ID скачанного гайда.
        send_func: Асинхронная функция ``send_func(user_id, guide_id, step)``
                   для отправки сообщения.
    """
    scheduler = get_scheduler()
    now = datetime.now(timezone.utc)

    # Серия сообщений: через 24ч, 3 дня, 7 дней
    delays = [
        (1, timedelta(hours=24)),
        (2, timedelta(days=3)),
        (3, timedelta(days=7)),
    ]

    for step, delta in delays:
        run_at = now + delta
        job_id = f"followup_{user_id}_{guide_id}_step{step}"

        scheduler.add_job(
            send_func,
            trigger="date",
            run_date=run_at,
            args=[user_id, guide_id, step],
            id=job_id,
            replace_existing=True,
            misfire_grace_time=3600,  # 1ч запас на misfire
        )
        logger.info(
            "Follow-up запланирован: user_id=%s, guide=%s, step=%d, run_at=%s",
            user_id, guide_id, step, run_at.isoformat(),
        )


def schedule_pending_sheets_retry(retry_func) -> None:
    """Планирует повторную отправку pending writes в Google Sheets каждые 5 минут."""
    scheduler = get_scheduler()
    job_id = "retry_pending_sheets_writes"

    if not scheduler.get_job(job_id):
        scheduler.add_job(
            retry_func,
            trigger="interval",
            minutes=5,
            id=job_id,
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info("Pending sheets retry scheduled every 5 min")
