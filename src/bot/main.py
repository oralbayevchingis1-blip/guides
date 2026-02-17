"""Точка входа Telegram-бота SOLIS Partners — AI-powered marketing hub.

v2.0: Enterprise-grade stability, deep analytics, OWASP security.
"""

import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand, BotCommandScopeChat

from src.bot.handlers import (
    admin,
    broadcast,
    cabinet,
    consult,
    consultation,
    content_manager,
    corporate,
    documents,
    email_campaigns,
    feedback,
    group_mode,
    language,
    lead_form,
    legal_tools,
    live_support,
    questions,
    referral,
    start,
    subscription,
    timezone_handler,
    voice,
    waitlist_handler,
)
from src.bot.handlers import digest, strategy
from src.bot.handlers.followup import send_followup_message
from src.bot.middlewares.logging_mw import LoggingMiddleware
from src.bot.utils.ai_client import get_orchestrator
from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.scheduler import get_scheduler
from src.config import settings
from src.database.models import init_db

logger = logging.getLogger(__name__)

# ── Module-level refs for scheduled jobs (set during main()) ────────────
_bot: Bot | None = None
_google: GoogleSheetsClient | None = None
_cache: TTLCache | None = None


async def _job_retention_check() -> None:
    from src.bot.utils.retention import check_sleeping_users
    await check_sleeping_users(bot=_bot, google=_google, cache=_cache)


async def _job_growth_report() -> None:
    from src.bot.utils.growth_report import send_growth_report
    await send_growth_report(bot=_bot, google=_google, cache=_cache)


async def _job_auto_stories() -> None:
    from src.bot.utils.stories_publisher import auto_stories_check
    await auto_stories_check(bot=_bot, google=_google, cache=_cache)


async def _job_telemetry_flush() -> None:
    from src.bot.utils.telemetry import scheduled_telemetry_flush
    await scheduled_telemetry_flush(google=_google, cache=_cache)


async def _job_funnel_analysis() -> None:
    from src.bot.utils.telemetry import weekly_funnel_analysis
    await weekly_funnel_analysis(bot=_bot, google=_google, cache=_cache)


async def _job_daily_backup() -> None:
    from src.backup import daily_backup
    await daily_backup(bot=_bot)


async def _job_qa_audit() -> None:
    from src.bot.utils.vector_search import scheduled_qa_audit
    await scheduled_qa_audit(bot=_bot, google=_google, cache=_cache)


# ── Команды бота ────────────────────────────────────────────────────────

# Команды для всех пользователей
USER_COMMANDS = [
    BotCommand(command="start", description="Начать / выбрать гайд"),
    BotCommand(command="consult", description="Задать вопрос юристу"),
    BotCommand(command="booking", description="Запись на консультацию"),
    BotCommand(command="profile", description="Личный кабинет"),
]

# Команды для администратора
ADMIN_COMMANDS = [
    BotCommand(command="start", description="Начать / выбрать гайд"),
    BotCommand(command="test_flow", description="Сброс — тест как новый"),
    BotCommand(command="admin", description="Панель управления"),
    BotCommand(command="broadcast", description="Рассылка (#сегмент)"),
    BotCommand(command="report", description="Dashboard 24ч"),
    BotCommand(command="refresh", description="Сброс кеша"),
    BotCommand(command="consult", description="Задать вопрос юристу"),
    BotCommand(command="booking", description="Запись на консультацию"),
    BotCommand(command="profile", description="Личный кабинет"),
]


async def _register_commands(bot: Bot) -> None:
    """Регистрирует меню команд в Telegram."""
    # Команды для всех
    await bot.set_my_commands(USER_COMMANDS)
    # Расширенные команды для админа
    await bot.set_my_commands(
        ADMIN_COMMANDS,
        scope=BotCommandScopeChat(chat_id=settings.ADMIN_ID),
    )
    logger.info("Команды бота зарегистрированы")


async def main() -> None:
    """Инициализация и запуск бота."""

    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    # P9: Ротация логов (RotatingFileHandler)
    from src.bot.utils.log_manager import setup_log_rotation
    setup_log_rotation()

    logger.info("Запуск бота v%s...", settings.VERSION)

    # P4: Sentry SDK (если SENTRY_DSN задан)
    from src.bot.utils.sentry_integration import init_sentry
    init_sentry()

    # P3: Проверка конфигурации при старте
    from src.bot.utils.validators import check_config_sanity
    config_warnings = check_config_sanity()
    for w in config_warnings:
        logger.warning("CONFIG: %s", w)

    # Инициализация БД (SQLite backup)
    await init_db()

    # Инициализация Google Sheets клиента и кеша
    google = GoogleSheetsClient(
        credentials_path=settings.GOOGLE_CREDENTIALS_PATH,
        spreadsheet_id=settings.GOOGLE_SPREADSHEET_ID,
        credentials_json=settings.GOOGLE_CREDENTIALS_JSON,
        credentials_base64=settings.GOOGLE_CREDENTIALS_BASE64,
    )
    cache = TTLCache(ttl_seconds=settings.CACHE_TTL_SECONDS)

    # Инициализация бота и диспетчера
    bot = Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)

    # AI Orchestrator (единый AI-клиент с aiohttp)
    ai = get_orchestrator()

    # Внедрение зависимостей — доступны в хендлерах как аргументы
    dp["google"] = google
    dp["cache"] = cache
    dp["ai"] = ai

    # Запуск планировщика
    scheduler = get_scheduler()

    # Обёртка для send_followup_message с прошитыми зависимостями
    async def _send_followup(user_id: int, guide_id: str, step: int) -> None:
        await send_followup_message(
            user_id, guide_id, step,
            bot=bot, google=google, cache=cache,
        )

    dp["send_followup"] = _send_followup
    dp["scheduler"] = scheduler
    dp["bot_ref"] = bot  # для digest/strategy доступ к bot

    if not scheduler.running:
        scheduler.start()
        logger.info("APScheduler запущен")

    # Регистрация проактивных задач (дайджест, напоминания)
    from src.bot.handlers.digest import register_scheduled_jobs

    register_scheduled_jobs(scheduler, bot=bot, google=google, cache=cache)

    # Еженедельный VACUUM + backup БД (воскресенье 03:00 UTC)
    from src.backup import scheduled_backup
    scheduler.add_job(
        scheduled_backup,
        trigger="cron",
        day_of_week="sun",
        hour=3, minute=0,
        id="weekly_db_maintenance",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Set module-level refs for scheduled jobs
    global _bot, _google, _cache
    _bot = bot
    _google = google
    _cache = cache

    # Retention Loop — ежедневная проверка спящих пользователей (08:00 UTC)
    scheduler.add_job(
        _job_retention_check,
        trigger="cron",
        hour=8, minute=0,
        id="daily_retention_check",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Weekly Growth Hacker Report (понедельник 09:00 UTC)
    scheduler.add_job(
        _job_growth_report,
        trigger="cron",
        day_of_week="mon",
        hour=9, minute=0,
        id="weekly_growth_report",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Auto-Stories: проверка новых статей каждые 4 часа
    scheduler.add_job(
        _job_auto_stories,
        trigger="interval",
        hours=4,
        id="auto_stories_check",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # P5: Телеметрия — сброс событий в Google Sheets каждые 6 часов
    scheduler.add_job(
        _job_telemetry_flush,
        trigger="interval",
        hours=6,
        id="telemetry_flush",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # P5: Еженедельный AI-анализ воронки (среда 10:00 UTC)
    scheduler.add_job(
        _job_funnel_analysis,
        trigger="cron",
        day_of_week="wed",
        hour=10, minute=0,
        id="weekly_funnel_analysis",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # P6: Ежедневный бэкап БД → отправка админу (04:00 UTC)
    scheduler.add_job(
        _job_daily_backup,
        trigger="cron",
        hour=4, minute=0,
        id="daily_db_backup",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # C10: Еженедельный QA аудит качества (пятница 16:00 UTC = 21:00 Алматы)
    scheduler.add_job(
        _job_qa_audit,
        trigger="cron",
        day_of_week="fri",
        hour=16, minute=0,
        id="weekly_qa_audit",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # P9: Очистка кеша/temp каждые 12 часов
    from src.bot.utils.log_manager import scheduled_cleanup

    scheduler.add_job(
        scheduled_cleanup,
        trigger="interval",
        hours=12,
        id="cache_cleanup",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # ── Middleware (порядок: outer → inner → handler → inner → outer) ──

    # P1: Global Error Handler (САМЫЙ ВНЕШНИЙ — ловит всё)
    from src.bot.middlewares.error_handler import ErrorHandlingMiddleware
    error_mw = ErrorHandlingMiddleware(bot)
    dp.message.middleware(error_mw)
    dp.callback_query.middleware(error_mw)

    # Self-Healing Middleware — AI-диагностика ошибок + уведомления админа
    from src.monitoring import SelfHealingMiddleware
    healing_mw = SelfHealingMiddleware(bot)
    dp.message.middleware(healing_mw)
    dp.callback_query.middleware(healing_mw)

    # P2: Throttling Middleware — общий антифлуд (1 msg/sec)
    from src.bot.middlewares.throttle import ThrottlingMiddleware
    throttle_mw = ThrottlingMiddleware()
    dp.message.middleware(throttle_mw)
    dp.callback_query.middleware(throttle_mw)

    # Middleware — TraceMiddleware для messages и callback queries
    trace_mw = LoggingMiddleware()
    dp.message.middleware(trace_mw)
    dp.callback_query.middleware(trace_mw)

    # AI Rate Limit Middleware — анти-флуд (5 AI-запросов в час на пользователя)
    from src.bot.middlewares.rate_limit import AIRateLimitMiddleware
    ai_limit_mw = AIRateLimitMiddleware(daily_limit=10)
    dp.message.middleware(ai_limit_mw)
    dp.callback_query.middleware(ai_limit_mw)

    # Регистрация роутеров
    # ПОРЯДОК ВАЖЕН: команды (/start, /consult, /chat, /referral) должны
    # обрабатываться РАНЬШЕ FSM-роутеров (content_manager, lead_form),
    # иначе FSM-состояние перехватит команду.
    dp.include_router(start.router)            # /start — всегда доступен
    dp.include_router(admin.router)            # /refresh, /report, /growth
    dp.include_router(live_support.router)     # /reply + Live Support FSM
    dp.include_router(consult.router)          # /consult
    dp.include_router(voice.router)            # голосовые сообщения → Whisper
    dp.include_router(strategy.router)         # /chat
    dp.include_router(cabinet.router)          # /profile, /karma
    dp.include_router(legal_tools.router)       # /review, /brainstorm, /bin, /tasks, /remind
    dp.include_router(corporate.router)        # /booking, /docgen, /mytasks, /invoice
    dp.include_router(documents.router)        # /doc + FSM
    dp.include_router(referral.router)         # /referral
    dp.include_router(broadcast.router)        # /broadcast #сегмент
    dp.include_router(content_manager.router)  # /admin, /publish + FSM
    dp.include_router(digest.router)           # дайджест колбеки
    dp.include_router(language.router)         # /lang выбор языка
    dp.include_router(timezone_handler.router) # /timezone + геолокация
    dp.include_router(waitlist_handler.router) # waitlist Coming Soon
    dp.include_router(consultation.router)     # запись на консультацию
    dp.include_router(questions.router)        # вопросы юристу
    dp.include_router(email_campaigns.router)  # email-ретаргетинг
    dp.include_router(subscription.router)     # подписка колбеки
    dp.include_router(feedback.router)         # NPS/feedback колбеки
    dp.include_router(group_mode.router)       # мониторинг групп
    dp.include_router(lead_form.router)        # лид-форма FSM (последний — ловит всё)

    # P8: Healthcheck HTTP API для Docker
    from src.bot.utils.healthcheck import start_healthcheck, stop_healthcheck, set_ready
    await start_healthcheck(bot=bot)

    # P10: Аудит безопасности при старте (логируем результат)
    try:
        from src.bot.utils.security_audit import run_security_audit
        audit = run_security_audit()
        logger.info("Security audit: %s — %d issues (%d critical)",
                     audit["grade"], audit["total_issues"], audit["critical"])
        if audit["critical"] > 0:
            logger.warning("⚠️ CRITICAL security issues found! Review immediately.")
    except Exception as e:
        logger.warning("Security audit skipped: %s", e)

    # Запуск бота
    logger.info("Бот запущен и слушает обновления (v%s)", settings.VERSION)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await _register_commands(bot)

        # Сохраняем username бота для deep link генерации
        try:
            from src.bot.utils.retargeting import set_bot_username
            me = await bot.get_me()
            if me.username:
                set_bot_username(me.username)
        except Exception as e:
            logger.warning("Failed to set bot username for retargeting: %s", e)

        set_ready(True)
        await dp.start_polling(bot)
    finally:
        set_ready(False)
        await stop_healthcheck()
        await ai.close()
        logger.info("AI сессия закрыта")
        if scheduler.running:
            scheduler.shutdown(wait=False)
            logger.info("APScheduler остановлен")


if __name__ == "__main__":
    asyncio.run(main())
