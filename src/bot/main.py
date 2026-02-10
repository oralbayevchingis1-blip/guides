"""Точка входа Telegram-бота SOLIS Partners — AI-powered marketing hub."""

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
    consult,
    content_manager,
    lead_form,
    referral,
    start,
    subscription,
)
from src.bot.handlers import digest, strategy
from src.bot.handlers.followup import send_followup_message
from src.bot.middlewares.logging_mw import LoggingMiddleware
from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.bot.utils.scheduler import get_scheduler
from src.config import settings
from src.database.models import init_db

logger = logging.getLogger(__name__)


# ── Команды бота ────────────────────────────────────────────────────────

# Команды для всех пользователей
USER_COMMANDS = [
    BotCommand(command="start", description="Начать / выбрать гайд"),
    BotCommand(command="consult", description="AI-юрист — задать вопрос"),
    BotCommand(command="referral", description="Реферальная программа"),
]

# Команды для администратора
ADMIN_COMMANDS = [
    BotCommand(command="start", description="Начать / выбрать гайд"),
    BotCommand(command="admin", description="Панель управления"),
    BotCommand(command="publish", description="Опубликовать статью"),
    BotCommand(command="chat", description="Чат с AI-стратегом"),
    BotCommand(command="consult", description="AI-юрист — задать вопрос"),
    BotCommand(command="broadcast", description="Рассылка пользователям"),
    BotCommand(command="referral", description="Реферальная программа"),
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
    logger.info("Запуск бота...")

    # Инициализация БД (SQLite backup)
    await init_db()

    # Инициализация Google Sheets клиента и кеша
    google = GoogleSheetsClient(
        credentials_path=settings.GOOGLE_CREDENTIALS_PATH,
        spreadsheet_id=settings.GOOGLE_SPREADSHEET_ID,
    )
    cache = TTLCache(ttl_seconds=settings.CACHE_TTL_SECONDS)

    # Инициализация бота и диспетчера
    bot = Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
    )
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)

    # Внедрение зависимостей — доступны в хендлерах как аргументы
    dp["google"] = google
    dp["cache"] = cache

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

    # Middleware
    dp.message.middleware(LoggingMiddleware())

    # Регистрация роутеров
    # ПОРЯДОК ВАЖЕН: команды (/start, /consult, /chat, /referral) должны
    # обрабатываться РАНЬШЕ FSM-роутеров (content_manager, lead_form),
    # иначе FSM-состояние перехватит команду.
    dp.include_router(start.router)            # /start — всегда доступен
    dp.include_router(admin.router)            # /refresh
    dp.include_router(consult.router)          # /consult
    dp.include_router(strategy.router)         # /chat
    dp.include_router(referral.router)         # /referral
    dp.include_router(broadcast.router)        # /broadcast
    dp.include_router(content_manager.router)  # /admin, /publish + FSM
    dp.include_router(digest.router)           # дайджест колбеки
    dp.include_router(subscription.router)     # подписка колбеки
    dp.include_router(lead_form.router)        # лид-форма FSM

    # Запуск бота
    logger.info("Бот запущен и слушает обновления")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await _register_commands(bot)
        await dp.start_polling(bot)
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)
            logger.info("APScheduler остановлен")


if __name__ == "__main__":
    asyncio.run(main())
