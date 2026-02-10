"""Бэкап и обслуживание БД: VACUUM + копирование в безопасное место.

Функции:
- vacuum_database: еженедельный VACUUM SQLite для оптимизации
- create_backup: создание timestamped копии БД
- scheduled_backup: комбинированная задача для APScheduler

Использование:
    from src.backup import scheduled_backup
    scheduler.add_job(scheduled_backup, 'cron', day_of_week='sun', hour=3)
"""

import logging
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.config import settings

logger = logging.getLogger(__name__)

# Директория для бэкапов
BACKUP_DIR = Path(getattr(settings, "BACKUP_DIR", "backups"))
BACKUP_RETAIN_DAYS = int(getattr(settings, "BACKUP_RETAIN_DAYS", 7))

# Извлекаем путь к файлу БД из DATABASE_URL
_DB_PATH = settings.DATABASE_URL.replace("sqlite+aiosqlite:///", "").replace("sqlite:///", "")


def _get_db_path() -> Path:
    """Возвращает путь к файлу SQLite."""
    return Path(_DB_PATH)


def vacuum_database() -> bool:
    """Выполняет VACUUM для оптимизации SQLite БД.

    Returns:
        True если VACUUM выполнен успешно.
    """
    db_path = _get_db_path()
    if not db_path.exists():
        logger.warning("DB file not found for VACUUM: %s", db_path)
        return False

    try:
        size_before = db_path.stat().st_size
        conn = sqlite3.connect(str(db_path))
        conn.execute("VACUUM")
        conn.close()
        size_after = db_path.stat().st_size

        saved = size_before - size_after
        logger.info(
            "VACUUM complete: %s -> %s (saved %s bytes)",
            _format_size(size_before),
            _format_size(size_after),
            saved,
        )
        return True

    except Exception as e:
        logger.error("VACUUM failed: %s", e)
        return False


def create_backup() -> Path | None:
    """Создаёт timestamped копию БД.

    Returns:
        Путь к файлу бэкапа или None при ошибке.
    """
    db_path = _get_db_path()
    if not db_path.exists():
        logger.warning("DB file not found for backup: %s", db_path)
        return None

    try:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_name = f"legal_bot_{timestamp}.db"
        backup_path = BACKUP_DIR / backup_name

        # Используем sqlite3 backup API для консистентной копии
        source = sqlite3.connect(str(db_path))
        dest = sqlite3.connect(str(backup_path))
        source.backup(dest)
        dest.close()
        source.close()

        size = backup_path.stat().st_size
        logger.info("Backup created: %s (%s)", backup_path, _format_size(size))
        return backup_path

    except Exception as e:
        logger.error("Backup failed: %s", e)
        return None


def cleanup_old_backups() -> int:
    """Удаляет бэкапы старше BACKUP_RETAIN_DAYS.

    Returns:
        Количество удалённых файлов.
    """
    if not BACKUP_DIR.exists():
        return 0

    now = datetime.now(timezone.utc).timestamp()
    max_age = BACKUP_RETAIN_DAYS * 86400
    removed = 0

    for f in BACKUP_DIR.glob("legal_bot_*.db"):
        age = now - f.stat().st_mtime
        if age > max_age:
            f.unlink()
            removed += 1
            logger.info("Old backup removed: %s", f.name)

    return removed


async def scheduled_backup(bot=None) -> None:
    """Комбинированная задача: VACUUM + backup + cleanup + (опционально) отправка админу.

    Вызывается APScheduler еженедельно.
    """
    import asyncio

    logger.info("Starting scheduled DB maintenance...")

    # VACUUM в отдельном потоке (блокирующая операция)
    vacuum_ok = await asyncio.to_thread(vacuum_database)

    # Backup
    backup_path = await asyncio.to_thread(create_backup)

    # Cleanup
    removed = await asyncio.to_thread(cleanup_old_backups)

    logger.info(
        "DB maintenance complete: vacuum=%s, backup=%s, cleaned=%d",
        "ok" if vacuum_ok else "fail",
        backup_path.name if backup_path else "fail",
        removed,
    )

    # Отправляем бэкап админу в Telegram (P6: Offsite Backup)
    if bot and backup_path and backup_path.exists():
        await send_backup_to_admin(bot, backup_path)


async def send_backup_to_admin(bot, backup_path: Path) -> bool:
    """Отправляет файл бэкапа БД в чат админа.

    Returns:
        True если отправлен успешно.
    """
    from aiogram.types import FSInputFile

    try:
        size = backup_path.stat().st_size
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        document = FSInputFile(str(backup_path))
        await bot.send_document(
            chat_id=settings.ADMIN_ID,
            document=document,
            caption=(
                f"💾 <b>Автоматический бэкап БД</b>\n\n"
                f"📅 {now}\n"
                f"📦 Размер: {_format_size(size)}\n"
                f"📁 Файл: {backup_path.name}"
            ),
        )
        logger.info("Backup sent to admin: %s", backup_path.name)
        return True
    except Exception as e:
        logger.error("Failed to send backup to admin: %s", e)
        return False


async def daily_backup(bot=None) -> None:
    """Ежедневный бэкап (без VACUUM, только копия + отправка).

    Легче чем scheduled_backup, запускается каждый день.
    """
    import asyncio

    backup_path = await asyncio.to_thread(create_backup)
    await asyncio.to_thread(cleanup_old_backups)

    if bot and backup_path and backup_path.exists():
        await send_backup_to_admin(bot, backup_path)


def _format_size(size: int) -> str:
    """Форматирует размер файла в человекочитаемый формат."""
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"
