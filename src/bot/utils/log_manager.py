"""P9. Ротация логов и автоматическая очистка кеша/temp файлов.

Функции:
- setup_log_rotation: настраивает RotatingFileHandler (10MB, 5 файлов)
- cleanup_cache: удаляет файлы из data/cache и data/temp старше 48 часов
- scheduled_cleanup: комбинированная задача для APScheduler

Использование:
    from src.bot.utils.log_manager import setup_log_rotation, scheduled_cleanup
    setup_log_rotation()  # при старте бота
    scheduler.add_job(scheduled_cleanup, 'interval', hours=12)
"""

import glob
import logging
import os
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

logger = logging.getLogger(__name__)

# Конфигурация
LOG_DIR = Path("data/logs")
LOG_FILE = LOG_DIR / "bot.log"
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
LOG_BACKUP_COUNT = 5

CACHE_DIRS = [
    Path("data/cache"),
    Path("data/temp"),
    Path("data/generated_docs"),
]
CACHE_MAX_AGE_HOURS = 48


def setup_log_rotation() -> bool:
    """Настраивает ротацию логов через RotatingFileHandler.

    Returns:
        True если настроено успешно.
    """
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)

        handler = RotatingFileHandler(
            filename=str(LOG_FILE),
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))

        # Добавляем к root logger
        root = logging.getLogger()
        root.addHandler(handler)

        # Отдельный файл для ошибок
        error_handler = RotatingFileHandler(
            filename=str(LOG_DIR / "errors.log"),
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s\n%(exc_info)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        root.addHandler(error_handler)

        logger.info("Log rotation configured: %s (max %dMB, %d backups)",
                     LOG_FILE, LOG_MAX_BYTES // (1024*1024), LOG_BACKUP_COUNT)
        return True

    except Exception as e:
        logger.error("Failed to setup log rotation: %s", e)
        return False


def cleanup_cache(max_age_hours: int = CACHE_MAX_AGE_HOURS) -> dict:
    """Удаляет старые файлы из кеш-директорий.

    Args:
        max_age_hours: Максимальный возраст файла в часах.

    Returns:
        {"deleted": int, "freed_bytes": int, "errors": int}
    """
    max_age_sec = max_age_hours * 3600
    now = time.time()

    stats = {"deleted": 0, "freed_bytes": 0, "errors": 0}

    for cache_dir in CACHE_DIRS:
        if not cache_dir.exists():
            continue

        for item in cache_dir.iterdir():
            if item.is_file():
                try:
                    age = now - item.stat().st_mtime
                    if age > max_age_sec:
                        size = item.stat().st_size
                        item.unlink()
                        stats["deleted"] += 1
                        stats["freed_bytes"] += size
                        logger.debug("Cache cleanup: removed %s (%d bytes)", item.name, size)
                except Exception as e:
                    stats["errors"] += 1
                    logger.warning("Cache cleanup error for %s: %s", item, e)

    if stats["deleted"] > 0:
        logger.info(
            "Cache cleanup: deleted %d files, freed %s",
            stats["deleted"],
            _format_size(stats["freed_bytes"]),
        )

    return stats


def get_log_stats() -> dict:
    """Статистика лог-файлов (для /report)."""
    result = {"total_size": 0, "file_count": 0}

    if LOG_DIR.exists():
        for f in LOG_DIR.iterdir():
            if f.is_file():
                result["total_size"] += f.stat().st_size
                result["file_count"] += 1

    result["total_size_human"] = _format_size(result["total_size"])
    return result


async def scheduled_cleanup() -> None:
    """Плановая очистка кеша (каждые 12 часов)."""
    import asyncio

    stats = await asyncio.to_thread(cleanup_cache)
    logger.info("Scheduled cleanup complete: %s", stats)


def _format_size(size: int) -> str:
    """Форматирует размер файла."""
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"
