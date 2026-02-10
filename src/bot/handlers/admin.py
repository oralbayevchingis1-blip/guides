"""Admin-команды бота (только для ADMIN_ID)."""

import logging

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_drive import clear_pdf_cache
from src.config import settings
from src.constants import get_text

router = Router()
logger = logging.getLogger(__name__)


@router.message(Command("refresh"))
async def cmd_refresh(message: Message, cache: TTLCache) -> None:
    """Сброс кеша — бот подтянет свежие данные из Google Sheets.

    Доступно только администратору (ADMIN_ID).
    Также очищает локальный кеш PDF-файлов.
    """
    if message.from_user is None or message.from_user.id != settings.ADMIN_ID:
        return

    # Сбрасываем кеш текстов и каталога
    cache.invalidate()

    # Очищаем кеш скачанных PDF
    pdf_count = clear_pdf_cache()

    logger.info(
        "Кеш сброшен администратором (user_id=%s), PDF удалено: %d",
        message.from_user.id,
        pdf_count,
    )

    await message.answer(
        f"✅ Кеш сброшен.\n"
        f"• Тексты и каталог обновятся при следующем запросе\n"
        f"• PDF-кеш очищен ({pdf_count} файлов)"
    )
