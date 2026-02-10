"""Обработка авто-серии follow-up сообщений после скачивания гайда.

Тексты серии загружаются из Google Sheets (лист «Авто-серия»).
Если лист недоступен, используются fallback-тексты.
"""

import logging

from aiogram import Bot

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient

logger = logging.getLogger(__name__)

# Fallback-тексты если Google Sheets недоступен
FALLBACK_FOLLOWUP: dict[int, str] = {
    1: (
        "👋 Здравствуйте! Вчера вы скачали наш гайд.\n\n"
        "Удалось ли начать изучение? Если есть вопросы — "
        "мы всегда готовы помочь!\n\n"
        "📩 Для консультации напишите нам: @SOLISlegal"
    ),
    2: (
        "📊 Привет! Прошло несколько дней с момента скачивания гайда.\n\n"
        "Хотим поделиться практическим кейсом SOLIS Partners по этой теме. "
        "Подписывайтесь на наш канал, чтобы не пропустить полезные материалы!\n\n"
        "📚 Другие гайды: /start"
    ),
    3: (
        "🎯 Добрый день! Надеемся, гайд оказался полезным.\n\n"
        "Мы предлагаем *бесплатную мини-консультацию* (15 минут) "
        "по теме гайда с нашим специалистом.\n\n"
        "Для записи напишите нам: @SOLISlegal\n\n"
        "📚 Посмотреть другие гайды: /start"
    ),
}


async def send_followup_message(
    user_id: int,
    guide_id: str,
    step: int,
    *,
    bot: Bot,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Отправляет follow-up сообщение пользователю.

    Args:
        user_id: Telegram ID пользователя.
        guide_id: ID гайда, который скачал пользователь.
        step: Шаг серии (1, 2 или 3).
        bot: Экземпляр бота для отправки сообщений.
        google: Клиент Google Sheets.
        cache: TTL-кеш.
    """
    try:
        # Пробуем загрузить тексты серии из Google Sheets
        followup_texts = await cache.get_or_fetch(
            "followup_series",
            google.get_followup_series,
        )

        # Ищем текст по ключу: step_{N} или guide_{guide_id}_step_{N}
        specific_key = f"{guide_id}_step_{step}"
        generic_key = f"step_{step}"

        text = (
            followup_texts.get(specific_key)
            or followup_texts.get(generic_key)
            or FALLBACK_FOLLOWUP.get(step, "")
        )

        if not text:
            logger.warning(
                "Текст follow-up не найден: step=%d, guide=%s", step, guide_id
            )
            return

        await bot.send_message(chat_id=user_id, text=text)
        logger.info(
            "Follow-up отправлен: user_id=%s, guide=%s, step=%d",
            user_id, guide_id, step,
        )
    except Exception as e:
        logger.error(
            "Ошибка отправки follow-up: user_id=%s, step=%d, error=%s",
            user_id, step, e,
        )
