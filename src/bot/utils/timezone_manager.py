"""Timezone Manager — умные уведомления по часовому поясу пользователя.

Хранит timezone пользователя:
- Через геолокацию (Location)
- Через явный выбор (/timezone)
- Через авто-детекцию по стране (Казахстан → UTC+5/+6)

Использование:
    from src.bot.utils.timezone_manager import get_user_tz, schedule_at_local
    tz = get_user_tz(user_id)
    schedule_at_local(scheduler, func, user_id, hour=9, minute=0)
"""

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Хранилище: {user_id: timezone_str}
_user_timezones: dict[int, str] = {}

# Казахстанские часовые пояса
KZ_TIMEZONES = {
    "Алматы": "Asia/Almaty",       # UTC+5 → +6 (зима → лето)
    "Астана": "Asia/Almaty",
    "Нур-Султан": "Asia/Almaty",
    "Шымкент": "Asia/Almaty",
    "Караганда": "Asia/Almaty",
    "Актау": "Asia/Aqtau",         # UTC+5
    "Актобе": "Asia/Aqtobe",       # UTC+5
    "Атырау": "Asia/Atyrau",       # UTC+5
    "Уральск": "Asia/Oral",        # UTC+5
}

# Популярные мировые зоны
COMMON_TIMEZONES = {
    "UTC+5 (Алматы)": "Asia/Almaty",
    "UTC+6 (Бишкек)": "Asia/Bishkek",
    "UTC+3 (Москва)": "Europe/Moscow",
    "UTC+0 (Лондон)": "Europe/London",
    "UTC+1 (Берлин)": "Europe/Berlin",
    "UTC+2 (Киев)": "Europe/Kyiv",
    "UTC+4 (Дубай)": "Asia/Dubai",
    "UTC+8 (Сингапур)": "Asia/Singapore",
}

DEFAULT_TZ = "Asia/Almaty"


def set_user_timezone(user_id: int, tz_str: str) -> bool:
    """Устанавливает часовой пояс пользователя.

    Returns:
        True если часовой пояс валиден и установлен.
    """
    try:
        ZoneInfo(tz_str)  # Проверяем валидность
        _user_timezones[user_id] = tz_str
        logger.info("Timezone set: user=%s -> %s", user_id, tz_str)
        return True
    except (KeyError, ValueError):
        logger.warning("Invalid timezone: %s", tz_str)
        return False


def get_user_tz(user_id: int) -> str:
    """Получает часовой пояс пользователя (default: Asia/Almaty)."""
    return _user_timezones.get(user_id, DEFAULT_TZ)


def get_user_zoneinfo(user_id: int) -> ZoneInfo:
    """Возвращает ZoneInfo объект для пользователя."""
    return ZoneInfo(get_user_tz(user_id))


def get_user_local_time(user_id: int) -> datetime:
    """Текущее время в часовом поясе пользователя."""
    tz = get_user_zoneinfo(user_id)
    return datetime.now(tz)


def timezone_from_location(latitude: float, longitude: float) -> str:
    """Определяет timezone по координатам (упрощённый метод).

    Для Казахстана: если долгота > 64° → Алматы (UTC+6), иначе Актау (UTC+5).
    Для остальных — примерная оценка по долготе.
    """
    # Казахстан: ~46-56°N, ~46-88°E
    if 40 <= latitude <= 56 and 46 <= longitude <= 88:
        if longitude >= 64:
            return "Asia/Almaty"
        else:
            return "Asia/Aqtau"

    # Россия
    if 50 <= latitude <= 70 and 25 <= longitude <= 50:
        return "Europe/Moscow"

    # Европа
    if 35 <= latitude <= 70 and -10 <= longitude <= 25:
        return "Europe/Berlin"

    # UAE/Gulf
    if 20 <= latitude <= 30 and 45 <= longitude <= 60:
        return "Asia/Dubai"

    # Fallback by longitude
    offset_hours = round(longitude / 15)
    zone_map = {
        0: "Europe/London",
        1: "Europe/Berlin",
        2: "Europe/Kyiv",
        3: "Europe/Moscow",
        4: "Asia/Dubai",
        5: "Asia/Almaty",
        6: "Asia/Almaty",
        8: "Asia/Singapore",
    }
    return zone_map.get(offset_hours, DEFAULT_TZ)


def schedule_at_local_time(
    scheduler,
    func,
    user_id: int,
    hour: int = 9,
    minute: int = 0,
    job_id_prefix: str = "local",
    **kwargs,
) -> None:
    """Планирует задачу на определённое время по местному часовому поясу пользователя."""
    tz = get_user_zoneinfo(user_id)
    now_local = datetime.now(tz)

    # Целевое время сегодня
    target = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)

    # Если время уже прошло — планируем на завтра
    if target <= now_local:
        target += timedelta(days=1)

    # Конвертируем в UTC для scheduler
    target_utc = target.astimezone(timezone.utc)

    job_id = f"{job_id_prefix}_{user_id}_{hour}_{minute}"

    scheduler.add_job(
        func,
        trigger="date",
        run_date=target_utc,
        id=job_id,
        replace_existing=True,
        misfire_grace_time=3600,
        kwargs=kwargs,
    )

    logger.info(
        "Job scheduled: %s at %s local (%s UTC) for user=%s",
        job_id, target.strftime("%H:%M"), target_utc.strftime("%H:%M"), user_id,
    )


def get_all_user_timezones() -> dict[str, int]:
    """Статистика часовых поясов."""
    from collections import Counter
    return dict(Counter(_user_timezones.values()))
