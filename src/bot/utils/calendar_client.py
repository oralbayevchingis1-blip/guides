"""C1. Интеграция с Google Calendar — Legal Booking.

Автоматизирует запись на консультации: показывает свободные слоты
из календаря юриста и создаёт событие с данными клиента.

Использование:
    from src.bot.utils.calendar_client import get_available_slots, create_event
    slots = await get_available_slots()
    event = await create_event(slot, client_name, client_email)
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

ALMATY_TZ = timezone(timedelta(hours=5))

# Часы приёма (UTC+5 Алматы)
WORKING_HOURS = (9, 18)  # 9:00 - 18:00
SLOT_DURATION_MIN = 30
LUNCH_HOUR = 13  # 13:00 - 14:00 обед

# In-memory хранилище забронированных слотов (+ Google Calendar при наличии)
_booked_slots: dict[str, dict] = {}

# Список юристов
LAWYERS = {
    "partner": {"name": "Партнёр SOLIS", "calendar_id": "primary"},
    "associate": {"name": "Ассоциированный юрист", "calendar_id": "primary"},
}


def _generate_slots(days_ahead: int = 5) -> list[dict]:
    """Генерирует доступные 30-минутные слоты на ближайшие N рабочих дней."""
    now = datetime.now(ALMATY_TZ)
    slots = []

    for d in range(days_ahead + 2):
        date = now.date() + timedelta(days=d)
        weekday = date.weekday()

        # Пропускаем выходные
        if weekday >= 5:
            continue

        for hour in range(WORKING_HOURS[0], WORKING_HOURS[1]):
            # Пропускаем обед
            if hour == LUNCH_HOUR:
                continue

            for minute in (0, 30):
                slot_time = datetime(
                    date.year, date.month, date.day,
                    hour, minute, tzinfo=ALMATY_TZ,
                )
                # Только будущие слоты (минимум 2ч от текущего времени)
                if slot_time < now + timedelta(hours=2):
                    continue

                slot_id = slot_time.strftime("%Y%m%d_%H%M")
                if slot_id not in _booked_slots:
                    slots.append({
                        "id": slot_id,
                        "date": slot_time.strftime("%d.%m.%Y"),
                        "time": slot_time.strftime("%H:%M"),
                        "weekday": ["Пн", "Вт", "Ср", "Чт", "Пт"][weekday],
                        "datetime_utc": slot_time.astimezone(timezone.utc).isoformat(),
                        "display": f"{['Пн','Вт','Ср','Чт','Пт'][weekday]} {slot_time.strftime('%d.%m %H:%M')}",
                    })

        if len(slots) >= 15:
            break

    return slots[:15]


async def get_available_slots(days_ahead: int = 5) -> list[dict]:
    """Возвращает доступные слоты для записи.

    Пытается подключиться к Google Calendar, при неудаче —
    генерирует слоты из рабочего расписания.
    """
    # Пытаемся получить слоты из Google Calendar
    try:
        return await _get_gcal_slots(days_ahead)
    except Exception as e:
        logger.info("Google Calendar unavailable, using local slots: %s", e)

    # Fallback: локальная генерация
    return await asyncio.to_thread(_generate_slots, days_ahead)


async def _get_gcal_slots(days_ahead: int) -> list[dict]:
    """Получает свободные слоты из Google Calendar API."""
    from src.config import settings
    import json

    creds_path = settings.GOOGLE_CREDENTIALS_PATH
    creds_b64 = getattr(settings, "GOOGLE_CREDENTIALS_BASE64", "")
    if not creds_path and not creds_b64:
        raise RuntimeError("No Google credentials")

    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    import base64

    SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]

    def _fetch():
        if creds_b64:
            info = json.loads(base64.b64decode(creds_b64))
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        else:
            creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        service = build("calendar", "v3", credentials=creds)

        now = datetime.now(timezone.utc)
        time_min = now.isoformat()
        time_max = (now + timedelta(days=days_ahead + 2)).isoformat()

        events = service.events().list(
            calendarId="primary",
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            orderBy="startTime",
        ).execute()

        # Собираем занятые временные окна
        busy = set()
        for ev in events.get("items", []):
            start = ev.get("start", {}).get("dateTime", "")
            if start:
                try:
                    dt = datetime.fromisoformat(start)
                    busy.add(dt.strftime("%Y%m%d_%H%M"))
                except Exception:
                    pass

        # Генерируем слоты, исключая занятые
        all_slots = _generate_slots(days_ahead)
        return [s for s in all_slots if s["id"] not in busy]

    return await asyncio.to_thread(_fetch)


async def create_event(
    slot_id: str,
    client_name: str,
    client_email: str = "",
    client_phone: str = "",
    topic: str = "Юридическая консультация",
    lawyer: str = "partner",
) -> dict:
    """Создаёт событие в Google Calendar и бронирует слот.

    Returns:
        {"success": bool, "event_id": str, "slot": dict, "error": str}
    """
    # Парсим время из slot_id
    try:
        dt = datetime.strptime(slot_id, "%Y%m%d_%H%M").replace(tzinfo=ALMATY_TZ)
    except ValueError:
        return {"success": False, "error": "Неверный формат слота"}

    # Проверяем, не занят ли
    if slot_id in _booked_slots:
        return {"success": False, "error": "Слот уже занят. Выберите другой."}

    end_dt = dt + timedelta(minutes=SLOT_DURATION_MIN)

    # Бронируем локально
    _booked_slots[slot_id] = {
        "client_name": client_name,
        "client_email": client_email,
        "topic": topic,
        "booked_at": datetime.now(timezone.utc).isoformat(),
    }

    event_data = {
        "summary": f"📞 {topic} — {client_name}",
        "description": (
            f"Клиент: {client_name}\n"
            f"Email: {client_email}\n"
            f"Телефон: {client_phone}\n"
            f"Тема: {topic}\n\n"
            f"Забронировано через бот SOLIS Partners"
        ),
        "start": {"dateTime": dt.isoformat(), "timeZone": "Asia/Almaty"},
        "end": {"dateTime": end_dt.isoformat(), "timeZone": "Asia/Almaty"},
        "reminders": {"useDefault": False, "overrides": [
            {"method": "popup", "minutes": 30},
        ]},
    }

    if client_email:
        event_data["attendees"] = [{"email": client_email}]

    # Пытаемся создать в Google Calendar
    gcal_id = ""
    try:
        gcal_id = await _create_gcal_event(event_data)
    except Exception as e:
        logger.warning("Google Calendar create failed (slot booked locally): %s", e)

    return {
        "success": True,
        "event_id": gcal_id or slot_id,
        "slot": {
            "date": dt.strftime("%d.%m.%Y"),
            "time": dt.strftime("%H:%M"),
            "end_time": end_dt.strftime("%H:%M"),
        },
        "client_name": client_name,
    }


async def _create_gcal_event(event_data: dict) -> str:
    """Создаёт событие в Google Calendar API."""
    from src.config import settings
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    import base64
    import json as _json

    SCOPES = ["https://www.googleapis.com/auth/calendar"]
    creds_b64 = getattr(settings, "GOOGLE_CREDENTIALS_BASE64", "")

    def _create():
        if creds_b64:
            info = _json.loads(base64.b64decode(creds_b64))
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        else:
            creds = Credentials.from_service_account_file(
                settings.GOOGLE_CREDENTIALS_PATH, scopes=SCOPES,
            )
        service = build("calendar", "v3", credentials=creds)
        event = service.events().insert(calendarId="primary", body=event_data).execute()
        return event.get("id", "")

    return await asyncio.to_thread(_create)


def get_booked_slots() -> dict:
    """Возвращает все забронированные слоты."""
    return dict(_booked_slots)


def cancel_booking(slot_id: str) -> bool:
    """Отменяет бронирование."""
    if slot_id in _booked_slots:
        del _booked_slots[slot_id]
        return True
    return False
