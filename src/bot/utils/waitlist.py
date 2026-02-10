"""Waitlist — система списков ожидания для новых услуг.

Когда в Data Room появляется услуга со статусом «Coming Soon»,
бот предлагает пользователям записаться в waitlist.
При релизе — автоматическое уведомление всех подписавшихся.

Использование:
    from src.bot.utils.waitlist import add_to_waitlist, notify_waitlist, get_coming_soon
"""

import logging
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Хранилище: {service_id: [user_id, ...]}
_waitlists: dict[str, list[int]] = defaultdict(list)

# Метаданные услуг
_service_meta: dict[str, dict] = {}


def get_coming_soon(data_room: list[dict]) -> list[dict]:
    """Находит услуги со статусом 'Coming Soon' в Data Room.

    Args:
        data_room: Данные из Google Sheets Data Room.

    Returns:
        Список словарей с coming soon услугами.
    """
    coming = []
    for item in data_room:
        status = str(item.get("status", item.get("Статус", ""))).lower().strip()
        if status in ("coming soon", "скоро", "coming_soon", "planned"):
            service_id = (
                item.get("id", "")
                or item.get("title", item.get("Заголовок", ""))
            ).strip()
            if service_id:
                coming.append({
                    "id": service_id,
                    "title": item.get("title", item.get("Заголовок", service_id)),
                    "description": item.get("content", item.get("Описание", "")),
                    "category": item.get("category", item.get("Категория", "")),
                    "waitlist_count": len(_waitlists.get(service_id, [])),
                })
                # Сохраняем мета
                _service_meta[service_id] = coming[-1]

    return coming


def add_to_waitlist(service_id: str, user_id: int) -> bool:
    """Добавляет пользователя в waitlist.

    Returns:
        True если добавлен, False если уже в списке.
    """
    if user_id in _waitlists[service_id]:
        return False
    _waitlists[service_id].append(user_id)
    logger.info("Waitlist +1: service=%s, user=%s (total: %d)",
                service_id, user_id, len(_waitlists[service_id]))
    return True


def remove_from_waitlist(service_id: str, user_id: int) -> bool:
    """Удаляет пользователя из waitlist."""
    if user_id in _waitlists[service_id]:
        _waitlists[service_id].remove(user_id)
        return True
    return False


def get_waitlist(service_id: str) -> list[int]:
    """Возвращает список user_id в waitlist."""
    return list(_waitlists.get(service_id, []))


def get_waitlist_count(service_id: str) -> int:
    """Количество пользователей в waitlist."""
    return len(_waitlists.get(service_id, []))


async def notify_waitlist_release(
    bot,
    service_id: str,
    title: str = "",
    message: str = "",
) -> dict:
    """Уведомляет всех в waitlist о релизе услуги.

    Returns:
        {"total": N, "sent": N, "failed": N}
    """
    import asyncio

    users = _waitlists.get(service_id, [])
    if not users:
        return {"total": 0, "sent": 0, "failed": 0}

    service = _service_meta.get(service_id, {})
    svc_title = title or service.get("title", service_id)

    text = message or (
        f"🚀 <b>Долгожданный релиз!</b>\n\n"
        f"Услуга «<b>{svc_title}</b>» теперь доступна!\n\n"
        f"Вы записывались в список ожидания — "
        f"и мы рады сообщить, что всё готово.\n\n"
        f"───────────────\n"
        f"💡 Узнать подробности: @SOLISlegal"
    )

    sent = 0
    failed = 0

    for uid in users:
        try:
            await bot.send_message(chat_id=uid, text=text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    # Очищаем waitlist после рассылки
    _waitlists[service_id] = []
    logger.info("Waitlist notified: service=%s, sent=%d, failed=%d", service_id, sent, failed)

    return {"total": len(users), "sent": sent, "failed": failed}


def get_all_waitlists() -> dict[str, int]:
    """Все активные waitlists с количеством подписчиков."""
    return {sid: len(users) for sid, users in _waitlists.items() if users}
