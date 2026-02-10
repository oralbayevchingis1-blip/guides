"""L7. Система тикетов для юристов (Task Manager).

Админ может пометить лид как «В работе» → бот создаёт задачу
в листе «Tasks» с дедлайном и ответственным.

L10. Ассистент по дедлайнам — напоминания о юридических сроках.

Использование:
    from src.bot.utils.ticket_manager import create_ticket, get_open_tickets
    ticket = await create_ticket(...)
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# In-memory хранилище тикетов (+ запись в Google Sheets)
_tickets: dict[str, dict] = {}
_ticket_counter = 0

# Статусы тикетов
TICKET_STATUSES = ["new", "in_progress", "review", "done", "cancelled"]

# Приоритеты
TICKET_PRIORITIES = {
    "urgent": "🔴 Срочно",
    "high": "🟠 Высокий",
    "normal": "🟡 Обычный",
    "low": "🟢 Низкий",
}


def _gen_ticket_id() -> str:
    global _ticket_counter
    _ticket_counter += 1
    return f"T-{_ticket_counter:04d}"


async def create_ticket(
    title: str,
    description: str = "",
    assignee: str = "",
    priority: str = "normal",
    deadline_days: int = 7,
    user_id: int = 0,
    lead_id: str = "",
    google=None,
) -> dict:
    """Создаёт новый тикет/задачу для юриста.

    Args:
        title: Название задачи.
        description: Описание.
        assignee: Ответственный юрист.
        priority: Приоритет (urgent/high/normal/low).
        deadline_days: Дедлайн через N дней.
        user_id: Telegram ID клиента.
        lead_id: ID лида из Sheets.
        google: GoogleSheetsClient для записи.

    Returns:
        Словарь с данными тикета.
    """
    ticket_id = _gen_ticket_id()
    now = datetime.now(timezone.utc)
    deadline = now + timedelta(days=deadline_days)

    ticket = {
        "id": ticket_id,
        "title": title,
        "description": description,
        "assignee": assignee,
        "priority": priority,
        "status": "new",
        "user_id": user_id,
        "lead_id": lead_id,
        "created_at": now.isoformat(),
        "deadline": deadline.isoformat(),
        "deadline_display": deadline.strftime("%d.%m.%Y"),
        "updated_at": now.isoformat(),
        "comments": [],
    }

    _tickets[ticket_id] = ticket

    # Запись в Google Sheets (лист «Tasks»)
    if google:
        try:
            import asyncio

            def _write_ticket():
                try:
                    ws = google._get_spreadsheet().worksheet("Tasks")
                except Exception:
                    # Лист не существует — создаём
                    try:
                        sp = google._get_spreadsheet()
                        ws = sp.add_worksheet("Tasks", rows=500, cols=10)
                        ws.append_row(["ID", "Title", "Description", "Assignee",
                                       "Priority", "Status", "UserID", "Created", "Deadline"])
                    except Exception:
                        return
                ws.append_row([
                    ticket_id, title, description[:200], assignee,
                    TICKET_PRIORITIES.get(priority, priority), "new",
                    str(user_id), now.strftime("%Y-%m-%d %H:%M"), deadline.strftime("%Y-%m-%d"),
                ], value_input_option="USER_ENTERED")

            await asyncio.to_thread(_write_ticket)
        except Exception as e:
            logger.warning("Failed to write ticket to Sheets: %s", e)

    logger.info("Ticket created: %s — %s", ticket_id, title[:50])
    return ticket


def update_ticket_status(ticket_id: str, status: str, comment: str = "") -> bool:
    """Обновляет статус тикета."""
    ticket = _tickets.get(ticket_id)
    if not ticket:
        return False

    if status not in TICKET_STATUSES:
        return False

    ticket["status"] = status
    ticket["updated_at"] = datetime.now(timezone.utc).isoformat()
    if comment:
        ticket["comments"].append({
            "text": comment,
            "time": datetime.now(timezone.utc).isoformat(),
        })

    logger.info("Ticket %s → %s", ticket_id, status)
    return True


def get_open_tickets(assignee: str = "") -> list[dict]:
    """Возвращает открытые тикеты (опционально фильтр по ответственному)."""
    result = []
    for t in _tickets.values():
        if t["status"] in ("new", "in_progress", "review"):
            if not assignee or t["assignee"] == assignee:
                result.append(t)
    return sorted(result, key=lambda x: x.get("deadline", ""))


def get_ticket(ticket_id: str) -> Optional[dict]:
    """Получает тикет по ID."""
    return _tickets.get(ticket_id)


def get_overdue_tickets() -> list[dict]:
    """Тикеты с просроченным дедлайном."""
    now = datetime.now(timezone.utc).isoformat()
    return [
        t for t in _tickets.values()
        if t["status"] in ("new", "in_progress") and t["deadline"] < now
    ]


def format_ticket(ticket: dict) -> str:
    """Форматирует тикет для Telegram (HTML)."""
    priority_emoji = TICKET_PRIORITIES.get(ticket["priority"], "⚪")
    status_map = {
        "new": "🆕 Новый",
        "in_progress": "🔄 В работе",
        "review": "👀 На проверке",
        "done": "✅ Готово",
        "cancelled": "❌ Отменён",
    }
    status = status_map.get(ticket["status"], ticket["status"])

    lines = [
        f"📋 <b>Тикет {ticket['id']}</b> {priority_emoji}\n",
        f"<b>{ticket['title']}</b>",
    ]
    if ticket.get("description"):
        lines.append(f"<i>{ticket['description'][:200]}</i>")
    lines.append(f"\n📊 Статус: {status}")
    if ticket.get("assignee"):
        lines.append(f"👤 Ответственный: {ticket['assignee']}")
    lines.append(f"📅 Дедлайн: {ticket.get('deadline_display', 'не задан')}")
    if ticket.get("user_id"):
        lines.append(f"👤 Клиент ID: {ticket['user_id']}")

    return "\n".join(lines)


def format_ticket_list(tickets: list[dict]) -> str:
    """Форматирует список тикетов."""
    if not tickets:
        return "✅ Открытых задач нет."

    lines = [f"📋 <b>Открытые задачи ({len(tickets)})</b>\n"]
    for t in tickets[:15]:
        priority = TICKET_PRIORITIES.get(t["priority"], "")
        lines.append(
            f"  {priority} <b>{t['id']}</b> — {t['title'][:40]}"
            f" (📅 {t.get('deadline_display', '?')})"
        )
    if len(tickets) > 15:
        lines.append(f"\n  ... и ещё {len(tickets) - 15}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
#  L10: Ассистент по дедлайнам
# ═══════════════════════════════════════════════════════════════════════════

# In-memory хранилище напоминаний
_reminders: list[dict] = []


def parse_deadline_request(text: str) -> dict | None:
    """Парсит запрос на напоминание из текста пользователя.

    Примеры:
        "напомни подать отчет через месяц"
        "напомни 15.03.2026 оплатить налог"
        "напомни через 7 дней продлить лицензию"

    Returns:
        {"task": str, "days": int, "date": datetime | None}
    """
    import re

    text_lower = text.lower().strip()

    # Паттерн: "через N дней/месяцев/недель"
    match = re.search(r'через\s+(\d+)\s+(день|дня|дней|месяц|месяца|месяцев|недел)', text_lower)
    if match:
        num = int(match.group(1))
        unit = match.group(2)
        if "месяц" in unit:
            days = num * 30
        elif "недел" in unit:
            days = num * 7
        else:
            days = num

        # Извлекаем задачу (убираем "напомни" и время)
        task = re.sub(r'напомн\w*\s+', '', text_lower)
        task = re.sub(r'через\s+\d+\s+\S+\s*', '', task).strip()
        if not task:
            task = text_lower

        return {"task": task.capitalize(), "days": days, "date": None}

    # Паттерн: дата dd.mm.yyyy
    date_match = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', text_lower)
    if date_match:
        try:
            day = int(date_match.group(1))
            month = int(date_match.group(2))
            year = int(date_match.group(3))
            target = datetime(year, month, day, tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            days = max(1, (target - now).days)

            task = re.sub(r'напомн\w*\s+', '', text_lower)
            task = re.sub(r'\d{1,2}[./]\d{1,2}[./]\d{4}\s*', '', task).strip()
            if not task:
                task = text_lower

            return {"task": task.capitalize(), "days": days, "date": target}
        except Exception:
            pass

    return None


async def schedule_reminder(
    scheduler,
    bot,
    user_id: int,
    task: str,
    days: int,
    admin_notify: bool = True,
) -> dict:
    """Планирует напоминание через scheduler.

    Args:
        scheduler: APScheduler instance.
        bot: Aiogram Bot.
        user_id: Telegram ID пользователя.
        task: Описание задачи.
        days: Через сколько дней напомнить.
        admin_notify: Уведомить также админа.

    Returns:
        Данные напоминания.
    """
    from src.config import settings

    fire_time = datetime.now(timezone.utc) + timedelta(days=days)
    reminder_id = f"reminder_{user_id}_{len(_reminders)}"

    reminder = {
        "id": reminder_id,
        "user_id": user_id,
        "task": task,
        "days": days,
        "fire_at": fire_time.isoformat(),
        "fire_display": fire_time.strftime("%d.%m.%Y %H:%M"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _reminders.append(reminder)

    async def _send_reminder():
        text = (
            f"⏰ <b>Напоминание!</b>\n\n"
            f"📋 {task}\n\n"
            f"Вы просили напомнить об этом {days} дн. назад.\n\n"
            f"⚖️ <i>Если вопрос актуален — обратитесь к юристам SOLIS Partners.</i>"
        )
        try:
            await bot.send_message(user_id, text)
        except Exception as e:
            logger.error("Reminder send failed: %s", e)

        if admin_notify:
            try:
                admin_text = (
                    f"⏰ <b>Напоминание для клиента</b>\n\n"
                    f"👤 User ID: {user_id}\n"
                    f"📋 {task}\n"
                    f"📅 Создано {days} дн. назад"
                )
                await bot.send_message(settings.ADMIN_ID, admin_text)
            except Exception:
                pass

    scheduler.add_job(
        _send_reminder,
        trigger="date",
        run_date=fire_time,
        id=reminder_id,
        replace_existing=True,
        misfire_grace_time=86400,
    )

    logger.info("Reminder scheduled: user=%s, task='%s', fire=%s", user_id, task[:50], fire_time)
    return reminder


def get_user_reminders(user_id: int) -> list[dict]:
    """Возвращает все напоминания пользователя."""
    return [r for r in _reminders if r["user_id"] == user_id]
