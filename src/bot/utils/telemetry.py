"""P5. Телеметрия и воронка продаж — Event Tracking.

Каждое действие пользователя записывается как событие.
Раз в неделю AI анализирует воронку и даёт рекомендации.

Использование:
    from src.bot.utils.telemetry import track_event, analyze_funnel
    await track_event(user_id, "guide_selected", {"guide_id": "esop"})
"""

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# In-memory event storage (+ periodic flush to Google Sheets)
_events: list[dict] = []
_funnel_counters: dict[str, int] = defaultdict(int)

# Стандартные этапы воронки (в порядке прохождения)
FUNNEL_STAGES = [
    "bot_started",          # /start
    "guide_menu_opened",    # Открыл меню гайдов
    "guide_selected",       # Выбрал гайд
    "consent_given",        # Дал согласие
    "email_entered",        # Ввёл email
    "name_entered",         # Ввёл имя
    "lead_saved",           # Лид сохранён
    "guide_downloaded",     # Гайд отправлен
    "consult_started",      # /consult
    "consult_question",     # Задал вопрос
    "consult_answered",     # Получил ответ AI
    "feedback_given",       # Оставил отзыв
    "referral_shared",      # Поделился ботом
    "payment_started",      # Начал оплату
    "payment_completed",    # Оплатил
]

# Промежуточные события
EXTRA_EVENTS = [
    "button_clicked",
    "article_read",
    "document_generated",
    "voice_message",
    "timezone_set",
    "language_changed",
    "profile_opened",
    "shop_opened",
    "waitlist_joined",
    "human_support_called",
    "group_question",
]


def track_event_sync(
    user_id: int,
    event_name: str,
    metadata: dict | None = None,
) -> None:
    """Синхронная запись события (для использования без await)."""
    _events.append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "event": event_name,
        "metadata": metadata or {},
    })
    _funnel_counters[event_name] += 1


async def track_event(
    user_id: int,
    event_name: str,
    metadata: dict | None = None,
) -> None:
    """Записывает событие в телеметрию.

    Args:
        user_id: ID пользователя Telegram.
        event_name: Имя события (из FUNNEL_STAGES или EXTRA_EVENTS).
        metadata: Дополнительные данные.
    """
    track_event_sync(user_id, event_name, metadata)
    logger.debug("EVENT: user=%s event=%s meta=%s", user_id, event_name, metadata)


def get_funnel_stats() -> dict[str, int]:
    """Возвращает счётчики по этапам воронки."""
    return dict(_funnel_counters)


def get_funnel_drop_rates() -> list[dict]:
    """Вычисляет конверсию между этапами воронки.

    Returns:
        [{"from": "guide_selected", "to": "consent_given", "rate": 85.5, "drop": 14.5}, ...]
    """
    result = []
    for i in range(len(FUNNEL_STAGES) - 1):
        stage_from = FUNNEL_STAGES[i]
        stage_to = FUNNEL_STAGES[i + 1]
        count_from = _funnel_counters.get(stage_from, 0)
        count_to = _funnel_counters.get(stage_to, 0)

        if count_from > 0:
            rate = round(count_to / count_from * 100, 1)
        else:
            rate = 0.0

        result.append({
            "from": stage_from,
            "to": stage_to,
            "count_from": count_from,
            "count_to": count_to,
            "rate": rate,
            "drop": round(100 - rate, 1),
        })
    return result


def get_recent_events(limit: int = 100) -> list[dict]:
    """Возвращает последние N событий."""
    return _events[-limit:]


async def flush_to_sheets(google) -> int:
    """Сбрасывает события в Google Sheets (лист 'Log_Events').

    Returns:
        Количество записанных событий.
    """
    if not _events:
        return 0

    # Берём пакет событий
    batch = _events[:500]

    try:
        rows = []
        for ev in batch:
            rows.append([
                ev["timestamp"],
                str(ev["user_id"]),
                ev["event"],
                str(ev.get("metadata", {})),
            ])

        ws = await asyncio.to_thread(
            google._open_worksheet, "Log_Events"
        )
        if ws:
            await asyncio.to_thread(ws.append_rows, rows)
            # Удаляем записанные
            del _events[:len(batch)]
            logger.info("Telemetry: flushed %d events to Google Sheets", len(batch))
            return len(batch)
        else:
            logger.warning("Telemetry: лист 'Log_Events' не найден")
            return 0

    except Exception as e:
        logger.error("Telemetry flush failed: %s", e)
        return 0


async def analyze_funnel(ai_client=None) -> str:
    """AI-анализ воронки: где теряем людей.

    Returns:
        Текст анализа с рекомендациями.
    """
    stats = get_funnel_stats()
    drops = get_funnel_drop_rates()

    if not stats:
        return "Недостаточно данных для анализа воронки."

    # Формируем отчёт для AI
    report_lines = ["📊 Воронка продаж бота SOLIS Partners:\n"]
    for stage in FUNNEL_STAGES:
        count = stats.get(stage, 0)
        report_lines.append(f"  {stage}: {count}")

    report_lines.append("\n📉 Конверсия между этапами:")
    worst_drop = None
    worst_drop_rate = 0.0

    for d in drops:
        if d["count_from"] > 0:
            report_lines.append(
                f"  {d['from']} → {d['to']}: "
                f"{d['rate']}% конверсия ({d['drop']}% потерь)"
            )
            if d["drop"] > worst_drop_rate and d["count_from"] >= 5:
                worst_drop_rate = d["drop"]
                worst_drop = d

    report = "\n".join(report_lines)

    # AI-анализ если доступен
    if ai_client:
        try:
            from src.bot.utils.ai_client import ask_marketing
            analysis = await ask_marketing(
                f"Проанализируй воронку продаж Telegram-бота юридической фирмы.\n\n"
                f"{report}\n\n"
                f"Определи самое слабое место воронки и дай 3 конкретные рекомендации "
                f"по улучшению конверсии. Ответ на русском, кратко, до 500 символов."
            )
            return f"{report}\n\n🧠 <b>AI-аналитика:</b>\n{analysis}"
        except Exception as e:
            logger.error("Funnel AI analysis failed: %s", e)

    # Fallback без AI
    if worst_drop:
        return (
            f"{report}\n\n"
            f"⚠️ <b>Узкое место:</b> {worst_drop['from']} → {worst_drop['to']} "
            f"(потеря {worst_drop['drop']}%)"
        )

    return report


async def scheduled_telemetry_flush(google, cache) -> None:
    """Плановый сброс телеметрии (каждые 6 часов)."""
    await flush_to_sheets(google)


async def weekly_funnel_analysis(bot, google, cache) -> None:
    """Еженедельный AI-анализ воронки → отправка админу."""
    from src.config import settings

    analysis = await analyze_funnel(ai_client=True)
    text = f"📈 <b>Еженедельный анализ воронки</b>\n\n{analysis}"

    try:
        await bot.send_message(
            chat_id=settings.ADMIN_ID,
            text=text[:4000],
        )
    except Exception as e:
        logger.error("Weekly funnel analysis send failed: %s", e)
