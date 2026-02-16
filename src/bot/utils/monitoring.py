"""Мониторинг бота: метрики, алерты, JSON-логи.

Компоненты:
- ``JSONFormatter`` — структурированные логи для stdout/файла
- ``BotMetrics``    — in-memory счётчики событий (thread-safe)
- ``AlertManager``  — Telegram-алерты админу при критических ошибках
- ``MonitoringMiddleware`` — aiogram middleware для автосбора метрик
"""

import asyncio
import json
import logging
import time
import traceback
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable

from aiogram import BaseMiddleware, Bot
from aiogram.types import CallbackQuery, Message, TelegramObject

from src.config import settings

logger = logging.getLogger(__name__)


# ─────────────────── JSON Log Formatter ───────────────────────────────


class JSONFormatter(logging.Formatter):
    """Формирует строку лога как JSON — готов к сбору Vector/Logstash/CloudWatch."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1]:
            log_obj["exception"] = traceback.format_exception(*record.exc_info)
        if hasattr(record, "event"):
            log_obj["event"] = record.event
        if hasattr(record, "user_id"):
            log_obj["user_id"] = record.user_id
        return json.dumps(log_obj, ensure_ascii=False, default=str)


# ─────────────────── In-Memory Metrics ────────────────────────────────


class BotMetrics:
    """Легковесный сборщик метрик (без Prometheus).

    Хранит:
    - счётчики событий (starts, downloads, emails, errors, ...)
    - скользящее окно ошибок для расчёта error rate
    - время старта (uptime)
    """

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}
        self._errors: deque[tuple[float, str]] = deque(maxlen=1000)
        self._started_at: float = time.monotonic()
        self._started_at_utc: datetime = datetime.now(timezone.utc)

    def inc(self, event: str, amount: int = 1) -> None:
        """Увеличивает счётчик события."""
        self._counters[event] = self._counters.get(event, 0) + amount

    def inc_error(self, error_name: str) -> None:
        """Фиксирует ошибку (со временем для rate)."""
        now = time.monotonic()
        self._errors.append((now, error_name))
        self.inc(f"error.{error_name}")
        self.inc("errors_total")

    def get(self, event: str) -> int:
        return self._counters.get(event, 0)

    def get_all(self) -> dict[str, int]:
        return dict(self._counters)

    def error_rate(self, window_seconds: int = 300) -> float:
        """Ошибок в минуту за последние N секунд."""
        now = time.monotonic()
        cutoff = now - window_seconds
        recent = sum(1 for ts, _ in self._errors if ts >= cutoff)
        minutes = window_seconds / 60
        return recent / minutes if minutes > 0 else 0.0

    def recent_errors(self, window_seconds: int = 300) -> dict[str, int]:
        """Группирует ошибки за окно по имени."""
        now = time.monotonic()
        cutoff = now - window_seconds
        counts: dict[str, int] = {}
        for ts, name in self._errors:
            if ts >= cutoff:
                counts[name] = counts.get(name, 0) + 1
        return counts

    def uptime_seconds(self) -> float:
        return time.monotonic() - self._started_at

    def uptime_str(self) -> str:
        total = int(self.uptime_seconds())
        days, rem = divmod(total, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, seconds = divmod(rem, 60)
        parts = []
        if days:
            parts.append(f"{days}д")
        if hours:
            parts.append(f"{hours}ч")
        parts.append(f"{minutes}м {seconds}с")
        return " ".join(parts)

    def started_at_str(self) -> str:
        return self._started_at_utc.strftime("%d.%m.%Y %H:%M UTC")


# Глобальный экземпляр
metrics = BotMetrics()


# ─────────────────── Alert Manager ────────────────────────────────────


class AlertManager:
    """Отправляет Telegram-алерты админу при критических событиях.

    Throttling: не чаще 1 алерта одного типа в ``cooldown`` секунд.
    """

    def __init__(self, cooldown: int = 300) -> None:
        self._cooldown = cooldown
        self._last_sent: dict[str, float] = {}
        self._bot: Bot | None = None

    def set_bot(self, bot: Bot) -> None:
        self._bot = bot

    async def alert(self, alert_type: str, message: str) -> None:
        """Отправляет алерт, если не в cooldown."""
        now = time.monotonic()
        last = self._last_sent.get(alert_type, 0)
        if now - last < self._cooldown:
            return

        self._last_sent[alert_type] = now

        if self._bot is None:
            logger.warning("AlertManager: bot not set, can't send alert: %s", message)
            return

        text = f"🚨 <b>Alert: {alert_type}</b>\n\n{message}"
        try:
            await self._bot.send_message(
                chat_id=settings.ADMIN_ID, text=text,
            )
            logger.info("Alert sent: %s", alert_type)
        except Exception as exc:
            logger.error("Failed to send alert: %s", exc)

    async def check_error_rate(self) -> None:
        """Проверяет error rate и алертит при превышении порога."""
        rate = metrics.error_rate(window_seconds=300)
        if rate > 5.0:
            recent = metrics.recent_errors(300)
            top_errors = sorted(recent.items(), key=lambda x: -x[1])[:5]
            lines = "\n".join(f"  • {name}: {cnt}" for name, cnt in top_errors)
            await self.alert(
                "high_error_rate",
                f"Error rate: <b>{rate:.1f}/мин</b> (за 5 мин)\n\n"
                f"Топ ошибок:\n{lines}",
            )

    async def check_sheets_health(self, success: bool, method: str) -> None:
        """Трекает здоровье Google Sheets API."""
        if success:
            metrics.inc("sheets.success")
        else:
            metrics.inc_error("sheets_api")
            consecutive = metrics.get("error.sheets_api")
            if consecutive >= 3:
                await self.alert(
                    "sheets_down",
                    f"Google Sheets API: <b>{consecutive}</b> ошибок подряд.\n"
                    f"Последний метод: <code>{method}</code>\n\n"
                    "Воронка может работать некорректно — "
                    "каталог и тексты используют fallback из кода.",
                )


# Глобальный экземпляр
alerts = AlertManager()


# ─────────────────── Aiogram Middleware ────────────────────────────────


class MonitoringMiddleware(BaseMiddleware):
    """Считает входящие updates, время обработки, ловит необработанные ошибки."""

    async def __call__(
        self,
        handler: Callable,
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        metrics.inc("updates_total")

        if isinstance(event, Message):
            metrics.inc("messages_total")
            text = event.text or ""
            if text.startswith("/start"):
                metrics.inc("cmd.start")
            elif text.startswith("/consultation"):
                metrics.inc("cmd.consultation")
            elif text.startswith("/library"):
                metrics.inc("cmd.library")
        elif isinstance(event, CallbackQuery):
            metrics.inc("callbacks_total")
            cb_data = event.data or ""
            if cb_data.startswith("download_"):
                metrics.inc("downloads_initiated")
            elif cb_data == "check_subscription":
                metrics.inc("subscription_checks")
            elif cb_data == "give_consent":
                metrics.inc("consents_given")
            elif cb_data == "book_consultation":
                metrics.inc("consultations_booked")

        t0 = time.monotonic()
        try:
            result = await handler(event, data)
            elapsed = time.monotonic() - t0
            if elapsed > 5.0:
                metrics.inc("slow_handlers")
                logger.warning(
                    "Slow handler: %.2fs, event_type=%s",
                    elapsed, type(event).__name__,
                )
            return result
        except Exception as exc:
            metrics.inc_error("unhandled")
            logger.error(
                "Unhandled error in handler: %s", exc, exc_info=True,
            )
            await alerts.check_error_rate()
            raise
