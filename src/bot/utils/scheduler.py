"""Persistent task scheduler — задачи хранятся в SQLite.

Архитектура:
- ``schedule_followup_series()`` записывает 3 задачи в таблицу ``scheduled_tasks``.
- ``TaskRunner`` — async-цикл, который каждые ``poll_interval`` секунд
  проверяет базу на due-задачи (status=pending, run_at <= now) и исполняет их.
- Задачи переживают рестарт бота — при старте runner подхватит все просроченные.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine

from src.database.crud import (
    create_scheduled_task,
    get_due_tasks,
    mark_task_done,
    mark_task_failed,
)

logger = logging.getLogger(__name__)

# Тип обработчика: async (task_dto) -> None
TaskHandler = Callable[..., Coroutine[Any, Any, None]]


# ─────────────────── Планирование follow-up серии ─────────────────────


async def schedule_followup_series(user_id: int, guide_id: str) -> None:
    """Создаёт 3 отложенные задачи в БД для follow-up серии.

    step 0 — через 24 ч
    step 1 — через 3 дня
    step 2 — через 7 дней
    """
    now = datetime.now(timezone.utc)

    delays = [
        (0, timedelta(hours=24)),
        (1, timedelta(days=3)),
        (2, timedelta(days=7)),
    ]

    for step, delta in delays:
        run_at = now + delta
        await create_scheduled_task(
            task_type="followup",
            user_id=user_id,
            run_at=run_at,
            payload={"guide_id": guide_id, "step": step},
        )

    logger.info(
        "Follow-up series scheduled: user_id=%s, guide=%s (3 tasks)",
        user_id, guide_id,
    )


# ─────────────────── TaskRunner (async poller) ────────────────────────


class TaskRunner:
    """Фоновый исполнитель задач из SQLite.

    Регистрируйте обработчики через ``register(task_type, handler)``.
    Запускайте ``start()`` и останавливайте ``stop()``.
    """

    def __init__(self, poll_interval: int = 60) -> None:
        self._poll_interval = poll_interval
        self._handlers: dict[str, TaskHandler] = {}
        self._task: asyncio.Task | None = None
        self._running = False

    def register(self, task_type: str, handler: TaskHandler) -> None:
        """Регистрирует async-обработчик для типа задачи."""
        self._handlers[task_type] = handler
        logger.info("TaskRunner: handler registered for '%s'", task_type)

    def start(self) -> None:
        """Запускает фоновый polling-цикл."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="task_runner_poll")
        logger.info("TaskRunner started (poll every %ds)", self._poll_interval)

    def stop(self) -> None:
        """Останавливает polling."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        logger.info("TaskRunner stopped")

    @property
    def running(self) -> bool:
        return self._running

    async def _loop(self) -> None:
        """Основной цикл: sleep → poll → execute → repeat."""
        while self._running:
            try:
                await self._poll_and_execute()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("TaskRunner poll error: %s", exc, exc_info=True)

            try:
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                break

    async def _poll_and_execute(self) -> None:
        """Забирает due-задачи и исполняет."""
        tasks = await get_due_tasks(limit=50)
        if not tasks:
            return

        logger.info("TaskRunner: %d due tasks found", len(tasks))

        for task in tasks:
            handler = self._handlers.get(task.task_type)
            if handler is None:
                logger.warning("No handler for task type '%s' (id=%s)", task.task_type, task.id)
                await mark_task_failed(task.id, error=f"No handler for '{task.task_type}'")
                continue

            try:
                await handler(task)
                await mark_task_done(task.id)
            except Exception as exc:
                logger.error(
                    "Task %s failed: %s", task.id, exc, exc_info=True,
                )
                await mark_task_failed(task.id, error=str(exc))
