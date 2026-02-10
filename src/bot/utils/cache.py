"""Простой TTL-кеш для каталога гайдов и текстов бота."""

import logging
import time
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


class TTLCache:
    """In-memory кеш с временем жизни (TTL).

    Используется для кеширования данных из Google Sheets,
    чтобы не обращаться к API при каждом запросе пользователя.
    """

    def __init__(self, ttl_seconds: int = 300) -> None:
        self._store: dict[str, tuple[float, Any]] = {}
        self.ttl = ttl_seconds

    async def get_or_fetch(
        self,
        key: str,
        fetcher: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Возвращает значение из кеша или запрашивает через fetcher.

        Args:
            key: Ключ кеша (например ``"catalog"`` или ``"texts"``).
            fetcher: Асинхронная функция для получения свежих данных.

        Returns:
            Закешированное или свежезапрошенное значение.
        """
        now = time.time()

        if key in self._store:
            ts, value = self._store[key]
            if now - ts < self.ttl:
                return value
            logger.debug("TTL истёк для ключа '%s', обновляем...", key)

        try:
            value = await fetcher()
            self._store[key] = (now, value)
            return value
        except Exception:
            # Если запрос упал, но есть старое значение — вернём его
            if key in self._store:
                _, stale_value = self._store[key]
                logger.warning(
                    "Не удалось обновить '%s', возвращаем устаревшие данные",
                    key,
                )
                return stale_value
            raise

    def invalidate(self, key: str | None = None) -> None:
        """Сбрасывает кеш.

        Args:
            key: Если указан — сбрасывает только этот ключ.
                 Если ``None`` — сбрасывает весь кеш.
        """
        if key:
            self._store.pop(key, None)
            logger.info("Кеш '%s' сброшен", key)
        else:
            self._store.clear()
            logger.info("Весь кеш сброшен")
