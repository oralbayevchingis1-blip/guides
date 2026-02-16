"""Умные рекомендации на основе коллаборативной фильтрации.

Анализирует реальные данные скачиваний: «пользователи, скачавшие гайд A,
часто скачивают гайд B» и кеширует матрицу в памяти.
"""

import asyncio
import logging
import time

logger = logging.getLogger(__name__)

# Обновлять матрицу не чаще чем раз в N секунд
_DEFAULT_TTL = 6 * 3600  # 6 часов


class SmartRecommender:
    """In-memory кеш матрицы «часто скачивают вместе».

    Usage::

        recommender = SmartRecommender()
        next_id = await recommender.get_recommendation("guide_too", exclude={"guide_ip"})
    """

    def __init__(self, ttl_seconds: int = _DEFAULT_TTL, min_shared: int = 2) -> None:
        self._matrix: dict[str, list[tuple[str, int]]] = {}
        self._last_refresh: float = 0.0
        self._ttl = ttl_seconds
        self._min_shared = min_shared
        self._lock = asyncio.Lock()

    async def _ensure_fresh(self) -> None:
        """Обновляет матрицу, если TTL истёк."""
        now = time.monotonic()
        if now - self._last_refresh < self._ttl and self._matrix:
            return

        async with self._lock:
            # Double-check после захвата lock
            if time.monotonic() - self._last_refresh < self._ttl and self._matrix:
                return

            from src.database.crud import get_codownload_matrix

            try:
                self._matrix = await get_codownload_matrix(min_shared=self._min_shared)
                self._last_refresh = time.monotonic()
                total_pairs = sum(len(v) for v in self._matrix.values()) // 2
                logger.info(
                    "SmartRecommender: matrix refreshed — %d guides, %d pairs",
                    len(self._matrix), total_pairs,
                )
            except Exception as e:
                logger.error("SmartRecommender refresh failed: %s", e)

    async def get_recommendation(
        self,
        guide_id: str,
        *,
        exclude: set[str] | None = None,
    ) -> str | None:
        """Возвращает лучший next_guide_id по коллаборативной фильтрации.

        Args:
            guide_id: Текущий гайд.
            exclude: Множество guide_id для исключения (уже скачанные).

        Returns:
            guide_id рекомендованного гайда или ``None``.
        """
        await self._ensure_fresh()

        pairs = self._matrix.get(guide_id, [])
        if not pairs:
            return None

        skip = exclude or set()
        for other_id, _score in pairs:
            if other_id not in skip and other_id != guide_id:
                return other_id
        return None

    async def get_top_pairs(self, limit: int = 20) -> list[tuple[str, str, int]]:
        """Топ пар «часто скачивают вместе» для админки.

        Returns:
            [(guide_a, guide_b, shared_users), ...]
        """
        await self._ensure_fresh()

        seen: set[tuple[str, str]] = set()
        result: list[tuple[str, str, int]] = []

        for guide_a, pairs in self._matrix.items():
            for guide_b, shared in pairs:
                key = (min(guide_a, guide_b), max(guide_a, guide_b))
                if key not in seen:
                    seen.add(key)
                    result.append((guide_a, guide_b, shared))

        result.sort(key=lambda x: -x[2])
        return result[:limit]

    def invalidate(self) -> None:
        """Сбрасывает кеш (для /refresh)."""
        self._last_refresh = 0.0
        self._matrix.clear()

    @property
    def is_loaded(self) -> bool:
        return bool(self._matrix)


# Глобальный экземпляр
smart_recommender = SmartRecommender()
