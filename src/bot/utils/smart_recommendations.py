"""Умные рекомендации на основе коллаборативной фильтрации.

Алгоритм «часто скачивают вместе» с улучшениями:
1. Взвешивание по давности (недавние co-downloads × 2)
2. Аффинность по сфере бизнеса (если пользователи из той же сферы чаще скачивают гайд → буст)
3. Персонализированный скоринг при выдаче рекомендации

Кеш обновляется:
  - По TTL (6 часов)
  - По команде /recommendations sync
  - Еженедельно через scheduler
"""

import asyncio
import logging
import time

logger = logging.getLogger(__name__)

_DEFAULT_TTL = 6 * 3600  # 6 часов

# Буст за совпадение сферы бизнеса
_SPHERE_BOOST = 1.5
# Штраф за гайд, который скачали >30 дней назад (давность)
_RECENCY_BOOST = 1.2


class SmartRecommender:
    """In-memory кеш матрицы «часто скачивают вместе» + сфера.

    Usage::

        recommender = SmartRecommender()
        next_id = await recommender.get_recommendation("guide_too", exclude={"guide_ip"})

        # Персонализированная рекомендация (с учётом сферы и истории):
        next_id = await recommender.get_personalized_recommendation(
            guide_id="guide_tax",
            user_id=12345,
            exclude={"guide_ip"},
        )
    """

    def __init__(self, ttl_seconds: int = _DEFAULT_TTL, min_shared: int = 2) -> None:
        # Базовая матрица: guide_id → [(other_id, shared_count), ...]
        self._matrix: dict[str, list[tuple[str, int]]] = {}
        # Взвешенная матрица: guide_id → [(other_id, weighted_score), ...]
        self._weighted_matrix: dict[str, list[tuple[str, float]]] = {}
        # Аффинность гайд → сфера: {guide_id: {sphere: count}}
        self._sphere_affinity: dict[str, dict[str, int]] = {}

        self._last_refresh: float = 0.0
        self._ttl = ttl_seconds
        self._min_shared = min_shared
        self._lock = asyncio.Lock()
        self._stats = {
            "requests": 0,
            "hits": 0,
            "misses": 0,
            "personalized_requests": 0,
        }

    async def _ensure_fresh(self) -> None:
        """Обновляет матрицы, если TTL истёк."""
        now = time.monotonic()
        if now - self._last_refresh < self._ttl and self._matrix:
            return

        async with self._lock:
            if time.monotonic() - self._last_refresh < self._ttl and self._matrix:
                return

            from src.database.crud import (
                get_codownload_matrix,
                get_codownload_matrix_weighted,
                get_guide_sphere_affinity,
            )

            try:
                # Загружаем обе матрицы параллельно
                basic, weighted, sphere = await asyncio.gather(
                    get_codownload_matrix(min_shared=self._min_shared),
                    get_codownload_matrix_weighted(min_shared=self._min_shared),
                    get_guide_sphere_affinity(),
                )
                self._matrix = basic
                self._weighted_matrix = weighted
                self._sphere_affinity = sphere
                self._last_refresh = time.monotonic()

                total_pairs = sum(len(v) for v in self._matrix.values()) // 2
                sphere_guides = len(self._sphere_affinity)
                logger.info(
                    "SmartRecommender: refreshed — %d guides, %d pairs, %d with sphere data",
                    len(self._matrix), total_pairs, sphere_guides,
                )
            except Exception as e:
                logger.error("SmartRecommender refresh failed: %s", e)

    async def get_recommendation(
        self,
        guide_id: str,
        *,
        exclude: set[str] | None = None,
    ) -> str | None:
        """Базовая рекомендация: лучший co-download (по весу).

        Args:
            guide_id: Текущий гайд.
            exclude: Множество guide_id для исключения (уже скачанные).

        Returns:
            guide_id рекомендованного гайда или ``None``.
        """
        await self._ensure_fresh()
        self._stats["requests"] += 1

        # Предпочитаем взвешенную матрицу
        pairs = self._weighted_matrix.get(guide_id) or self._matrix.get(guide_id, [])
        if not pairs:
            self._stats["misses"] += 1
            return None

        skip = exclude or set()
        for other_id, _score in pairs:
            if other_id not in skip and other_id != guide_id:
                self._stats["hits"] += 1
                return other_id

        self._stats["misses"] += 1
        return None

    async def get_personalized_recommendation(
        self,
        guide_id: str,
        user_id: int,
        *,
        exclude: set[str] | None = None,
        user_sphere: str = "",
    ) -> str | None:
        """Персонализированная рекомендация с учётом сферы бизнеса.

        Ранжирование:
        1. Берём co-download кандидатов из взвешенной матрицы
        2. Бустим кандидатов, популярных в сфере пользователя
        3. Возвращаем лучшего

        Args:
            guide_id: Текущий гайд.
            user_id: ID пользователя (для получения сферы из БД если не передана).
            exclude: Множество guide_id для исключения.
            user_sphere: Сфера бизнеса (если пустая — берём из БД).

        Returns:
            guide_id или ``None``.
        """
        await self._ensure_fresh()
        self._stats["personalized_requests"] += 1
        self._stats["requests"] += 1

        # Получаем сферу пользователя
        sphere = user_sphere
        if not sphere:
            try:
                from src.database.crud import get_lead_by_user_id
                lead = await get_lead_by_user_id(user_id)
                sphere = getattr(lead, "business_sphere", "") or "" if lead else ""
            except Exception:
                pass

        pairs = self._weighted_matrix.get(guide_id) or self._matrix.get(guide_id, [])
        if not pairs:
            self._stats["misses"] += 1
            return None

        skip = exclude or set()

        # Скоринг кандидатов
        scored: list[tuple[str, float]] = []
        for other_id, base_score in pairs:
            if other_id in skip or other_id == guide_id:
                continue

            score = float(base_score)

            # Буст за совпадение сферы
            if sphere and self._sphere_affinity.get(other_id):
                affinity = self._sphere_affinity[other_id]
                total_sphere_users = sum(affinity.values())
                same_sphere_users = affinity.get(sphere, 0)

                if total_sphere_users > 0 and same_sphere_users > 0:
                    sphere_ratio = same_sphere_users / total_sphere_users
                    score *= (1 + sphere_ratio * _SPHERE_BOOST)

            scored.append((other_id, score))

        if not scored:
            self._stats["misses"] += 1
            return None

        scored.sort(key=lambda x: -x[1])
        self._stats["hits"] += 1
        return scored[0][0]

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

    async def get_top_weighted_pairs(self, limit: int = 20) -> list[tuple[str, str, float]]:
        """Топ пар по взвешенному скору (свежие скачивания весят больше).

        Returns:
            [(guide_a, guide_b, weighted_score), ...]
        """
        await self._ensure_fresh()

        seen: set[tuple[str, str]] = set()
        result: list[tuple[str, str, float]] = []

        for guide_a, pairs in self._weighted_matrix.items():
            for guide_b, score in pairs:
                key = (min(guide_a, guide_b), max(guide_a, guide_b))
                if key not in seen:
                    seen.add(key)
                    result.append((guide_a, guide_b, score))

        result.sort(key=lambda x: -x[2])
        return result[:limit]

    async def get_sphere_report(self) -> dict[str, list[tuple[str, int]]]:
        """Отчёт: какие сферы скачивают какие гайды.

        Returns:
            ``{guide_id: [(sphere, count), ...]}``
        """
        await self._ensure_fresh()
        report: dict[str, list[tuple[str, int]]] = {}
        for gid, spheres in self._sphere_affinity.items():
            sorted_spheres = sorted(spheres.items(), key=lambda x: -x[1])
            report[gid] = sorted_spheres
        return report

    def get_stats(self) -> dict:
        """Статистика использования рекомендаций."""
        total = self._stats["requests"]
        hit_rate = (self._stats["hits"] / total * 100) if total > 0 else 0
        return {
            **self._stats,
            "hit_rate": round(hit_rate, 1),
            "matrix_size": len(self._matrix),
            "weighted_matrix_size": len(self._weighted_matrix),
            "sphere_guides": len(self._sphere_affinity),
            "is_loaded": self.is_loaded,
        }

    def invalidate(self) -> None:
        """Сбрасывает кеш (для /refresh)."""
        self._last_refresh = 0.0
        self._matrix.clear()
        self._weighted_matrix.clear()
        self._sphere_affinity.clear()

    @property
    def is_loaded(self) -> bool:
        return bool(self._matrix)


# Глобальный экземпляр
smart_recommender = SmartRecommender()
