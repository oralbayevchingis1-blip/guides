"""A/B Testing Engine — детерминированное распределение пользователей по вариантам.

Использование:

    from src.bot.utils.ab_testing import ab_get_text, ab_convert

    # В хендлере — получить текст для пользователя:
    text = await ab_get_text(
        experiment="welcome_v2",
        user_id=callback.from_user.id,
        default="Стандартный текст приветствия",
    )

    # При конверсии (скачивание, консультация):
    await ab_convert(user_id, step="pdf_delivered")

Эксперименты создаются через /ab_create (admin).
Результаты смотрятся через /ab_results (admin).
"""

import hashlib
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Кэш активных экспериментов (обновляется при старте и /ab_create)
_experiments_cache: dict[str, dict] = {}
_cache_loaded = False


async def _ensure_cache() -> None:
    """Загружает активные эксперименты в кэш (lazy)."""
    global _cache_loaded
    if _cache_loaded:
        return
    await refresh_experiments_cache()


async def refresh_experiments_cache() -> None:
    """Перезагружает кэш экспериментов из БД."""
    global _experiments_cache, _cache_loaded
    try:
        from src.database.crud import get_active_experiments
        experiments = await get_active_experiments()
        _experiments_cache = {e["name"]: e for e in experiments}
        _cache_loaded = True
        logger.info("AB cache refreshed: %d active experiments", len(_experiments_cache))
    except Exception as e:
        logger.warning("AB cache refresh failed: %s", e)
        _cache_loaded = True


def _pick_variant(user_id: int, experiment_name: str, variant_keys: list[str]) -> str:
    """Детерминированно выбирает вариант на основе hash(user_id + experiment).

    Один и тот же пользователь всегда получит один и тот же вариант
    для одного и того же эксперимента.
    """
    if not variant_keys:
        return "A"
    raw = f"{user_id}:{experiment_name}"
    h = int(hashlib.md5(raw.encode()).hexdigest(), 16)
    idx = h % len(variant_keys)
    return variant_keys[idx]


async def ab_get_variant(
    experiment: str,
    user_id: int,
) -> tuple[str, str | None]:
    """Возвращает (variant_key, variant_text) для пользователя.

    Если эксперимент не найден или неактивен, возвращает ("control", None).
    """
    await _ensure_cache()

    exp = _experiments_cache.get(experiment)
    if not exp:
        return ("control", None)

    variants = exp.get("variants", {})
    if not variants:
        return ("control", None)

    # Проверяем, есть ли уже назначение
    from src.database.crud import get_user_ab_variant, assign_ab_variant

    existing = await get_user_ab_variant(experiment, user_id)
    if existing:
        return (existing, variants.get(existing))

    # Назначаем новый вариант
    variant_keys = sorted(variants.keys())
    chosen = _pick_variant(user_id, experiment, variant_keys)

    # Сохраняем назначение (fire-and-forget safe)
    try:
        await assign_ab_variant(experiment, user_id, chosen)
    except Exception as e:
        logger.warning("AB assign failed: %s", e)

    return (chosen, variants.get(chosen))


async def ab_get_text(
    experiment: str,
    user_id: int,
    default: str = "",
) -> str:
    """Получает текст для пользователя из A/B эксперимента.

    Если эксперимент неактивен — возвращает default.
    """
    variant_key, variant_text = await ab_get_variant(experiment, user_id)

    if variant_text is not None:
        return variant_text

    return default


async def ab_convert(user_id: int, step: str) -> None:
    """Отмечает конверсию для всех активных экспериментов с данной метрикой.

    Вызывается при ключевых событиях воронки (pdf_delivered, consultation, etc.).
    Fire-and-forget — не блокирует хендлер.
    """
    await _ensure_cache()

    from src.database.crud import mark_ab_conversion

    for name, exp in _experiments_cache.items():
        if exp.get("metric") == step:
            try:
                await mark_ab_conversion(name, user_id)
            except Exception as e:
                logger.warning("AB conversion mark failed (%s/%s): %s", name, user_id, e)
