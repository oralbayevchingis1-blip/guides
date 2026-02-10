"""Тесты TTL-кеша."""

import asyncio

import pytest

from src.bot.utils.cache import TTLCache


@pytest.mark.asyncio
async def test_cache_returns_fetched_value():
    """Кеш вызывает fetcher и возвращает результат."""
    cache = TTLCache(ttl_seconds=60)

    async def fetcher():
        return {"key": "value"}

    result = await cache.get_or_fetch("test", fetcher)
    assert result == {"key": "value"}


@pytest.mark.asyncio
async def test_cache_returns_cached_value():
    """Повторный вызов не вызывает fetcher."""
    cache = TTLCache(ttl_seconds=60)
    call_count = 0

    async def fetcher():
        nonlocal call_count
        call_count += 1
        return "data"

    await cache.get_or_fetch("test", fetcher)
    await cache.get_or_fetch("test", fetcher)
    await cache.get_or_fetch("test", fetcher)

    assert call_count == 1


@pytest.mark.asyncio
async def test_cache_invalidate_key():
    """Сброс конкретного ключа."""
    cache = TTLCache(ttl_seconds=60)
    call_count = 0

    async def fetcher():
        nonlocal call_count
        call_count += 1
        return f"version_{call_count}"

    await cache.get_or_fetch("test", fetcher)
    cache.invalidate("test")
    result = await cache.get_or_fetch("test", fetcher)

    assert call_count == 2
    assert result == "version_2"


@pytest.mark.asyncio
async def test_cache_invalidate_all():
    """Сброс всего кеша."""
    cache = TTLCache(ttl_seconds=60)

    async def fetcher_a():
        return "a"

    async def fetcher_b():
        return "b"

    await cache.get_or_fetch("a", fetcher_a)
    await cache.get_or_fetch("b", fetcher_b)

    cache.invalidate()

    assert cache._store == {}


@pytest.mark.asyncio
async def test_cache_ttl_expiry():
    """Кеш обновляется после истечения TTL."""
    cache = TTLCache(ttl_seconds=0)  # TTL = 0 — сразу устаревает
    call_count = 0

    async def fetcher():
        nonlocal call_count
        call_count += 1
        return call_count

    result1 = await cache.get_or_fetch("test", fetcher)
    result2 = await cache.get_or_fetch("test", fetcher)

    assert result1 == 1
    assert result2 == 2


@pytest.mark.asyncio
async def test_cache_returns_stale_on_error():
    """Если fetcher упал, возвращаем устаревшие данные."""
    cache = TTLCache(ttl_seconds=0)  # TTL = 0 — всегда перезапрашивает

    async def good_fetcher():
        return "good_data"

    async def bad_fetcher():
        raise RuntimeError("API down")

    # Первый вызов — успех
    result = await cache.get_or_fetch("test", good_fetcher)
    assert result == "good_data"

    # Второй вызов — ошибка, но есть устаревшие данные
    result = await cache.get_or_fetch("test", bad_fetcher)
    assert result == "good_data"
