"""Парсер новостей для мониторинга правового и IT-пространства Казахстана.

Источники:
    - zakon.kz (RSS)
    - digitalbusiness.kz (RSS)
    - Google News (RSS по ключевым словам)

Использование:
    from src.bot.utils.news_parser import fetch_all_news
    news = await fetch_all_news()
"""

import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# RSS-источники
RSS_FEEDS = [
    {
        "source": "zakon.kz",
        "url": "https://www.zakon.kz/rss/",
        "keywords": [
            "IT", "цифров", "бизнес", "предпринимат", "инвестиц",
            "МФЦА", "AIFC", "налог", "корпоратив", "M&A", "стартап",
            "кибер", "данных", "персональн", "закон", "право",
        ],
    },
    {
        "source": "digitalbusiness.kz",
        "url": "https://digitalbusiness.kz/feed/",
        "keywords": [],  # Все новости релевантны
    },
]

# Google News RSS — ключевые запросы
GOOGLE_NEWS_QUERIES = [
    "Казахстан IT право закон",
    "МФЦА AIFC новости",
    "Казахстан стартапы инвестиции",
]


def _parse_feed_sync(url: str) -> list[dict]:
    """Синхронный парсинг одного RSS-канала."""
    try:
        import feedparser

        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries[:15]:
            items.append({
                "title": entry.get("title", ""),
                "url": entry.get("link", ""),
                "summary": entry.get("summary", "")[:200],
                "published": entry.get("published", ""),
            })
        return items
    except ImportError:
        logger.warning("feedparser не установлен. pip install feedparser")
        return []
    except Exception as e:
        logger.error("Ошибка парсинга %s: %s", url, e)
        return []


def _filter_by_keywords(items: list[dict], keywords: list[str]) -> list[dict]:
    """Фильтрует новости по ключевым словам."""
    if not keywords:
        return items

    filtered = []
    for item in items:
        text = (item.get("title", "") + " " + item.get("summary", "")).lower()
        if any(kw.lower() in text for kw in keywords):
            filtered.append(item)
    return filtered


def _fetch_all_sync() -> list[dict]:
    """Синхронно собирает новости из всех источников."""
    all_news = []

    for feed_config in RSS_FEEDS:
        items = _parse_feed_sync(feed_config["url"])
        keywords = feed_config.get("keywords", [])
        filtered = _filter_by_keywords(items, keywords)

        for item in filtered:
            item["source"] = feed_config["source"]

        all_news.extend(filtered)
        logger.info(
            "Новости %s: %d/%d (после фильтрации)",
            feed_config["source"],
            len(filtered),
            len(items),
        )

    # Google News RSS
    for query in GOOGLE_NEWS_QUERIES:
        try:
            google_url = f"https://news.google.com/rss/search?q={query.replace(' ', '+')}&hl=ru&gl=KZ&ceid=KZ:ru"
            items = _parse_feed_sync(google_url)
            for item in items[:5]:
                item["source"] = f"Google News ({query[:20]})"
            all_news.extend(items[:5])
        except Exception as e:
            logger.error("Google News error for '%s': %s", query, e)

    # Удаляем дубликаты по URL
    seen_urls = set()
    unique = []
    for item in all_news:
        url = item.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique.append(item)

    logger.info("Всего новостей после дедупликации: %d", len(unique))
    return unique


async def fetch_all_news() -> list[dict]:
    """Асинхронно собирает новости из всех RSS-источников.

    Returns:
        Список словарей: {source, title, url, summary, published}
    """
    return await asyncio.to_thread(_fetch_all_sync)
