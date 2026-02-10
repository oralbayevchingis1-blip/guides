"""HTML-санитайзер для статей и контента.

Очищает HTML от опасных тегов и атрибутов,
исправляет незакрытые теги, генерирует уникальные slug'и.
"""

import logging
import re
import unicodedata

import bleach
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Допустимые HTML-теги для статей
ALLOWED_TAGS = [
    "h2", "h3", "p", "ul", "ol", "li",
    "strong", "em", "b", "i",
    "blockquote", "a", "br",
]

ALLOWED_ATTRIBUTES = {
    "a": ["href", "title", "target"],
}


def sanitize_article_html(html: str) -> str:
    """Очищает HTML: оставляет только допустимые теги, удаляет опасные.

    Args:
        html: Сырой HTML-контент из AI или пользовательского ввода.

    Returns:
        Очищенный HTML.
    """
    if not html:
        return ""

    cleaned = bleach.clean(
        html,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        strip=True,
    )

    return cleaned


def fix_broken_tags(html: str) -> str:
    """Исправляет незакрытые теги через BeautifulSoup.

    Args:
        html: HTML с возможно невалидной разметкой.

    Returns:
        Валидный HTML.
    """
    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")
    return str(soup)


def sanitize_and_fix(html: str) -> str:
    """Полная очистка: sanitize + fix broken tags."""
    return fix_broken_tags(sanitize_article_html(html))


# ── Slug генерация ───────────────────────────────────────────────────────

_TRANSLIT_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "yo",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    "ә": "a", "і": "i", "ғ": "g", "қ": "q", "ң": "n", "ө": "o",
    "ұ": "u", "ү": "u", "һ": "h",
}


def slugify(text: str) -> str:
    """Генерирует URL-slug из текста с транслитерацией.

    Args:
        text: Произвольный текст (заголовок статьи).

    Returns:
        Slug вида 'uvol-nenie-bez-riskov'.
    """
    text = text.lower().strip()

    # Транслитерация
    result = []
    for ch in text:
        if ch in _TRANSLIT_MAP:
            result.append(_TRANSLIT_MAP[ch])
        elif ch.isascii() and (ch.isalnum() or ch in "-_ "):
            result.append(ch)
        else:
            # Попробовать NFD-декомпозицию
            nfd = unicodedata.normalize("NFD", ch)
            for c in nfd:
                if c.isascii() and c.isalnum():
                    result.append(c)
    slug = "".join(result)

    # Замена пробелов/подчёркиваний на дефис
    slug = re.sub(r"[\s_]+", "-", slug)
    # Убираем дублирующие дефисы
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")

    return slug[:80] if slug else "article"


async def unique_slug(text: str, existing_slugs: list[str]) -> str:
    """Генерирует уникальный slug, добавляя суффикс при дубликате.

    Args:
        text: Заголовок.
        existing_slugs: Уже занятые slug'и.

    Returns:
        Уникальный slug.
    """
    base = slugify(text)
    if base not in existing_slugs:
        return base

    counter = 2
    while f"{base}-{counter}" in existing_slugs:
        counter += 1
    return f"{base}-{counter}"
