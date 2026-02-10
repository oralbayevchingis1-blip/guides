"""RAG (Retrieval-Augmented Generation) — умный контекст для AI.

Ищет релевантные записи из Data Room и статей перед генерацией ответа.
Использует keyword-matching + AI query expansion для ранжирования.

Использование:
    context = await find_relevant_context("аттестация увольнение", google, cache)
    answer = await ask_legal(question, context=context)
"""

import logging
import re

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient

logger = logging.getLogger(__name__)

# Стоп-слова для фильтрации
_STOP_WORDS = frozenset({
    "и", "в", "на", "с", "по", "для", "от", "из", "к", "о", "об", "до",
    "как", "что", "это", "при", "не", "или", "а", "но", "же", "ли",
    "бы", "если", "чтобы", "так", "уже", "ещё", "еще", "все", "вот",
    "мне", "мой", "свой", "нужно", "можно", "есть", "быть", "будет",
    "the", "is", "in", "to", "and", "of", "for", "a", "an", "it",
})


def _tokenize(text: str) -> set[str]:
    """Разбивает текст на уникальные слова (lowercase, без стоп-слов)."""
    words = re.findall(r"[a-zA-Zа-яА-ЯёЁ]{3,}", text.lower())
    return {w for w in words if w not in _STOP_WORDS}


def _score_entry(query_tokens: set[str], entry_text: str) -> int:
    """Считает количество совпадений токенов запроса с текстом записи."""
    entry_tokens = _tokenize(entry_text)
    return len(query_tokens & entry_tokens)


async def _expand_query_with_ai(query: str) -> set[str]:
    """AI Query Expansion: расширяет запрос семантически связанными ключами.

    Бот сам формулирует дополнительные поисковые ключи для максимального
    извлечения контекста из Data Room.
    """
    try:
        from src.bot.utils.ai_client import get_orchestrator

        ai = get_orchestrator()
        expansion_prompt = (
            f"Для поискового запроса юридической тематики: «{query}»\n\n"
            "Сгенерируй 5-10 дополнительных поисковых слов и фраз (на русском), "
            "которые семантически связаны с этим запросом.\n"
            "Включи: синонимы, связанные юридические термины, номера статей законов, "
            "названия законов РК, связанные практики.\n\n"
            "Верни ТОЛЬКО слова через запятую, без нумерации и пояснений.\n"
            "Пример: трудовой кодекс, увольнение, аттестация, ст 52, ТК РК, квалификация"
        )

        expanded = await ai.call_gemini(
            expansion_prompt,
            "Ты — поисковый ассистент. Генерируй ключевые слова для поиска.",
            max_tokens=256,
            temperature=0.3,
        )

        # Парсим результат — слова через запятую
        extra_tokens = set()
        for part in expanded.split(","):
            part = part.strip().lower()
            tokens = _tokenize(part)
            extra_tokens.update(tokens)

        logger.info("RAG query expansion: +%d tokens for '%s'", len(extra_tokens), query[:50])
        return extra_tokens

    except Exception as e:
        logger.warning("RAG query expansion failed: %s", e)
        return set()


async def find_relevant_context(
    query: str,
    google: GoogleSheetsClient,
    cache: TTLCache,
    *,
    top_k: int = 5,
    expand: bool = True,
) -> str:
    """Ищет релевантные записи из Data Room и статей для AI-контекста.

    Args:
        query: Вопрос или запрос пользователя.
        google: Google Sheets клиент.
        cache: TTL-кеш.
        top_k: Количество топ записей для возврата.
        expand: Использовать AI query expansion (по умолчанию True).

    Returns:
        Форматированная строка с релевантными записями.
    """
    query_tokens = _tokenize(query)
    if not query_tokens:
        return ""

    # AI Query Expansion — расширяем набор токенов
    if expand:
        expanded_tokens = await _expand_query_with_ai(query)
        # Оригинальные токены имеют вес 2x (добавляем дубль)
        all_tokens = query_tokens | expanded_tokens
    else:
        all_tokens = query_tokens

    scored: list[tuple[int, str, str]] = []  # (score, source, text)

    # 1. Data Room
    try:
        data_room = await cache.get_or_fetch("data_room", google.get_data_room)
        for item in data_room:
            title = item.get("title", item.get("Заголовок", ""))
            content = item.get("content", item.get("Описание", ""))
            category = item.get("category", item.get("Категория", ""))
            full_text = f"{category} {title} {content}"
            # Двойной вес для оригинальных токенов
            base_score = _score_entry(query_tokens, full_text) * 2
            expanded_score = _score_entry(all_tokens - query_tokens, full_text) if expand else 0
            total_score = base_score + expanded_score
            if total_score > 0:
                entry = f"[{category}] {title}: {content[:300]}"
                scored.append((total_score, "Data Room", entry))
    except Exception as e:
        logger.warning("RAG: ошибка загрузки Data Room: %s", e)

    # 2. Статьи сайта
    try:
        articles = await google.get_articles_list(limit=30)
        for art in articles:
            title = art.get("title", art.get("Заголовок", ""))
            desc = art.get("description", art.get("Описание", ""))
            cat = art.get("category", art.get("Категория", ""))
            full_text = f"{cat} {title} {desc}"
            base_score = _score_entry(query_tokens, full_text) * 2
            expanded_score = _score_entry(all_tokens - query_tokens, full_text) if expand else 0
            total_score = base_score + expanded_score
            if total_score > 0:
                entry = f"[Статья: {cat}] {title}: {desc[:200]}"
                scored.append((total_score, "Статьи", entry))
    except Exception as e:
        logger.warning("RAG: ошибка загрузки статей: %s", e)

    if not scored:
        return ""

    # Сортируем по score (убывание), берём top_k
    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:top_k]

    lines = [f"- {entry}" for _, _, entry in top]
    return "\n".join(lines)
