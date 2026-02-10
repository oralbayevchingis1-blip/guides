"""C9. Vector Search 2.0 — поиск по смыслу в Consult Log и статьях.

Реализует семантический поиск: если похожий вопрос уже решался,
AI берёт за основу старый проверенный ответ.

C8. Practice Area AI — узкоспециализированный контекст по отрасли.

C10. QA Audit AI — еженедельный аудит качества ответов.
"""

import asyncio
import logging
import re
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
#  C9: Vector Search 2.0 (TF-IDF + cosine similarity)
# ═══════════════════════════════════════════════════════════════════════════

# Стоп-слова (расширенные)
_STOP_WORDS = {
    "и", "в", "на", "с", "по", "из", "к", "у", "о", "за", "от", "до",
    "не", "что", "как", "это", "то", "все", "его", "но", "да", "мы",
    "он", "она", "они", "вы", "мне", "нас", "вас", "ему", "ей", "их",
    "быть", "был", "была", "были", "будет", "есть", "ли", "же", "бы",
    "для", "при", "так", "ещё", "еще", "уже", "тоже", "или", "а",
    "ваш", "наш", "этот", "тот", "мой", "свой", "какой", "который",
    "нужно", "может", "можно", "очень", "только", "даже", "через",
    "здравствуйте", "добрый", "день", "пожалуйста", "спасибо", "подскажите",
}


def _tokenize(text: str) -> list[str]:
    """Токенизация с удалением стоп-слов."""
    words = re.findall(r'[а-яёa-z0-9]+', text.lower())
    return [w for w in words if w not in _STOP_WORDS and len(w) > 2]


def _compute_tfidf(docs: list[list[str]]) -> tuple[list[dict], dict]:
    """Вычисляет TF-IDF для списка документов."""
    import math

    doc_freq = defaultdict(int)
    for doc in docs:
        seen = set()
        for w in doc:
            if w not in seen:
                doc_freq[w] += 1
                seen.add(w)

    n_docs = len(docs)
    idf = {}
    for word, df in doc_freq.items():
        idf[word] = math.log(n_docs / (df + 1)) + 1

    tfidf_docs = []
    for doc in docs:
        tf = defaultdict(int)
        for w in doc:
            tf[w] += 1
        max_tf = max(tf.values()) if tf else 1
        tfidf = {}
        for w, count in tf.items():
            tfidf[w] = (count / max_tf) * idf.get(w, 1)
        tfidf_docs.append(tfidf)

    return tfidf_docs, idf


def _cosine_similarity(vec_a: dict, vec_b: dict) -> float:
    """Косинусное сходство между двумя TF-IDF векторами."""
    import math

    common = set(vec_a.keys()) & set(vec_b.keys())
    if not common:
        return 0.0

    dot = sum(vec_a[w] * vec_b[w] for w in common)
    norm_a = math.sqrt(sum(v ** 2 for v in vec_a.values()))
    norm_b = math.sqrt(sum(v ** 2 for v in vec_b.values()))

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot / (norm_a * norm_b)


# In-memory индекс
_index: list[dict] = []  # [{text, tokens, tfidf, source, metadata}]
_index_built = False


def build_index(entries: list[dict]) -> int:
    """Строит поисковый индекс из записей.

    Args:
        entries: [{"text": str, "source": str, "metadata": dict}]

    Returns:
        Количество проиндексированных записей.
    """
    global _index, _index_built

    if not entries:
        return 0

    docs = []
    for entry in entries:
        text = entry.get("text", "")
        tokens = _tokenize(text)
        if tokens:
            docs.append(tokens)
            _index.append({
                "text": text[:1000],
                "tokens": tokens,
                "source": entry.get("source", ""),
                "metadata": entry.get("metadata", {}),
            })

    if docs:
        tfidf_docs, _ = _compute_tfidf(docs)
        for i, tfidf in enumerate(tfidf_docs):
            if i < len(_index):
                _index[i]["tfidf"] = tfidf

    _index_built = True
    logger.info("Vector index built: %d entries", len(_index))
    return len(_index)


def search_similar(query: str, top_k: int = 5, min_score: float = 0.1) -> list[dict]:
    """Ищет похожие записи по смыслу.

    Returns:
        [{text, source, score, metadata}]
    """
    if not _index:
        return []

    query_tokens = _tokenize(query)
    if not query_tokens:
        return []

    # Строим TF для запроса
    from collections import Counter
    tf = Counter(query_tokens)
    max_tf = max(tf.values())
    query_vec = {w: count / max_tf for w, count in tf.items()}

    # Считаем сходство со всеми документами
    results = []
    for entry in _index:
        tfidf = entry.get("tfidf", {})
        if not tfidf:
            continue
        score = _cosine_similarity(query_vec, tfidf)
        if score >= min_score:
            results.append({
                "text": entry["text"],
                "source": entry["source"],
                "score": round(score, 4),
                "metadata": entry.get("metadata", {}),
            })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]


async def search_consult_history(
    query: str, google=None, cache=None, top_k: int = 3,
) -> list[dict]:
    """Ищет похожие вопросы в истории консультаций.

    Загружает Consult Log и статьи, строит индекс (при первом вызове)
    и ищет похожие записи.
    """
    global _index_built

    # Перестраиваем индекс если он пустой и Google доступен
    if not _index_built and google:
        try:
            entries = []

            # Consult Log
            consult_log = await google.get_consult_log(300)
            for entry in consult_log:
                q = entry.get("question", "")
                a = entry.get("answer", "")
                if q:
                    entries.append({
                        "text": f"Вопрос: {q}\nОтвет: {a}",
                        "source": "consult_log",
                        "metadata": {"user_id": entry.get("user_id", "")},
                    })

            # Статьи
            articles = await google.get_articles_list(limit=50)
            for art in articles:
                title = art.get("title", "")
                content = art.get("content", art.get("description", ""))
                if title:
                    entries.append({
                        "text": f"{title}\n{content[:500]}",
                        "source": "article",
                        "metadata": {"title": title},
                    })

            if entries:
                build_index(entries)
        except Exception as e:
            logger.warning("Failed to build vector index: %s", e)

    return search_similar(query, top_k=top_k)


def format_search_results(results: list[dict]) -> str:
    """Форматирует результаты поиска в контекст для AI."""
    if not results:
        return ""

    parts = ["📚 ПОХОЖИЕ ПРЕЦЕДЕНТЫ ИЗ БАЗЫ ЗНАНИЙ:"]
    for i, r in enumerate(results, 1):
        source = "📝 Консультация" if r["source"] == "consult_log" else "📰 Статья"
        parts.append(f"\n{i}. {source} (релевантность: {r['score']:.0%}):")
        parts.append(f"   {r['text'][:300]}")

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════════════
#  C8: Practice Area AI
# ═══════════════════════════════════════════════════════════════════════════

PRACTICE_AREAS = {
    "tax": {
        "name": "Налоговое право",
        "keywords": ["налог", "кпн", "ндс", "ипн", "декларац", "бюджет", "фискал", "налоговая"],
        "context": (
            "СПЕЦИАЛИЗАЦИЯ: Налоговое право РК.\n"
            "Ключевые НПА: Кодекс РК «О налогах и других обязательных платежах» (НК РК).\n"
            "Обращай внимание на: ставки налогов, сроки сдачи деклараций, "
            "налоговые льготы (СЭЗ, IT-парк, МФЦА), трансфертное ценообразование.\n"
            "При ответе указывай КОНКРЕТНЫЕ статьи НК РК."
        ),
    },
    "it_aifc": {
        "name": "IT-право и МФЦА",
        "keywords": ["мфца", "aifc", "it", "стартап", "цифров", "данные", "персональн", "gdpr"],
        "context": (
            "СПЕЦИАЛИЗАЦИЯ: IT-право, право МФЦА (Международный финансовый центр Астана).\n"
            "Ключевые НПА: Конституционный закон о МФЦА, AIFC Regulations, "
            "AIFC Data Protection Regulations 2020, Companies Regulations 2017.\n"
            "В МФЦА действует АНГЛИЙСКОЕ ОБЩЕЕ ПРАВО. Указывай это.\n"
            "Для IT: Закон о персональных данных РК, Закон об информатизации."
        ),
    },
    "corporate": {
        "name": "Корпоративное право",
        "keywords": ["тоо", "ао", "участник", "учредитель", "устав", "доля", "акци", "дивиденд"],
        "context": (
            "СПЕЦИАЛИЗАЦИЯ: Корпоративное право РК.\n"
            "Ключевые НПА: Закон о ТОО, Закон об АО, ГК РК (Часть общая).\n"
            "Обращай внимание на: уставный капитал, доли участников, "
            "выход участника (ст. 69 Закона о ТОО), реорганизация, ликвидация."
        ),
    },
    "labor": {
        "name": "Трудовое право",
        "keywords": ["работник", "работодатель", "трудов", "уволь", "зарплат", "отпуск", "больничн"],
        "context": (
            "СПЕЦИАЛИЗАЦИЯ: Трудовое право РК.\n"
            "Ключевые НПА: Трудовой кодекс РК (ТК РК).\n"
            "Ключевые статьи: ст. 28 (содержание ТД), ст. 49-56 (прекращение), "
            "ст. 65 (дисциплинарные), ст. 87-93 (отпуска), ст. 101 (оплата труда).\n"
            "НП ВС РК №1 от 28.11.2024 — важно для аттестации."
        ),
    },
    "litigation": {
        "name": "Судебные споры",
        "keywords": ["суд", "иск", "истец", "ответчик", "апелляц", "кассац", "арбитраж", "взыскан"],
        "context": (
            "СПЕЦИАЛИЗАЦИЯ: Судебные споры и арбитраж.\n"
            "Ключевые НПА: ГПК РК, Закон об арбитраже, Закон об исполнительном производстве.\n"
            "Указывай: сроки исковой давности (3 года общий — ст. 178 ГК РК), "
            "подсудность, размер госпошлины, порядок обжалования."
        ),
    },
    "ip": {
        "name": "Интеллектуальная собственность",
        "keywords": ["авторск", "патент", "товарный знак", "бренд", "лицензи", "франшиз"],
        "context": (
            "СПЕЦИАЛИЗАЦИЯ: Интеллектуальная собственность.\n"
            "Ключевые НПА: Книга 5 ГК РК, Патентный закон РК, "
            "Закон о товарных знаках.\n"
            "Обращай внимание на: регистрацию, сроки охраны, лицензионные договоры."
        ),
    },
}


def detect_practice_area(question: str) -> list[dict]:
    """Определяет область права по вопросу.

    Returns:
        Список подходящих областей [{name, context}].
    """
    q_lower = question.lower()
    matched = []

    for area_id, area in PRACTICE_AREAS.items():
        score = 0
        for kw in area["keywords"]:
            if kw in q_lower:
                score += 1

        if score > 0:
            matched.append({
                "id": area_id,
                "name": area["name"],
                "context": area["context"],
                "score": score,
            })

    matched.sort(key=lambda x: x["score"], reverse=True)
    return matched[:2]  # Макс 2 области


def get_practice_context(question: str) -> str:
    """Получает специализированный контекст по области права.

    Returns:
        Дополнительный контекст для AI.
    """
    areas = detect_practice_area(question)
    if not areas:
        return ""

    parts = ["🎯 СПЕЦИАЛИЗИРОВАННЫЙ КОНТЕКСТ:"]
    for area in areas:
        parts.append(f"\n[{area['name']}]")
        parts.append(area["context"])

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════════════
#  C10: QA Audit AI
# ═══════════════════════════════════════════════════════════════════════════


async def run_qa_audit(google=None, cache=None, bot=None) -> str:
    """Еженедельный AI-аудит качества ответов.

    Анализирует 10 случайных диалогов с низкой оценкой
    и генерирует отчёт с рекомендациями.

    Returns:
        Отчёт в HTML для Telegram.
    """
    from src.bot.utils.ai_client import get_orchestrator
    import random

    ai = get_orchestrator()

    # Собираем диалоги с низкой оценкой
    low_rated = []
    try:
        from src.database.models import async_session, FeedbackScore
        from sqlalchemy import select

        async with async_session() as session:
            result = await session.execute(
                select(FeedbackScore)
                .where(FeedbackScore.score <= 3)
                .order_by(FeedbackScore.created_at.desc())
                .limit(50)
            )
            low_scores = result.scalars().all()

        # Дополняем данными из Consult Log
        if google and low_scores:
            consult_log = await google.get_consult_log(200)
            for score_entry in low_scores:
                for cl in consult_log:
                    if str(cl.get("user_id", "")) == str(score_entry.user_id):
                        low_rated.append({
                            "user_id": score_entry.user_id,
                            "score": score_entry.score,
                            "question": cl.get("question", ""),
                            "answer": cl.get("answer", ""),
                        })
                        break

    except Exception as e:
        logger.warning("QA Audit: failed to get low-rated dialogues: %s", e)

    # Fallback: берём последние записи из Consult Log
    if not low_rated and google:
        try:
            consult_log = await google.get_consult_log(50)
            low_rated = [
                {
                    "user_id": cl.get("user_id", ""),
                    "score": "N/A",
                    "question": cl.get("question", ""),
                    "answer": cl.get("answer", ""),
                }
                for cl in consult_log[:20]
            ]
        except Exception:
            pass

    if not low_rated:
        return "ℹ️ Нет данных для аудита качества."

    # Берём 10 случайных
    sample = random.sample(low_rated, min(10, len(low_rated)))

    dialogues_text = ""
    for i, d in enumerate(sample, 1):
        dialogues_text += (
            f"\nДиалог {i} (оценка: {d['score']}):\n"
            f"  Вопрос: {d['question'][:200]}\n"
            f"  Ответ: {d['answer'][:200]}\n"
        )

    prompt = (
        "Ты — QA-аудитор юридической фирмы SOLIS Partners.\n\n"
        "Проанализируй эти 10 диалогов и напиши отчёт:\n\n"
        f"{dialogues_text}\n\n"
        "ФОРМАТ ОТЧЁТА:\n"
        "1. 📊 <b>Общая оценка качества</b> (от 1 до 10)\n"
        "2. 🔴 <b>Критические проблемы</b> (ошибки, которые нужно исправить немедленно)\n"
        "3. 🟡 <b>Рекомендации по улучшению промптов</b>\n"
        "4. 🟢 <b>Что хорошо работает</b>\n"
        "5. 📝 <b>Конкретные изменения в системном промпте</b>\n\n"
        "Будь конкретным. Используй HTML для Telegram."
    )

    try:
        report = await ai.call_with_fallback(
            prompt,
            "Ты — директор по качеству юридической фирмы. Анализируй объективно.",
            primary="openai", max_tokens=2048, temperature=0.4,
        )
    except Exception as e:
        logger.error("QA Audit AI failed: %s", e)
        report = f"❌ AI-аудит не удался: {e}"

    header = (
        "📊 <b>Еженедельный аудит качества</b>\n"
        f"Проанализировано: {len(sample)} диалогов\n\n"
    )

    return header + report


async def scheduled_qa_audit(bot=None, google=None, cache=None) -> None:
    """Scheduled задача для еженедельного QA аудита."""
    from src.config import settings

    report = await run_qa_audit(google=google, cache=cache, bot=bot)

    if bot:
        try:
            if len(report) > 4000:
                for i in range(0, len(report), 4000):
                    await bot.send_message(settings.ADMIN_ID, report[i:i+4000])
            else:
                await bot.send_message(settings.ADMIN_ID, report)
        except Exception as e:
            logger.error("QA report send failed: %s", e)
