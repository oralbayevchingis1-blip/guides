"""L3. Legal Search Agent — поиск по законам РК перед ответом AI.

Обогащает контекст /consult актуальными статьями законов.
Источники: Data Room + локальная база ключевых статей + (опционально) веб-поиск.

L4. Automatic Conflict Check — проверка новых клиентов на конфликт интересов.

L6. OSINT-lite — проверка контрагентов по БИН.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
#  L3: Legal Search — база ключевых статей
# ═══════════════════════════════════════════════════════════════════════════

# Наиболее часто цитируемые статьи законов РК (мини-справочник)
KEY_LEGAL_ARTICLES = {
    "увольнение": [
        "ст. 49 ТК РК — основания прекращения трудового договора",
        "ст. 50 ТК РК — расторжение по соглашению сторон",
        "ст. 52 ТК РК — расторжение по инициативе работодателя",
        "ст. 56 ТК РК — расторжение по инициативе работника",
        "ст. 65 ТК РК — порядок дисциплинарного взыскания",
        "ст. 131 ТК РК — компенсация при увольнении",
    ],
    "трудовой договор": [
        "ст. 28 ТК РК — содержание трудового договора",
        "ст. 29 ТК РК — обязательные условия",
        "ст. 30 ТК РК — срок трудового договора",
        "ст. 33 ТК РК — перевод работника",
    ],
    "тоо": [
        "ст. 2 Закона о ТОО — понятие ТОО",
        "ст. 22-23 Закона о ТОО — уставный капитал",
        "ст. 28 Закона о ТОО — права участников",
        "ст. 36 Закона о ТОО — органы управления",
        "ст. 69 Закона о ТОО — выход участника",
    ],
    "налоги": [
        "ст. 225 НК РК — объекты КПН",
        "ст. 366 НК РК — НДС",
        "ст. 316 НК РК — ИПН",
        "ст. 683 НК РК — упрощённая декларация",
    ],
    "мфца": [
        "Конституционный закон о МФЦА от 07.12.2015 №438-V",
        "AIFC Employment Framework Regulations 2017",
        "AIFC Companies Regulations 2017",
        "AIFC Data Protection Regulations 2020",
    ],
    "интеллектуальная собственность": [
        "ст. 961 ГК РК — авторское право",
        "ст. 992 ГК РК — смежные права",
        "Закон об охране селекционных достижений",
        "Патентный закон РК от 16.07.1999",
    ],
    "договор": [
        "ст. 378-383 ГК РК — общие положения о договорах",
        "ст. 393 ГК РК — ответственность за нарушение",
        "ст. 401 ГК РК — неустойка",
        "ст. 349 ГК РК — исковая давность",
    ],
    "недвижимость": [
        "ст. 118 ГК РК — право собственности",
        "Закон о государственной регистрации прав на недвижимое имущество",
        "Земельный кодекс РК",
    ],
}

# Ключевые слова для каждой темы
_TOPIC_KEYWORDS = {
    "увольнение": ["уволь", "увольн", "уволит", "расторж", "сокращ", "аттестац", "дисциплин"],
    "трудовой договор": ["трудов", "работник", "работодатель", "зарплат", "отпуск"],
    "тоо": ["тоо", "участник", "учредитель", "устав", "капитал", "доля"],
    "налоги": ["налог", "кпн", "ндс", "ипн", "декларац", "бюджет"],
    "мфца": ["мфца", "aifc", "астана", "международный финансовый"],
    "интеллектуальная собственность": ["авторск", "патент", "товарный знак", "лицензи"],
    "договор": ["договор", "контракт", "обязательств", "неустойк", "ответственност"],
    "недвижимость": ["недвижим", "земельн", "квартир", "аренд", "собственност"],
}


def find_relevant_laws(question: str) -> str:
    """Находит релевантные статьи законов по ключевым словам.

    Returns:
        Текст с релевантными статьями для AI-контекста.
    """
    q_lower = question.lower()
    matched_topics = []

    for topic, keywords in _TOPIC_KEYWORDS.items():
        for kw in keywords:
            if kw in q_lower:
                matched_topics.append(topic)
                break

    if not matched_topics:
        return ""

    lines = ["📚 АКТУАЛЬНЫЕ СТАТЬИ ЗАКОНОВ РК:"]
    seen = set()
    for topic in matched_topics:
        articles = KEY_LEGAL_ARTICLES.get(topic, [])
        for art in articles:
            if art not in seen:
                lines.append(f"  • {art}")
                seen.add(art)

    return "\n".join(lines)


async def search_legal_context(question: str, google=None, cache=None) -> str:
    """Полный поиск юридического контекста: законы + Data Room + AI-расширение.

    Returns:
        Обогащённый контекст для AI-консультации.
    """
    parts = []

    # 1. Локальная база статей
    laws = find_relevant_laws(question)
    if laws:
        parts.append(laws)

    # 2. RAG из Data Room (если доступен)
    if google and cache:
        try:
            from src.bot.utils.rag import find_relevant_context
            rag = await find_relevant_context(question, google, cache)
            if rag:
                parts.append(rag)
        except Exception as e:
            logger.warning("RAG search failed: %s", e)

    return "\n\n".join(parts)


# ═══════════════════════════════════════════════════════════════════════════
#  L4: Conflict Check
# ═══════════════════════════════════════════════════════════════════════════

async def check_conflicts(
    name: str,
    company: str = "",
    google=None,
    cache=None,
) -> dict:
    """Проверяет нового клиента на конфликт интересов.

    Сканирует листы «Лиды» и «Consult Log» на предмет упоминания
    конкурентов или связанных сторон.

    Returns:
        {"has_conflict": bool, "matches": [...], "risk_level": "LOW/MEDIUM/HIGH"}
    """
    matches = []
    search_terms = [t.strip().lower() for t in [name, company] if t.strip()]

    if not search_terms or not google:
        return {"has_conflict": False, "matches": [], "risk_level": "LOW"}

    try:
        # Получаем данные из Sheets
        leads = await google.get_recent_leads(500)
        consult_log = await google.get_consult_log(200)

        # Поиск по лидам
        for lead in leads:
            lead_name = str(lead.get("name", "")).lower()
            lead_email = str(lead.get("email", "")).lower()
            lead_company = str(lead.get("company", "")).lower()

            for term in search_terms:
                if len(term) >= 3 and (
                    term in lead_name or term in lead_email or term in lead_company
                ):
                    matches.append({
                        "type": "lead",
                        "name": lead.get("name", ""),
                        "detail": lead.get("email", ""),
                        "match_term": term,
                    })

        # Поиск по Consult Log
        for entry in consult_log:
            question = str(entry.get("question", "")).lower()
            for term in search_terms:
                if len(term) >= 3 and term in question:
                    matches.append({
                        "type": "consult",
                        "name": entry.get("user_id", ""),
                        "detail": question[:100],
                        "match_term": term,
                    })

    except Exception as e:
        logger.error("Conflict check failed: %s", e)
        return {"has_conflict": False, "matches": [], "risk_level": "LOW", "error": str(e)}

    # Определяем уровень риска
    if len(matches) >= 3:
        risk = "HIGH"
    elif len(matches) >= 1:
        risk = "MEDIUM"
    else:
        risk = "LOW"

    return {
        "has_conflict": len(matches) > 0,
        "matches": matches[:10],  # Макс 10 совпадений
        "risk_level": risk,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  L6: OSINT-lite — проверка контрагента по БИН
# ═══════════════════════════════════════════════════════════════════════════

BIN_PATTERN = re.compile(r'^\d{12}$')


def is_valid_bin(text: str) -> bool:
    """Проверяет, является ли текст валидным БИН (12 цифр)."""
    return bool(BIN_PATTERN.match(text.strip()))


async def check_counterparty_by_bin(bin_number: str) -> dict:
    """Проверяет контрагента по БИН через открытые источники.

    Использует stat.gov.kz и другие открытые API Казахстана.

    Returns:
        {"bin": str, "name": str, "status": str, "info": str, "found": bool}
    """
    bin_number = bin_number.strip()
    if not is_valid_bin(bin_number):
        return {"bin": bin_number, "found": False, "error": "Невалидный БИН (нужно 12 цифр)"}

    result = {
        "bin": bin_number,
        "found": False,
        "name": "",
        "status": "",
        "registration_date": "",
        "activity": "",
        "address": "",
        "info": "",
    }

    # Попытка получить данные через открытый API stat.gov.kz
    try:
        import aiohttp

        url = f"https://old.stat.gov.kz/api/juridical/counter/api/?bin={bin_number}&lang=ru"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and isinstance(data, dict):
                        obj = data.get("obj", data)
                        result.update({
                            "found": True,
                            "name": obj.get("name", ""),
                            "status": obj.get("status", "Активна" if obj.get("name") else ""),
                            "registration_date": str(obj.get("registerDate", "")),
                            "activity": obj.get("okedName", ""),
                            "address": obj.get("katoAddress", ""),
                        })
    except Exception as e:
        logger.warning("stat.gov.kz API failed for BIN %s: %s", bin_number, e)

    # Если не нашли через stat.gov, формируем AI-справку
    if not result["found"]:
        try:
            from src.bot.utils.ai_client import get_orchestrator
            ai = get_orchestrator()
            info = await ai.call_with_fallback(
                f"Дай краткую справку о компании с БИН {bin_number} в Казахстане. "
                f"Если не можешь найти — объясни, как проверить через egov.kz и stat.gov.kz.",
                "Ты — юрист-аналитик. Отвечай кратко, на русском, в HTML для Telegram.",
                primary="openai", max_tokens=512, temperature=0.3,
            )
            result["info"] = info
            result["found"] = True
        except Exception as e:
            logger.warning("AI BIN check failed: %s", e)
            result["info"] = (
                f"Для проверки БИН {bin_number} используйте:\n"
                "• https://stat.gov.kz — Бюро национальной статистики\n"
                "• https://egov.kz — Портал электронного правительства\n"
                "• https://kgd.gov.kz — Комитет госдоходов"
            )

    return result


def format_bin_report(data: dict) -> str:
    """Форматирует отчёт по БИН для Telegram (HTML)."""
    if not data.get("found"):
        return (
            f"❌ <b>БИН {data['bin']}</b> не найден.\n\n"
            f"{data.get('error', data.get('info', 'Попробуйте проверить вручную.'))}"
        )

    lines = [f"🏢 <b>Справка по БИН {data['bin']}</b>\n"]

    if data.get("name"):
        lines.append(f"📋 <b>Наименование:</b> {data['name']}")
    if data.get("status"):
        lines.append(f"📊 <b>Статус:</b> {data['status']}")
    if data.get("registration_date"):
        lines.append(f"📅 <b>Регистрация:</b> {data['registration_date']}")
    if data.get("activity"):
        lines.append(f"🏭 <b>Вид деятельности:</b> {data['activity']}")
    if data.get("address"):
        lines.append(f"📍 <b>Адрес:</b> {data['address']}")
    if data.get("info"):
        lines.append(f"\n💡 {data['info']}")

    lines.append(
        "\n───────────────\n"
        "⚖️ <i>Данные получены из открытых источников. "
        "Для юридической проверки обратитесь в SOLIS Partners.</i>"
    )
    return "\n".join(lines)
