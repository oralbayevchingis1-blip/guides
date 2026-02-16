"""ИИ-ассистент юриста — предварительные ответы на вопросы пользователей.

Цепочка: OpenAI (gpt-4o-mini) → Gemini (gemini-2.0-flash) → fallback-текст.
Ответ содержит disclaimer, что это не юридическая консультация.
"""

import logging
from typing import Optional

from src.config import settings

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
Ты — ИИ-помощник юридической фирмы SOLIS Partners (Казахстан).

Твоя задача — дать краткий, полезный предварительный ответ на юридический вопрос пользователя.

Правила:
1. Отвечай на русском языке, кратко (3-7 предложений).
2. Фокусируйся на казахстанском праве (ГК РК, НК РК, ТК РК, закон об ИТ, МФЦА/AIFC и др.).
3. Если вопрос выходит за рамки права Казахстана — укажи это.
4. Давай практичные рекомендации, ссылайся на конкретные нормативные акты, если уместно.
5. НЕ давай категоричных советов типа "вам точно нужно...". Используй формулировки "рекомендуется", "как правило", "стоит обратить внимание".
6. В конце ответа НЕ добавляй disclaimer — он будет добавлен отдельно.
7. Если вопрос не юридический — вежливо объясни, что ты специализируешься на правовых вопросах.
8. Не используй markdown-разметку, отвечай простым текстом.
"""

_DISCLAIMER = (
    "\n\n<i>Это предварительный ответ ИИ-ассистента. "
    "Он не является юридической консультацией. "
    "Наш юрист также рассмотрит ваш вопрос и ответит лично.</i>"
)

_FALLBACK_TEXT = (
    "К сожалению, ИИ-ассистент сейчас недоступен. "
    "Ваш вопрос принят — наш юрист ответит в ближайшее время."
)


async def _ask_openai(question: str) -> Optional[str]:
    """Запрос к OpenAI GPT-4o-mini."""
    if not settings.OPENAI_API_KEY:
        return None

    try:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": question},
            ],
            max_tokens=600,
            temperature=0.3,
        )
        text = response.choices[0].message.content
        if text:
            return text.strip()
        return None
    except Exception as e:
        logger.warning("OpenAI request failed: %s", e)
        return None


async def _ask_gemini(question: str) -> Optional[str]:
    """Запрос к Google Gemini (fallback)."""
    if not settings.GEMINI_API_KEY:
        return None

    try:
        import google.generativeai as genai

        genai.configure(api_key=settings.GEMINI_API_KEY)
        model = genai.GenerativeModel(
            "gemini-2.0-flash",
            system_instruction=_SYSTEM_PROMPT,
        )
        response = await model.generate_content_async(
            question,
            generation_config=genai.GenerationConfig(
                max_output_tokens=600,
                temperature=0.3,
            ),
        )
        text = response.text
        if text:
            return text.strip()
        return None
    except Exception as e:
        logger.warning("Gemini request failed: %s", e)
        return None


async def get_ai_answer(question: str) -> str:
    """Получает ответ от ИИ (OpenAI → Gemini → fallback).

    Возвращает HTML-форматированный ответ с disclaimer.
    """
    if not settings.AI_ENABLED:
        return _FALLBACK_TEXT

    answer = await _ask_openai(question)
    source = "OpenAI"

    if not answer:
        answer = await _ask_gemini(question)
        source = "Gemini"

    if not answer:
        logger.warning("All AI providers failed for question: %s...", question[:50])
        return _FALLBACK_TEXT

    logger.info("AI answer generated via %s (%d chars)", source, len(answer))

    escaped = (
        answer
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return f"🔹 <b>Предварительный ответ ИИ-ассистента:</b>\n\n{escaped}{_DISCLAIMER}"
