"""Клиент Gemini AI для юридических мини-консультаций."""

import asyncio
import json
import logging
from urllib.request import Request, urlopen

from src.config import settings

logger = logging.getLogger(__name__)

# Контекст SOLIS Partners для Gemini
SOLIS_CONTEXT = """Ты — AI-ассистент юридической фирмы SOLIS Partners (Казахстан, Алматы).
Специализация фирмы: IT-право, корпоративное право, M&A, судебные споры, МФЦА (английское право).

Правила ответа:
1. Отвечай кратко (3-5 абзацев), структурировано, по существу.
2. Ссылайся на конкретные статьи законов РК если применимо.
3. Используй markdown-форматирование (жирный, курсив, списки).
4. В конце ВСЕГДА рекомендуй обратиться к специалистам SOLIS Partners для детальной проработки вопроса.
5. НЕ давай конкретных правовых заключений — только общую ориентировку.
6. Если вопрос не юридический — вежливо объясни что специализируешься на праве.
7. Отвечай на языке вопроса (русский или казахский).
"""

GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"


def _sync_ask_gemini(
    question: str,
    *,
    system_prompt: str | None = None,
    max_tokens: int = 1024,
    temperature: float = 0.7,
) -> str:
    """Синхронный запрос к Gemini API.

    Args:
        question: Текст запроса.
        system_prompt: Системный промпт (по умолчанию SOLIS_CONTEXT).
        max_tokens: Лимит выходных токенов.
        temperature: Температура генерации.

    Returns:
        Ответ от Gemini.

    Raises:
        RuntimeError: Если API недоступен или ключ не настроен.
    """
    api_key = settings.GEMINI_API_KEY
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY не настроен в .env")

    url = f"{GEMINI_API_URL}?key={api_key}"

    # Если системный промпт не указан, используем SOLIS_CONTEXT
    if system_prompt is None:
        prompt_text = f"{SOLIS_CONTEXT}\n\nВопрос клиента:\n{question}"
    else:
        prompt_text = question  # Весь промпт уже в question

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt_text}
                ]
            }
        ],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_tokens,
            "topP": 0.9,
        },
        "safetySettings": [
            {
                "category": "HARM_CATEGORY_HARASSMENT",
                "threshold": "BLOCK_ONLY_HIGH",
            },
            {
                "category": "HARM_CATEGORY_HATE_SPEECH",
                "threshold": "BLOCK_ONLY_HIGH",
            },
            {
                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "threshold": "BLOCK_ONLY_HIGH",
            },
            {
                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                "threshold": "BLOCK_ONLY_HIGH",
            },
        ],
    }

    data = json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")

    try:
        with urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.error("Gemini API error: %s", e)
        raise RuntimeError(f"Ошибка запроса к Gemini: {e}") from e

    # Извлекаем текст ответа
    try:
        candidates = result.get("candidates", [])
        if not candidates:
            raise RuntimeError("Gemini вернул пустой ответ")

        parts = candidates[0].get("content", {}).get("parts", [])
        if not parts:
            raise RuntimeError("Gemini вернул ответ без текста")

        answer = parts[0].get("text", "").strip()
        if not answer:
            raise RuntimeError("Пустой текст в ответе Gemini")

        return answer

    except (KeyError, IndexError) as e:
        logger.error("Ошибка парсинга ответа Gemini: %s | raw=%s", e, result)
        raise RuntimeError("Не удалось обработать ответ AI") from e


async def ask_gemini(
    question: str,
    *,
    system_prompt: str | None = None,
    max_tokens: int = 1024,
    temperature: float = 0.7,
) -> str:
    """Асинхронный запрос к Gemini AI.

    Args:
        question: Текст запроса.
        system_prompt: Кастомный системный промпт (None = SOLIS_CONTEXT).
        max_tokens: Лимит выходных токенов.
        temperature: Температура генерации.

    Returns:
        Ответ от Gemini.
    """
    return await asyncio.to_thread(
        _sync_ask_gemini,
        question,
        system_prompt=system_prompt,
        max_tokens=max_tokens,
        temperature=temperature,
    )
