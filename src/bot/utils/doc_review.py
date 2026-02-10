"""L1. AI DocReview — анализ юридических рисков загруженных документов.

Поддерживает .pdf и .docx. Извлекает текст, передаёт AI для поиска
кабальных условий, отсутствия ограничений ответственности, сомнительных сроков.

Использование:
    from src.bot.utils.doc_review import extract_text, analyze_legal_document
    text = await extract_text(filepath)
    review = await analyze_legal_document(text)
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

TEMP_DIR = Path("data/temp")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

MAX_DOC_LENGTH = 15000  # Лимит символов для AI


async def extract_text_from_pdf(filepath: str) -> str:
    """Извлекает текст из PDF (PyPDF2 или fallback)."""
    try:
        import PyPDF2
        text = ""
        with open(filepath, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages[:50]:  # Макс 50 страниц
                text += (page.extract_text() or "") + "\n"
        return text.strip()
    except ImportError:
        logger.info("PyPDF2 not installed — trying pdfminer")
    except Exception as e:
        logger.warning("PyPDF2 failed: %s", e)

    # Fallback: pdfminer
    try:
        from pdfminer.high_level import extract_text
        import asyncio
        text = await asyncio.to_thread(extract_text, filepath)
        return text.strip()
    except ImportError:
        logger.warning("No PDF library available (PyPDF2 or pdfminer)")
    except Exception as e:
        logger.warning("pdfminer failed: %s", e)

    return ""


async def extract_text_from_docx(filepath: str) -> str:
    """Извлекает текст из .docx."""
    try:
        import docx
        import asyncio

        def _read():
            doc = docx.Document(filepath)
            return "\n".join(p.text for p in doc.paragraphs if p.text.strip())

        return await asyncio.to_thread(_read)
    except ImportError:
        logger.warning("python-docx not installed")
    except Exception as e:
        logger.warning("docx extraction failed: %s", e)
    return ""


async def extract_text(filepath: str) -> str:
    """Универсальный экстрактор текста по расширению."""
    ext = Path(filepath).suffix.lower()
    if ext == ".pdf":
        return await extract_text_from_pdf(filepath)
    elif ext in (".docx", ".doc"):
        return await extract_text_from_docx(filepath)
    elif ext == ".txt":
        try:
            return Path(filepath).read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return ""
    else:
        return ""


async def analyze_legal_document(text: str, user_question: str = "") -> str:
    """AI-анализ юридического документа на риски.

    Args:
        text: Извлечённый текст документа.
        user_question: Опциональный вопрос пользователя.

    Returns:
        HTML-форматированный отчёт о рисках.
    """
    from src.bot.utils.ai_client import get_orchestrator

    # Ограничиваем длину
    doc_text = text[:MAX_DOC_LENGTH]
    if len(text) > MAX_DOC_LENGTH:
        doc_text += "\n\n[...документ обрезан, показаны первые 15000 символов...]"

    prompt = (
        "Проанализируй этот текст договора/документа.\n\n"
        "ЗАДАЧА:\n"
        "1. Найди 3-7 критических рисков для клиента (казахстанское право)\n"
        "2. Проверь на кабальные условия\n"
        "3. Оцени ограничения ответственности\n"
        "4. Проверь сроки и штрафные санкции\n"
        "5. Найди нечёткие или двусмысленные формулировки\n\n"
        "ФОРМАТ ОТВЕТА (HTML для Telegram):\n"
        "Каждый риск:\n"
        "🔴/🟡/🟢 <b>НАЗВАНИЕ РИСКА</b>\n"
        "📋 Пункт: [какой раздел/пункт]\n"
        "⚠️ Опасность: [почему это плохо]\n"
        "✅ Рекомендация: [как исправить]\n\n"
        "В конце: общий рейтинг документа (🔴 опасно / 🟡 требует доработки / 🟢 приемлемо)\n\n"
    )

    if user_question:
        prompt += f"ДОПОЛНИТЕЛЬНЫЙ ВОПРОС КЛИЕНТА: {user_question}\n\n"

    prompt += f"ТЕКСТ ДОКУМЕНТА:\n{doc_text}"

    instruction = (
        "Ты — Senior Lawyer в SOLIS Partners (Алматы, Казахстан). "
        "Анализируй документы на риски по казахстанскому праву, праву МФЦА. "
        "Будь конкретным: указывай номера пунктов, суммы, сроки. "
        "Используй HTML-теги Telegram: <b>, <i>, <code>. "
        "НЕ используй Markdown."
    )

    ai = get_orchestrator()
    return await ai.call_with_fallback(
        prompt, instruction,
        primary="openai", max_tokens=2048, temperature=0.3,
    )


async def quick_doc_summary(text: str) -> str:
    """Быстрое резюме документа (1-2 абзаца)."""
    from src.bot.utils.ai_client import get_orchestrator

    doc_text = text[:8000]
    ai = get_orchestrator()
    return await ai.call_with_fallback(
        f"Дай краткое резюме этого юридического документа (2-3 предложения):\n\n{doc_text}",
        "Ты — юрист. Пиши кратко на русском. Используй HTML-теги Telegram.",
        primary="openai", max_tokens=512, temperature=0.3,
    )
