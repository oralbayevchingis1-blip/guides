"""Voice-to-Text — голосовые сообщения для AI-консультаций.

Пользователь отправляет голосовое → Whisper API → текст → ask_legal.
Опциональный TTS-ответ обратно голосом.

Работает в состоянии ConsultStates.waiting_for_question
и вне состояния (если пользователь отправляет голос без /consult).
"""

import logging
import os
import tempfile

from aiogram import Bot, F, Router
from aiogram.enums import ChatAction
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from src.bot.utils.cache import TTLCache
from src.bot.utils.google_sheets import GoogleSheetsClient
from src.config import settings

router = Router()
logger = logging.getLogger(__name__)

# Директория для временных файлов
TEMP_DIR = os.path.join("data", "temp")
os.makedirs(TEMP_DIR, exist_ok=True)


async def transcribe_voice(bot: Bot, file_id: str) -> str:
    """Скачивает голосовое и транскрибирует через OpenAI Whisper.

    Args:
        bot: Экземпляр бота.
        file_id: Telegram file_id голосового.

    Returns:
        Текст транскрипции.
    """
    import aiohttp

    # Скачиваем файл
    file = await bot.get_file(file_id)
    file_path = file.file_path

    local_path = os.path.join(TEMP_DIR, f"{file_id}.oga")

    try:
        await bot.download_file(file_path, local_path)

        # Транскрибация через OpenAI Whisper API
        url = "https://api.openai.com/v1/audio/transcriptions"
        headers = {"Authorization": f"Bearer {settings.OPENAI_API_KEY}"}

        async with aiohttp.ClientSession() as session:
            with open(local_path, "rb") as audio_file:
                form = aiohttp.FormData()
                form.add_field("file", audio_file, filename="voice.oga", content_type="audio/ogg")
                form.add_field("model", "whisper-1")
                form.add_field("language", "ru")

                async with session.post(url, headers=headers, data=form, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error("Whisper API error %d: %s", resp.status, body[:200])
                        raise RuntimeError(f"Whisper API: {resp.status}")

                    result = await resp.json()
                    text = result.get("text", "").strip()

        logger.info("Transcribed voice %s: '%s...'", file_id[:10], text[:50])
        return text

    finally:
        # Удаляем временный файл
        try:
            os.remove(local_path)
        except OSError:
            pass


@router.message(F.voice)
async def handle_voice(
    message: Message,
    bot: Bot,
    state: FSMContext,
    google: GoogleSheetsClient,
    cache: TTLCache,
) -> None:
    """Обработка голосовых сообщений — транскрибация + AI-консультация."""
    if message.from_user is None:
        return

    if not settings.OPENAI_API_KEY:
        await message.answer(
            "🎤 Голосовые сообщения временно недоступны.\n"
            "Напишите ваш вопрос текстом: /consult"
        )
        return

    # Показываем что бот «печатает»
    await bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    try:
        # Транскрибируем
        text = await transcribe_voice(bot, message.voice.file_id)

        if not text or len(text) < 3:
            await message.answer(
                "🎤 Не удалось распознать речь. "
                "Попробуйте записать голосовое ещё раз или напишите текстом."
            )
            return

        # Показываем пользователю распознанный текст
        await message.answer(
            f"🎤 <i>Распознано:</i>\n<blockquote>{text}</blockquote>\n\n"
            f"🔍 Анализирую вопрос..."
        )

        # Обрабатываем как обычный текстовый вопрос
        from src.bot.handlers.consult import process_question

        # Создаём фейковое сообщение с текстом для обработки
        message.text = text
        await process_question(message, state, google, cache)

    except Exception as e:
        logger.error("Voice processing error: %s", e)
        await message.answer(
            "❌ Ошибка обработки голосового сообщения.\n\n"
            "Попробуйте написать вопрос текстом: /consult"
        )
