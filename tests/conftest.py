"""Общие фикстуры для тестов SOLIS Partners Bot."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio


# ── Mock Bot ─────────────────────────────────────────────────────────────

@pytest.fixture
def mock_bot():
    """Mock aiogram Bot."""
    bot = AsyncMock()
    bot.send_message = AsyncMock(return_value=MagicMock(message_id=1))
    bot.delete_message = AsyncMock()
    bot.edit_message_text = AsyncMock()
    return bot


# ── Mock Google Sheets ───────────────────────────────────────────────────

@pytest.fixture
def mock_google():
    """Mock GoogleSheetsClient с предзаданными данными."""
    google = AsyncMock()

    # Каталог гайдов
    google.get_guides_catalog = AsyncMock(return_value=[
        {
            "id": "guide-test",
            "title": "Тестовый гайд",
            "description": "Описание тестового гайда",
            "file_path": "guides/test.pdf",
        },
    ])

    # Тексты бота
    google.get_bot_texts = AsyncMock(return_value={
        "welcome": "Добро пожаловать!",
        "choose_guide": "Выберите гайд:",
        "ask_email": "Введите email:",
        "invalid_email": "Некорректный email. Попробуйте снова:",
        "ask_name": "Введите имя:",
        "consent_prompt": "Согласны ли вы на обработку данных?",
        "success": "Спасибо! Гайд отправлен.",
    })

    # Пустые списки по умолчанию
    google.get_data_room = AsyncMock(return_value=[])
    google.get_articles_list = AsyncMock(return_value=[])
    google.get_recent_leads = AsyncMock(return_value=[])
    google.get_recent_news = AsyncMock(return_value=[])

    # Запись
    google.append_lead = AsyncMock()
    google.append_article = AsyncMock()
    google.log_consult = AsyncMock()

    return google


# ── Mock AI Orchestrator ────────────────────────────────────────────────

@pytest.fixture
def mock_ai():
    """Mock AIOrchestrator с фиксированными ответами."""
    ai = AsyncMock()
    ai.call_gemini = AsyncMock(return_value="Тестовый юридический ответ от Gemini.")
    ai.call_openai = AsyncMock(return_value="Тестовый маркетинговый ответ от GPT.")
    ai.call_with_fallback = AsyncMock(return_value="Тестовый ответ с fallback.")
    ai.close = AsyncMock()
    ai.total_tokens_used = 0
    return ai


# ── Mock FSM Context ────────────────────────────────────────────────────

@pytest.fixture
def mock_state():
    """Mock FSMContext."""
    state = AsyncMock()
    state.get_data = AsyncMock(return_value={})
    state.update_data = AsyncMock()
    state.set_state = AsyncMock()
    state.clear = AsyncMock()
    return state


# ── Mock TTLCache ────────────────────────────────────────────────────────

@pytest.fixture
def mock_cache():
    """Mock TTLCache."""
    from src.bot.utils.cache import TTLCache
    return TTLCache(ttl_seconds=3600)


# ── Mock Message ─────────────────────────────────────────────────────────

@pytest.fixture
def mock_message():
    """Mock aiogram Message."""
    msg = AsyncMock()
    msg.from_user = MagicMock()
    msg.from_user.id = 12345
    msg.from_user.username = "testuser"
    msg.from_user.full_name = "Test User"
    msg.text = ""
    msg.answer = AsyncMock(return_value=MagicMock(message_id=2))
    msg.reply = AsyncMock()
    msg.chat = MagicMock()
    msg.chat.id = 12345
    return msg
