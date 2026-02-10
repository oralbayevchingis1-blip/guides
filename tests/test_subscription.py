"""Тесты проверки подписки на канал."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from aiogram.enums import ChatMemberStatus


@pytest.fixture(autouse=True)
def mock_settings(monkeypatch):
    """Подменяем настройки для тестов."""
    monkeypatch.setenv("BOT_TOKEN", "test_token")
    monkeypatch.setenv("CHANNEL_USERNAME", "@test_channel")
    monkeypatch.setenv("ADMIN_ID", "123456789")
    monkeypatch.setenv("GOOGLE_SPREADSHEET_ID", "test_spreadsheet_id")


@pytest.mark.asyncio
async def test_check_subscription_member(monkeypatch):
    """Пользователь-участник канала -> True."""
    from src.bot.utils.subscription_check import check_subscription

    bot = AsyncMock()
    member_mock = MagicMock()
    member_mock.status = ChatMemberStatus.MEMBER
    bot.get_chat_member.return_value = member_mock

    result = await check_subscription(123456, bot)
    assert result is True


@pytest.mark.asyncio
async def test_check_subscription_administrator(monkeypatch):
    """Администратор канала -> True."""
    from src.bot.utils.subscription_check import check_subscription

    bot = AsyncMock()
    member_mock = MagicMock()
    member_mock.status = ChatMemberStatus.ADMINISTRATOR
    bot.get_chat_member.return_value = member_mock

    result = await check_subscription(123456, bot)
    assert result is True


@pytest.mark.asyncio
async def test_check_subscription_not_member(monkeypatch):
    """Пользователь не в канале -> False."""
    from src.bot.utils.subscription_check import check_subscription

    bot = AsyncMock()
    member_mock = MagicMock()
    member_mock.status = ChatMemberStatus.LEFT
    bot.get_chat_member.return_value = member_mock

    result = await check_subscription(123456, bot)
    assert result is False


@pytest.mark.asyncio
async def test_check_subscription_api_error(monkeypatch):
    """Ошибка API -> False (graceful degradation)."""
    from src.bot.utils.subscription_check import check_subscription

    bot = AsyncMock()
    bot.get_chat_member.side_effect = Exception("API Error")

    result = await check_subscription(123456, bot)
    assert result is False
