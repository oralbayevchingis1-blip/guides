"""Тесты AI-клиента: AIOrchestrator, retry, fallback."""

from unittest.mock import AsyncMock, patch, MagicMock

import pytest
import pytest_asyncio

from src.bot.utils.ai_client import (
    AIOrchestrator,
    ask_legal,
    ask_marketing,
    ask_content,
    ask_digest,
    get_orchestrator,
    LEGAL_PERSONA,
    MARKETING_PERSONA,
)


class TestAIOrchestrator:
    """Тесты класса AIOrchestrator."""

    @pytest.mark.asyncio
    async def test_call_gemini_extracts_text(self):
        """call_gemini корректно парсит ответ."""
        ai = AIOrchestrator()

        mock_response = {
            "candidates": [
                {"content": {"parts": [{"text": "Ответ от Gemini"}]}}
            ],
            "usageMetadata": {"totalTokenCount": 100},
        }

        with patch.object(ai, "_request_with_retry", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = mock_response
            result = await ai.call_gemini("тест", "system")
            assert result == "Ответ от Gemini"
            assert ai.total_tokens_used == 100

        await ai.close()

    @pytest.mark.asyncio
    async def test_call_openai_extracts_text(self):
        """call_openai корректно парсит ответ."""
        ai = AIOrchestrator()

        mock_response = {
            "choices": [
                {"message": {"content": "Ответ от GPT"}}
            ],
            "usage": {"total_tokens": 200},
        }

        with patch.object(ai, "_request_with_retry", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = mock_response
            result = await ai.call_openai("тест", "system")
            assert result == "Ответ от GPT"
            assert ai.total_tokens_used == 200

        await ai.close()

    @pytest.mark.asyncio
    async def test_fallback_on_openai_failure(self):
        """При ошибке GPT переключается на Gemini."""
        ai = AIOrchestrator()

        gemini_response = {
            "candidates": [
                {"content": {"parts": [{"text": "Fallback Gemini"}]}}
            ],
            "usageMetadata": {"totalTokenCount": 50},
        }

        with patch.object(ai, "call_openai", new_callable=AsyncMock) as mock_gpt:
            mock_gpt.side_effect = RuntimeError("GPT error")
            with patch.object(ai, "_request_with_retry", new_callable=AsyncMock) as mock_req:
                mock_req.return_value = gemini_response
                result = await ai.call_with_fallback("тест", "system", primary="openai")
                assert result == "Fallback Gemini"

        await ai.close()

    @pytest.mark.asyncio
    async def test_empty_candidates_raises(self):
        """Пустой ответ от Gemini вызывает RuntimeError."""
        ai = AIOrchestrator()

        with patch.object(ai, "_request_with_retry", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = {"candidates": []}
            with pytest.raises(RuntimeError, match="пустой ответ"):
                await ai.call_gemini("тест", "system")

        await ai.close()

    @pytest.mark.asyncio
    async def test_empty_choices_raises(self):
        """Пустой ответ от OpenAI вызывает RuntimeError."""
        ai = AIOrchestrator()

        with patch.object(ai, "_request_with_retry", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = {"choices": []}
            with pytest.raises(RuntimeError, match="пустой ответ"):
                await ai.call_openai("тест", "system")

        await ai.close()


class TestPublicFunctions:
    """Тесты публичных функций (ask_legal, ask_marketing, etc.)."""

    @pytest.mark.asyncio
    async def test_ask_legal_calls_gemini(self):
        """ask_legal вызывает Gemini."""
        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_get:
            mock_ai = AsyncMock()
            mock_ai.call_gemini = AsyncMock(return_value="Юрответ")
            mock_get.return_value = mock_ai

            result = await ask_legal("Вопрос")
            assert result == "Юрответ"
            mock_ai.call_gemini.assert_called_once()

    @pytest.mark.asyncio
    async def test_ask_legal_with_context(self):
        """ask_legal передаёт RAG-контекст."""
        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_get:
            mock_ai = AsyncMock()
            mock_ai.call_gemini = AsyncMock(return_value="Ответ")
            mock_get.return_value = mock_ai

            await ask_legal("Вопрос", context="Data Room info")
            args = mock_ai.call_gemini.call_args
            prompt = args[0][0]  # первый позиционный аргумент
            assert "Data Room info" in prompt

    @pytest.mark.asyncio
    async def test_ask_marketing_calls_with_fallback(self):
        """ask_marketing вызывает call_with_fallback."""
        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_get:
            mock_ai = AsyncMock()
            mock_ai.call_with_fallback = AsyncMock(return_value="Маркетинг")
            mock_get.return_value = mock_ai

            result = await ask_marketing("Контент-план")
            assert result == "Маркетинг"
            mock_ai.call_with_fallback.assert_called_once()

    @pytest.mark.asyncio
    async def test_ask_content_format_article(self):
        """ask_content для задачи format_article."""
        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_get:
            mock_ai = AsyncMock()
            mock_ai.call_with_fallback = AsyncMock(return_value='{"title":"Test"}')
            mock_get.return_value = mock_ai

            result = await ask_content("текст статьи", task="format_article")
            assert "Test" in result

    @pytest.mark.asyncio
    async def test_ask_digest_calls_with_fallback(self):
        """ask_digest вызывает call_with_fallback."""
        with patch("src.bot.utils.ai_client.get_orchestrator") as mock_get:
            mock_ai = AsyncMock()
            mock_ai.call_with_fallback = AsyncMock(return_value="Дайджест")
            mock_get.return_value = mock_ai

            result = await ask_digest("Утренний обзор")
            assert result == "Дайджест"
