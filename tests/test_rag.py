"""Тесты RAG-модуля (Retrieval-Augmented Generation)."""

from unittest.mock import AsyncMock

import pytest

from src.bot.utils.rag import find_relevant_context, _tokenize, _score_entry


class TestTokenize:
    """Тесты токенизации."""

    def test_basic_tokenize(self):
        tokens = _tokenize("Увольнение сотрудника по аттестации")
        assert "увольнение" in tokens
        assert "сотрудника" in tokens
        assert "аттестации" in tokens

    def test_stop_words_removed(self):
        tokens = _tokenize("и в на с по для от из")
        assert len(tokens) == 0

    def test_short_words_removed(self):
        tokens = _tokenize("я он мы ок")
        assert len(tokens) == 0

    def test_empty_string(self):
        assert _tokenize("") == set()


class TestScoreEntry:
    """Тесты скоринга."""

    def test_matching_score(self):
        query_tokens = _tokenize("увольнение аттестация трудовой")
        score = _score_entry(query_tokens, "Процедура увольнение и аттестация работника по трудовой")
        assert score >= 1

    def test_no_match(self):
        query_tokens = _tokenize("налоговый вычет")
        score = _score_entry(query_tokens, "Регистрация ТОО")
        assert score == 0


class TestFindRelevantContext:
    """Тесты поиска релевантного контекста."""

    @pytest.mark.asyncio
    async def test_returns_context_when_matching(self):
        mock_google = AsyncMock()
        mock_google.get_data_room = AsyncMock(return_value=[
            {"title": "Аттестация сотрудников", "content": "Процедура увольнения через аттестацию", "category": "HR"},
            {"title": "Регистрация ТОО", "content": "Шаги для создания ТОО в Казахстане", "category": "Корпоративное"},
        ])
        mock_google.get_articles_list = AsyncMock(return_value=[])

        from src.bot.utils.cache import TTLCache
        cache = TTLCache(ttl_seconds=0)

        result = await find_relevant_context("аттестация увольнение", mock_google, cache)
        assert "Аттестация" in result

    @pytest.mark.asyncio
    async def test_empty_when_no_match(self):
        mock_google = AsyncMock()
        mock_google.get_data_room = AsyncMock(return_value=[
            {"title": "Бухгалтерия", "content": "Финансовые отчёты", "category": "Финансы"},
        ])
        mock_google.get_articles_list = AsyncMock(return_value=[])

        from src.bot.utils.cache import TTLCache
        cache = TTLCache(ttl_seconds=0)

        result = await find_relevant_context("криптовалюта блокчейн", mock_google, cache)
        assert result == ""

    @pytest.mark.asyncio
    async def test_handles_google_error(self):
        mock_google = AsyncMock()
        mock_google.get_data_room = AsyncMock(side_effect=RuntimeError("API error"))
        mock_google.get_articles_list = AsyncMock(side_effect=RuntimeError("API error"))

        from src.bot.utils.cache import TTLCache
        cache = TTLCache(ttl_seconds=0)

        # Не должен упасть
        result = await find_relevant_context("тест", mock_google, cache)
        assert result == ""
