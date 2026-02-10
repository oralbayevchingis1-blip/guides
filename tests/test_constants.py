"""Тесты get_text() — получение текста с fallback."""

from src.constants import FALLBACK_TEXTS, get_text


class TestGetText:
    """Тесты функции get_text()."""

    def test_returns_from_sheets_texts(self):
        """Возвращает текст из Google Sheets словаря."""
        texts = {"welcome_subscribed": "Custom welcome!"}
        result = get_text(texts, "welcome_subscribed")
        assert result == "Custom welcome!"

    def test_falls_back_to_local(self):
        """При отсутствии ключа в Sheets — fallback на локальные."""
        texts = {}
        result = get_text(texts, "welcome_subscribed")
        assert result == FALLBACK_TEXTS["welcome_subscribed"]

    def test_format_substitution(self):
        """Подстановка переменных через .format()."""
        texts = {"consent_given": "Спасибо, {name}! Email: {email}"}
        result = get_text(texts, "consent_given", name="Иван", email="ivan@test.com")
        assert result == "Спасибо, Иван! Email: ivan@test.com"

    def test_empty_key_returns_empty_string(self):
        """Неизвестный ключ без fallback — пустая строка."""
        texts = {}
        result = get_text(texts, "nonexistent_key_xyz")
        assert result == ""

    def test_format_with_missing_key_returns_raw(self):
        """Если kwargs не совпадают с плейсхолдерами — вернуть raw текст."""
        texts = {"consent_text": "URL: {privacy_url}"}
        result = get_text(texts, "consent_text", wrong_key="value")
        assert result == "URL: {privacy_url}"
