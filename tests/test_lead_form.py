"""Тесты валидации email и формы сбора лидов."""

import pytest

from src.bot.handlers.lead_form import EMAIL_REGEX


class TestEmailValidation:
    """Тесты валидации email."""

    @pytest.mark.parametrize(
        "email",
        [
            "user@example.com",
            "user.name@domain.ru",
            "user+tag@domain.co.uk",
            "test123@gmail.com",
        ],
    )
    def test_valid_emails(self, email: str):
        """Корректные email проходят валидацию."""
        assert EMAIL_REGEX.match(email) is not None

    @pytest.mark.parametrize(
        "email",
        [
            "not-an-email",
            "@domain.com",
            "user@",
            "user@.com",
            "",
            "user @domain.com",
            "user@domain",
        ],
    )
    def test_invalid_emails(self, email: str):
        """Некорректные email не проходят валидацию."""
        assert EMAIL_REGEX.match(email) is None
