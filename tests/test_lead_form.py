"""Тесты валидации email и формы сбора лидов."""

import re

import pytest

# Regex из lead_form.py
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


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
