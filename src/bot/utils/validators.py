"""P3. Pydantic-валидация входящих данных — лиды, статьи, конфиги.

Единообразная проверка:
- LeadValidator: email, имя, guide_id
- ArticleValidator: title, content, category
- ConfigValidator: settings sanity check

Включает проверку «мусорности» текста (asdfgh, 111111).
"""

import re
import logging
from typing import Optional

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

# Паттерны мусорного текста
_GARBAGE_PATTERNS = [
    re.compile(r'^[a-z]{1,3}$', re.IGNORECASE),                    # 'a', 'ab'
    re.compile(r'^(.)\1{3,}$'),                                      # 'aaaa', '1111'
    re.compile(r'^[qwerty]{5,}$', re.IGNORECASE),                   # 'qwerty'
    re.compile(r'^[asdfgh]{5,}$', re.IGNORECASE),                   # 'asdfgh'
    re.compile(r'^[zxcvbn]{5,}$', re.IGNORECASE),                   # 'zxcvbn'
    re.compile(r'^test\s*\d*$', re.IGNORECASE),                     # 'test', 'test123'
    re.compile(r'^[!@#$%^&*()_+\-=\[\]{};:\'",.<>?/\\|`~\s]+$'),  # Только символы
]


def is_garbage_text(text: str) -> bool:
    """Проверяет, является ли текст «мусорным»."""
    cleaned = text.strip()
    if len(cleaned) < 2:
        return True
    for pattern in _GARBAGE_PATTERNS:
        if pattern.match(cleaned):
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════
#  LEAD VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════


class LeadValidator(BaseModel):
    """Валидация данных лида."""

    name: str = Field(..., min_length=2, max_length=100)
    email: str = Field(..., min_length=5, max_length=255)
    guide_id: str = Field(default="", max_length=100)
    phone: str = Field(default="", max_length=20)

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip().lower()
        pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        if not pattern.match(v):
            raise ValueError("Некорректный email")
        # Проверяем на одноразовые домены
        disposable = {"mailinator.com", "guerrillamail.com", "tempmail.com",
                       "throwaway.email", "yopmail.com", "10minutemail.com"}
        domain = v.split("@")[1]
        if domain in disposable:
            raise ValueError("Одноразовый email не принимается")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if is_garbage_text(v):
            raise ValueError("Пожалуйста, введите настоящее имя")
        # Имя не должно содержать только цифры
        if v.replace(" ", "").isdigit():
            raise ValueError("Имя не может состоять только из цифр")
        return v


def validate_lead(name: str, email: str, guide_id: str = "") -> tuple[bool, str]:
    """Валидирует данные лида.

    Returns:
        (is_valid, error_message)
    """
    try:
        LeadValidator(name=name, email=email, guide_id=guide_id)
        return True, ""
    except Exception as e:
        errors = str(e)
        # Извлекаем первую ошибку для пользователя
        if "email" in errors.lower():
            return False, "Некорректный email. Попробуйте другой."
        if "имя" in errors.lower() or "name" in errors.lower():
            return False, "Пожалуйста, введите настоящее имя (минимум 2 символа)."
        return False, "Ошибка валидации данных."


# ═══════════════════════════════════════════════════════════════════════════
#  ARTICLE VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════


class ArticleValidator(BaseModel):
    """Валидация данных статьи."""

    title: str = Field(..., min_length=5, max_length=500)
    content: str = Field(..., min_length=50)
    category: str = Field(default="general", max_length=50)
    description: str = Field(default="", max_length=1000)

    @field_validator("title")
    @classmethod
    def validate_title(cls, v: str) -> str:
        v = v.strip()
        if is_garbage_text(v):
            raise ValueError("Заголовок статьи некорректен")
        return v

    @field_validator("content")
    @classmethod
    def validate_content(cls, v: str) -> str:
        if len(v.strip()) < 50:
            raise ValueError("Содержание статьи слишком короткое (мин. 50 символов)")
        return v.strip()


def validate_article(title: str, content: str, **kwargs) -> tuple[bool, str]:
    """Валидирует данные статьи."""
    try:
        ArticleValidator(title=title, content=content, **kwargs)
        return True, ""
    except Exception as e:
        return False, str(e).split("\n")[0][:200]


# ═══════════════════════════════════════════════════════════════════════════
#  URL VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════


_URL_PATTERN = re.compile(
    r'^https?://'
    r'(?:[a-zA-Z0-9\-._~:/?#\[\]@!$&\'()*+,;=%])+$'
)


def is_valid_url(url: str) -> bool:
    """Проверяет корректность URL."""
    return bool(_URL_PATTERN.match(url.strip()))


# ═══════════════════════════════════════════════════════════════════════════
#  CONFIG SANITY CHECK
# ═══════════════════════════════════════════════════════════════════════════


def check_config_sanity() -> list[str]:
    """Проверяет конфигурацию на корректность.

    Returns:
        Список предупреждений (пустой = всё ок).
    """
    from src.config import settings

    warnings = []

    if not settings.BOT_TOKEN or len(settings.BOT_TOKEN) < 30:
        warnings.append("BOT_TOKEN не задан или слишком короткий")

    if not settings.CHANNEL_USERNAME:
        warnings.append("CHANNEL_USERNAME не задан")
    elif not settings.CHANNEL_USERNAME.startswith("@"):
        warnings.append("CHANNEL_USERNAME должен начинаться с @")

    if settings.ADMIN_ID == 0:
        warnings.append("ADMIN_ID не задан")

    if not settings.GEMINI_API_KEY and not settings.OPENAI_API_KEY:
        warnings.append("Ни один AI API ключ не настроен")

    if not settings.GOOGLE_SPREADSHEET_ID:
        warnings.append("GOOGLE_SPREADSHEET_ID не задан — Google Sheets не работает")

    return warnings
