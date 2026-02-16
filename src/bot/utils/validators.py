"""GENNADY TECH_DIRECTOR_V4.2 — P3 + Stage 15: Validation & Production Hardening.

Единообразная проверка:
- LeadValidator / HardenedLeadValidator: email, имя, guide_id + XSS/SQL фильтр
- ArticleValidator: title, content, category
- ConfigValidator: settings sanity check
- BIN Validator: 12-значный БИН РК с контрольной суммой

Stage 15 — Production Hardening:
- sanitize_input(): удаление HTML/script-тегов из пользовательского ввода
- XSS/SQL injection detection: блокировка опасных паттернов
- validate_bizdev_input(): проверка тем и вопросов для BizDev-пайплайна

Включает проверку «мусорности» текста (asdfgh, 111111)
и утилиты маскировки PII для логов.
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
#  Stage 15: INJECTION DETECTION & INPUT SANITIZATION
# ═══════════════════════════════════════════════════════════════════════════

# Паттерны для блокировки опасного контента
XSS_PATTERN = re.compile(
    r'<script|javascript:|on\w+\s*=|eval\s*\(|document\.|window\.',
    re.IGNORECASE,
)
SQL_PATTERN = re.compile(
    r'\b(SELECT\s+.+\s+FROM|INSERT\s+INTO|UPDATE\s+.+\s+SET|'
    r'DELETE\s+FROM|DROP\s+(TABLE|DATABASE)|UNION\s+(ALL\s+)?SELECT|'
    r';\s*(DROP|DELETE|UPDATE|INSERT))\b',
    re.IGNORECASE,
)


def contains_injection(text: str) -> bool:
    """Проверяет текст на XSS/SQL-инъекции.

    Returns:
        True если найден опасный паттерн.
    """
    if not text:
        return False
    return bool(XSS_PATTERN.search(text) or SQL_PATTERN.search(text))


def sanitize_input(text: str) -> str:
    """Очищает текст от потенциально опасных конструкций.

    - Удаляет HTML-теги (кроме безопасных: b, i, em, strong, code)
    - Удаляет script/style блоки целиком
    - Нормализует пробелы

    Returns:
        Очищенный текст.
    """
    if not text:
        return ""
    # Удаляем script/style блоки целиком
    clean = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', text, flags=re.IGNORECASE | re.DOTALL)
    # Удаляем все HTML-теги
    clean = re.sub(r'<[^>]*?>', '', clean)
    # Нормализуем пробелы
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean


def validate_bizdev_input(text: str, max_len: int = 5000) -> tuple[bool, str]:
    """Проверяет входящую тему или вопрос на валидность для BizDev-пайплайна.

    Проверки:
    1. Длина (10-max_len символов)
    2. Мусорный текст (менее 3 уникальных символов)
    3. XSS/SQL-инъекции
    4. Чисто числовой ввод

    Returns:
        (is_valid, error_message) — пустая строка если валидно.
    """
    if not text or not text.strip():
        return False, "Текст не может быть пустым."
    cleaned = text.strip()
    if len(cleaned) < 10:
        return False, "Слишком коротко — минимум 10 символов."
    if len(cleaned) > max_len:
        return False, f"Слишком длинно — максимум {max_len} символов."
    # Мусор: менее 3 уникальных символов при длине >10
    if len(set(cleaned.replace(" ", ""))) < 3 and len(cleaned) > 10:
        return False, "Текст выглядит как мусорный ввод."
    # Инъекции
    if contains_injection(cleaned):
        logger.warning("BizDev input injection attempt: %s", cleaned[:100])
        return False, "Текст содержит запрещённые конструкции."
    # Чисто числовой
    if cleaned.replace(" ", "").replace(".", "").isdigit():
        return False, "Введите текст, а не только числа."
    return True, ""


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
        # Stage 15: injection check
        if contains_injection(v):
            logger.warning("Injection attempt in lead email: %s", v[:50])
            raise ValueError("Email содержит запрещённые символы")
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
        # Stage 15: XSS/SQL injection check
        if contains_injection(v):
            logger.warning("Injection attempt in lead name: %s", v[:50])
            raise ValueError("Имя содержит запрещённые символы")
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


# ═══════════════════════════════════════════════════════════════════════════
#  BIN VALIDATOR (12-значный БИН РК)
# ═══════════════════════════════════════════════════════════════════════════

_BIN_PATTERN = re.compile(r'^\d{12}$')

# Веса для проверки контрольной суммы БИН
_BIN_WEIGHTS_1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
_BIN_WEIGHTS_2 = [3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2]


def is_valid_bin(bin_str: str) -> bool:
    """Проверка БИН/ИИН РК (12 цифр + алгоритм контрольной суммы).

    Алгоритм:
    1. Вычисляем сумму с весами [1..11].
    2. Если остаток от деления на 11 == 10 — повторяем с весами [3..2].
    3. Контрольная цифра (12-я) = остаток.

    Returns:
        True если БИН/ИИН валиден.
    """
    cleaned = bin_str.strip()
    if not _BIN_PATTERN.match(cleaned):
        return False

    digits = [int(c) for c in cleaned]
    check_digit = digits[11]

    # 1-я итерация
    total = sum(d * w for d, w in zip(digits[:11], _BIN_WEIGHTS_1))
    remainder = total % 11

    if remainder == 10:
        # 2-я итерация с другими весами
        total = sum(d * w for d, w in zip(digits[:11], _BIN_WEIGHTS_2))
        remainder = total % 11
        if remainder == 10:
            return False  # Невалидный БИН

    return remainder == check_digit


# ═══════════════════════════════════════════════════════════════════════════
#  PII MASKING (маскировка персональных данных в логах)
# ═══════════════════════════════════════════════════════════════════════════

# Паттерны для PII
_PHONE_PATTERN = re.compile(
    r'(\+?\d{1,3}[\s\-]?)?'          # код страны
    r'(\(?\d{2,4}\)?[\s\-]?)?'       # код оператора
    r'\d{3}[\s\-]?\d{2}[\s\-]?\d{2}' # номер
)
_EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
)
_BIN_LOG_PATTERN = re.compile(r'\b\d{12}\b')


def mask_pii(text: str) -> str:
    """Маскирует телефоны, email и БИН/ИИН в текстовых логах.

    - +7 701 123 45 67 → +7 701 ***-**-67
    - user@mail.com → u***@mail.com
    - 123456789012 → 1234****9012

    Returns:
        Текст с замаскированными PII.
    """
    # Маскируем email: оставляем 1-ю букву + домен
    def _mask_email(match: re.Match) -> str:
        email = match.group()
        local, domain = email.split("@", 1)
        return f"{local[0]}***@{domain}"

    # Маскируем телефон: оставляем последние 2 цифры
    def _mask_phone(match: re.Match) -> str:
        phone = match.group()
        digits = re.sub(r'\D', '', phone)
        if len(digits) < 7:
            return phone
        return phone[:-4] + "**" + phone[-2:]

    # Маскируем БИН/ИИН: оставляем первые 4 и последние 4
    def _mask_bin(match: re.Match) -> str:
        num = match.group()
        return f"{num[:4]}****{num[-4:]}"

    result = _EMAIL_PATTERN.sub(_mask_email, text)
    result = _PHONE_PATTERN.sub(_mask_phone, result)
    result = _BIN_LOG_PATTERN.sub(_mask_bin, result)
    return result


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
