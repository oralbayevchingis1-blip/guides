"""Мультиязычность (i18n) — RU / KZ / EN.

Определение языка:
1. Явная команда /lang ru|kz|en
2. Автодетекция по первому сообщению (AI)
3. Fallback: language_code из Telegram профиля

Тексты:
- Базовые ключи в TRANSLATIONS
- Google Sheets: колонки text_ru, text_kz, text_en

Использование:
    from src.bot.utils.i18n import get_user_lang, t, detect_language
    lang = get_user_lang(user_id)
    text = t("welcome_subscribed", lang)
"""

import logging
import re
from collections import defaultdict

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
#  SUPPORTED LANGUAGES
# ═══════════════════════════════════════════════════════════════════════════

LANGUAGES = {
    "ru": "🇷🇺 Русский",
    "kz": "🇰🇿 Қазақша",
    "en": "🇬🇧 English",
}

DEFAULT_LANG = "ru"

# Хранилище языка пользователя: {user_id: "ru"|"kz"|"en"}
_user_languages: dict[int, str] = {}


# ═══════════════════════════════════════════════════════════════════════════
#  TRANSLATIONS
# ═══════════════════════════════════════════════════════════════════════════

TRANSLATIONS: dict[str, dict[str, str]] = {
    "welcome_not_subscribed": {
        "ru": (
            "📋 Для получения бесплатного мини-гайда по юридическим вопросам "
            "необходимо подписаться на наш канал.\n\n"
            "После подписки нажмите «Проверить подписку» 👇"
        ),
        "kz": (
            "📋 Заңды мәселелер бойынша тегін мини-гайд алу үшін "
            "біздің арнаға жазылуыңыз қажет.\n\n"
            "Жазылғаннан кейін «Жазылымды тексеру» батырмасын басыңыз 👇"
        ),
        "en": (
            "📋 To receive a free mini-guide on legal matters, "
            "please subscribe to our channel.\n\n"
            "After subscribing, tap «Check subscription» 👇"
        ),
    },
    "welcome_subscribed": {
        "ru": "✅ Отлично! Вы подписаны на канал.\nВыберите интересующий вас гайд:",
        "kz": "✅ Тамаша! Сіз арнаға жазылдыңыз.\nСізді қызықтыратын гайдты таңдаңыз:",
        "en": "✅ Great! You're subscribed to the channel.\nChoose a guide that interests you:",
    },
    "guide_delivered": {
        "ru": "📚 Ваш мини-гайд по юридическим вопросам.\nСохраните его для дальнейшего использования.",
        "kz": "📚 Заңды мәселелер бойынша мини-гайдыңыз.\nОны болашақта пайдалану үшін сақтаңыз.",
        "en": "📚 Your mini-guide on legal matters.\nSave it for future reference.",
    },
    "ask_email": {
        "ru": "📝 Укажите ваш email для получения дополнительных материалов:",
        "kz": "📝 Қосымша материалдар алу үшін email-іңізді жазыңыз:",
        "en": "📝 Please provide your email to receive additional materials:",
    },
    "consult_intro": {
        "ru": (
            "🤖 <b>AI Мини-консультация от SOLIS Partners</b>\n\n"
            "Задайте ваш юридический вопрос 👇"
        ),
        "kz": (
            "🤖 <b>SOLIS Partners-тен AI Мини-кеңес</b>\n\n"
            "Заңды сұрағыңызды жазыңыз 👇"
        ),
        "en": (
            "🤖 <b>AI Mini-consultation by SOLIS Partners</b>\n\n"
            "Type your legal question 👇"
        ),
    },
    "rate_limit": {
        "ru": "⚠️ Вы исчерпали дневной лимит AI-запросов ({limit} в день).",
        "kz": "⚠️ Сіз AI сұраныстарының күнделікті лимитін ({limit}) таусыпсыз.",
        "en": "⚠️ You've reached the daily AI query limit ({limit} per day).",
    },
    "nps_request": {
        "ru": "💬 <b>Нам важно ваше мнение!</b>\n\nБыл ли полезен ответ нашего AI-юриста?",
        "kz": "💬 <b>Пікіріңіз бізге маңызды!</b>\n\nAI-заңгеріміздің жауабы пайдалы болды ма?",
        "en": "💬 <b>Your opinion matters!</b>\n\nWas our AI lawyer's response helpful?",
    },
    "choose_language": {
        "ru": "🌍 Выберите язык / Тілді таңдаңыз / Choose language:",
        "kz": "🌍 Тілді таңдаңыз / Выберите язык / Choose language:",
        "en": "🌍 Choose language / Выберите язык / Тілді таңдаңыз:",
    },
}


# ═══════════════════════════════════════════════════════════════════════════
#  PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════


def set_user_lang(user_id: int, lang: str) -> None:
    """Устанавливает язык пользователя."""
    if lang in LANGUAGES:
        _user_languages[user_id] = lang
        logger.info("Language set: user_id=%s -> %s", user_id, lang)


def get_user_lang(user_id: int, telegram_lang: str = "") -> str:
    """Получает язык пользователя.

    Приоритет: явная настройка > Telegram language_code > default.
    """
    if user_id in _user_languages:
        return _user_languages[user_id]

    # Маппинг Telegram language_code → наш код
    tg_map = {"ru": "ru", "kk": "kz", "en": "en", "uk": "ru"}
    if telegram_lang in tg_map:
        return tg_map[telegram_lang]

    return DEFAULT_LANG


def t(key: str, lang: str = "ru", **kwargs) -> str:
    """Получает перевод по ключу.

    Args:
        key: Ключ перевода.
        lang: Код языка (ru/kz/en).
        **kwargs: Подстановки для .format().

    Returns:
        Переведённый текст.
    """
    translations = TRANSLATIONS.get(key, {})
    text = translations.get(lang) or translations.get(DEFAULT_LANG, key)

    if kwargs:
        try:
            text = text.format(**kwargs)
        except (KeyError, IndexError):
            pass

    return text


def detect_language(text: str) -> str:
    """Определяет язык текста по характерным символам.

    Returns:
        "ru", "kz" или "en".
    """
    if not text:
        return DEFAULT_LANG

    text_lower = text.lower()

    # Казахский: специфические буквы
    kz_chars = set("әіңғүұқөһ")
    if any(c in kz_chars for c in text_lower):
        return "kz"

    # Кириллица → русский
    cyrillic = sum(1 for c in text_lower if "\u0400" <= c <= "\u04ff")
    latin = sum(1 for c in text_lower if "a" <= c <= "z")

    if cyrillic > latin:
        return "ru"
    elif latin > cyrillic:
        return "en"

    return DEFAULT_LANG


def get_all_user_langs() -> dict[str, int]:
    """Статистика языков пользователей."""
    stats: dict[str, int] = defaultdict(int)
    for lang in _user_languages.values():
        stats[lang] += 1
    return dict(stats)
