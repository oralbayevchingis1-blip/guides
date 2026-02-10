"""Утилита добавления юридического дисклеймера к сообщениям."""

from src.constants import FALLBACK_TEXTS


def add_disclaimer(text: str, texts: dict[str, str] | None = None) -> str:
    """Добавляет юридический дисклеймер в конец сообщения.

    Args:
        text: Исходный текст сообщения.
        texts: Словарь текстов из Google Sheets (опционально).

    Returns:
        Текст с добавленным дисклеймером.
    """
    if texts:
        disclaimer = texts.get("disclaimer", FALLBACK_TEXTS.get("disclaimer", ""))
    else:
        disclaimer = FALLBACK_TEXTS.get("disclaimer", "")
    return text + disclaimer
