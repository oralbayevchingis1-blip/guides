"""Генерация промо-материалов для гайдов.

Создаёт готовые тексты для:
- Постов в Telegram-канал
- Вставок в статьи (Telegraph / сайт)
- CTA-блоков с deep link
"""

import html
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _esc(text: str) -> str:
    return html.escape(str(text))


def build_guide_promo(
    guide: dict,
    bot_username: str,
    *,
    utm_source: str = "",
    download_count: int = 0,
) -> dict[str, str]:
    """Генерирует промо-материалы для одного гайда.

    Args:
        guide: Словарь из каталога гайдов (id, title, description, ...).
        bot_username: Username бота (без @).
        utm_source: Источник для UTM-метки (linkedin, channel, email...).
        download_count: Количество скачиваний (для social proof).

    Returns:
        Словарь с ключами:
        - ``channel_post``  — HTML-текст для Telegram-канала
        - ``article_block`` — HTML-блок для вставки в статьи
        - ``telegraph_cta`` — CTA-блок для Telegraph статей
        - ``deep_link``     — Deep link на гайд с UTM
        - ``short_cta``     — Короткая строка CTA для соцсетей
    """
    gid = guide.get("id", "")
    title = guide.get("title", gid)
    desc = guide.get("description", "")
    preview = guide.get("preview_text", "") or guide.get("preview", "")
    highlights = guide.get("highlights", "")
    pages = str(guide.get("pages", "")).strip()
    category = guide.get("category", "")
    social_proof = guide.get("social_proof", "")

    # Deep link с UTM
    utm_suffix = f"--{utm_source}" if utm_source else ""
    deep_link = f"https://t.me/{bot_username}?start=guide_{gid}{utm_suffix}"

    # ── Разбираем highlights ──────────────────────────────────────────
    highlight_items = _parse_highlights(highlights)

    # ── Channel post ──────────────────────────────────────────────────
    channel_post = _build_channel_post(
        title=title,
        desc=desc,
        highlights=highlight_items,
        preview=preview,
        pages=pages,
        category=category,
        deep_link=deep_link,
        download_count=download_count,
        social_proof=social_proof,
    )

    # ── Article block (вставка в статью) ─────────────────────────────
    article_block = _build_article_block(
        title=title,
        desc=desc,
        highlights=highlight_items,
        preview=preview,
        pages=pages,
        deep_link=deep_link,
    )

    # ── Telegraph CTA block ──────────────────────────────────────────
    telegraph_cta = _build_telegraph_cta(
        title=title,
        highlights=highlight_items,
        preview=preview,
        pages=pages,
        deep_link=deep_link,
    )

    # ── Short CTA (для соцсетей) ─────────────────────────────────────
    short_cta = (
        f"📥 Скачайте бесплатный гайд «{title}» — "
        f"с шаблонами и чек-листами → {deep_link}"
    )

    return {
        "channel_post": channel_post,
        "article_block": article_block,
        "telegraph_cta": telegraph_cta,
        "deep_link": deep_link,
        "short_cta": short_cta,
    }


def _parse_highlights(raw: str) -> list[str]:
    """Разбирает строку highlights в список пунктов.

    Поддерживает:
    - Перенос строк: ``пункт1\\nпункт2``
    - Разделитель запятой: ``пункт1, пункт2``
    - Разделитель точки с запятой: ``пункт1; пункт2``
    """
    if not raw or not raw.strip():
        return []

    for sep in ("\n", ";"):
        if sep in raw:
            return [item.strip() for item in raw.split(sep) if item.strip()]

    if "," in raw and raw.count(",") >= 2:
        return [item.strip() for item in raw.split(",") if item.strip()]

    return [raw.strip()] if raw.strip() else []


def _build_channel_post(
    *,
    title: str,
    desc: str,
    highlights: list[str],
    preview: str,
    pages: str,
    category: str,
    deep_link: str,
    download_count: int,
    social_proof: str,
) -> str:
    """Пост для Telegram-канала: тизер + выдержки + CTA."""
    parts: list[str] = []

    # Заголовок
    if category:
        parts.append(f"📂 {_esc(category)}")
    parts.append(f"📚 <b>{_esc(title)}</b>")

    # Описание
    if desc:
        parts.append(f"\n{_esc(desc)}")

    # Выдержки / что внутри
    if highlights:
        parts.append("\n📋 <b>Что внутри:</b>")
        for item in highlights[:6]:
            parts.append(f"  ✓ {_esc(item)}")
    elif preview:
        parts.append(f"\n📋 <b>Что внутри:</b>\n{_esc(preview)}")

    # Метаданные
    meta: list[str] = []
    if pages:
        meta.append(f"{_esc(pages)} стр.")
    meta.append("PDF с шаблонами")
    meta.append("бесплатно")
    parts.append(f"\n📎 {' · '.join(meta)}")

    # Social proof
    if download_count > 10:
        parts.append(f"\n👥 Уже скачали {download_count}+ предпринимателей")
    elif social_proof:
        parts.append(f"\n✅ {_esc(social_proof)}")

    # CTA
    parts.append(
        f"\n📥 <b>Скачать полную версию с шаблонами:</b>\n"
        f"👉 <a href=\"{_esc(deep_link)}\">Получить гайд в боте</a>"
    )

    return "\n".join(parts)


def _build_article_block(
    *,
    title: str,
    desc: str,
    highlights: list[str],
    preview: str,
    pages: str,
    deep_link: str,
) -> str:
    """HTML-блок для вставки в статью на сайте."""
    parts: list[str] = [
        '<div style="background:#f8f9fa;border-left:4px solid #2563eb;'
        'padding:20px;margin:24px 0;border-radius:8px;">',
        f'<p style="margin:0 0 12px;font-size:18px;font-weight:bold;">'
        f'📚 {_esc(title)}</p>',
    ]

    if desc:
        parts.append(f'<p style="margin:0 0 12px;color:#555;">{_esc(desc)}</p>')

    if highlights:
        parts.append('<p style="margin:0 0 8px;font-weight:600;">Что внутри:</p>')
        parts.append("<ul style=\"margin:0 0 12px;padding-left:20px;\">")
        for item in highlights[:6]:
            parts.append(f"<li>{_esc(item)}</li>")
        parts.append("</ul>")
    elif preview:
        parts.append(
            f'<p style="margin:0 0 12px;color:#555;">'
            f'<b>Что внутри:</b> {_esc(preview)}</p>'
        )

    meta: list[str] = []
    if pages:
        meta.append(f"{_esc(pages)} страниц")
    meta.append("PDF")
    meta.append("бесплатно")
    parts.append(
        f'<p style="margin:0 0 12px;font-size:13px;color:#888;">'
        f'{" · ".join(meta)}</p>'
    )

    parts.append(
        f'<a href="{_esc(deep_link)}" '
        f'style="display:inline-block;background:#2563eb;color:#fff;'
        f'padding:10px 24px;border-radius:6px;text-decoration:none;'
        f'font-weight:bold;">📥 Скачать полную версию</a>'
    )
    parts.append("</div>")

    return "\n".join(parts)


def _build_telegraph_cta(
    *,
    title: str,
    highlights: list[str],
    preview: str,
    pages: str,
    deep_link: str,
) -> str:
    """CTA-блок для вставки в конец Telegraph-статьи."""
    parts: list[str] = [
        "─" * 30,
        "",
        f"📚 <b>Скачайте полный гайд: «{_esc(title)}»</b>",
    ]

    if highlights:
        parts.append("")
        parts.append("Внутри вы найдёте:")
        for item in highlights[:5]:
            parts.append(f"✓ {_esc(item)}")
    elif preview:
        parts.append(f"\nВнутри: {_esc(preview)}")

    meta = []
    if pages:
        meta.append(f"{_esc(pages)} страниц")
    meta.append("шаблоны документов")
    meta.append("чек-листы")
    parts.append(f"\n📎 {' · '.join(meta)}")

    parts.append(
        f"\n👉 Скачать бесплатно: {deep_link}"
    )

    return "\n".join(parts)
