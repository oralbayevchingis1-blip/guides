"""Генерация промо-материалов для гайдов.

Создаёт готовые тексты для:
- Постов в Telegram-канал (тизер + выдержки + статистика + CTA)
- Превью-блоков для статей (цитаты, цифры, ценностное предложение)
- CTA-блоков с deep link
- Постов для LinkedIn / соцсетей
- Email-сниппетов
"""

import html
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ── Хуки по категориям (вовлекающее вступление) ──────────────────────

_CATEGORY_HOOKS: dict[str, str] = {
    "налог": "Каждый 3-й предприниматель переплачивает налоги из-за незнания льгот.",
    "труд": "67% трудовых споров в Казахстане заканчиваются штрафами для работодателя.",
    "it": "IT-компании в Казахстане могут экономить до 40% на налогах — если правильно структурировать бизнес.",
    "инвест": "70% инвесторов не знают о специальных режимах защиты вложений в Казахстане.",
    "m&a": "Каждая вторая сделка M&A в СНГ затягивается из-за юридических ошибок на старте.",
    "корпоратив": "8 из 10 ТОО в Казахстане имеют ошибки в учредительных документах.",
    "договор": "Неправильно составленный договор — причина 60% бизнес-споров.",
    "недвижим": "При покупке коммерческой недвижимости 90% рисков можно устранить на этапе due diligence.",
    "лицензи": "Работа без лицензии — штраф до 200 МРП и приостановка деятельности.",
    "интеллектуал": "Только 15% компаний в Казахстане защищают свою интеллектуальную собственность.",
}

_DEFAULT_HOOK = "Юридическая ошибка может стоить бизнесу миллионы. Мы собрали практические решения в одном документе."


def _get_category_hook(category: str) -> str:
    """Подбирает вовлекающий хук по категории гайда."""
    low = category.lower()
    for key, hook in _CATEGORY_HOOKS.items():
        if key in low:
            return hook
    return _DEFAULT_HOOK


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
        - ``channel_post``  — HTML-текст для Telegram-канала (с превью)
        - ``article_block`` — HTML-блок для вставки в статьи
        - ``telegraph_cta`` — CTA-блок для Telegraph статей
        - ``linkedin_post`` — Текст для LinkedIn / Facebook
        - ``email_snippet``  — HTML-сниппет для email-рассылки
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
    excerpt = guide.get("excerpt", "") or guide.get("key_quote", "")
    key_stat = guide.get("key_stat", "") or guide.get("statistic", "")

    # Deep link с UTM
    utm_suffix = f"--{utm_source}" if utm_source else ""
    deep_link = f"https://t.me/{bot_username}?start=guide_{gid}{utm_suffix}"

    # ── Разбираем highlights ──────────────────────────────────────────
    highlight_items = _parse_highlights(highlights)

    # Хук по категории
    hook = _get_category_hook(category)

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
        hook=hook,
        excerpt=excerpt,
        key_stat=key_stat,
    )

    # ── Article block (вставка в статью) ─────────────────────────────
    article_block = _build_article_block(
        title=title,
        desc=desc,
        highlights=highlight_items,
        preview=preview,
        pages=pages,
        deep_link=deep_link,
        excerpt=excerpt,
        key_stat=key_stat,
        download_count=download_count,
    )

    # ── Telegraph CTA block ──────────────────────────────────────────
    telegraph_cta = _build_telegraph_cta(
        title=title,
        highlights=highlight_items,
        preview=preview,
        pages=pages,
        deep_link=deep_link,
        excerpt=excerpt,
        download_count=download_count,
    )

    # ── LinkedIn post ─────────────────────────────────────────────────
    linkedin_post = _build_linkedin_post(
        title=title,
        desc=desc,
        highlights=highlight_items,
        hook=hook,
        key_stat=key_stat,
        deep_link=deep_link.replace(utm_suffix, "--linkedin") if utm_suffix else deep_link + "--linkedin",
        download_count=download_count,
    )

    # ── Email snippet ─────────────────────────────────────────────────
    email_snippet = _build_email_snippet(
        title=title,
        desc=desc,
        highlights=highlight_items,
        excerpt=excerpt,
        pages=pages,
        deep_link=deep_link.replace(utm_suffix, "--email") if utm_suffix else deep_link + "--email",
        download_count=download_count,
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
        "linkedin_post": linkedin_post,
        "email_snippet": email_snippet,
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
    hook: str = "",
    excerpt: str = "",
    key_stat: str = "",
) -> str:
    """Пост для Telegram-канала: хук → выдержка → выгода → CTA."""
    parts: list[str] = []

    # Вовлекающий хук (статистика / вопрос)
    if hook:
        parts.append(f"💡 <i>{_esc(hook)}</i>")
        parts.append("")

    # Заголовок
    if category:
        parts.append(f"📂 {_esc(category)}")
    parts.append(f"📚 <b>{_esc(title)}</b>")

    # Описание
    if desc:
        parts.append(f"\n{_esc(desc)}")

    # Ключевая цитата / выдержка из гайда
    if excerpt:
        parts.append(f"\n<blockquote>«{_esc(excerpt)}»</blockquote>")
    elif key_stat:
        parts.append(f"\n📊 <b>{_esc(key_stat)}</b>")

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

    # CTA — конкретная ценность перехода
    cta_value = "с шаблонами договоров и чек-листами"
    if any(kw in title.lower() for kw in ("налог", "tax")):
        cta_value = "с расчётами, примерами и чек-листами"
    elif any(kw in title.lower() for kw in ("труд", "labor", "кадр")):
        cta_value = "с образцами документов и порядком действий"
    elif any(kw in title.lower() for kw in ("it", "ит", "цифр")):
        cta_value = "со схемами оптимизации и примерами"

    parts.append(
        f"\n📥 <b>Полную версию {cta_value} скачивайте бесплатно:</b>\n"
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
    excerpt: str = "",
    key_stat: str = "",
    download_count: int = 0,
) -> str:
    """HTML-блок для вставки в статью на сайте.

    Включает превью контента (выдержку/цитату) для повышения
    ценности перехода.
    """
    parts: list[str] = [
        '<div style="background:#f8f9fa;border-left:4px solid #2563eb;'
        'padding:20px;margin:24px 0;border-radius:8px;">',
        f'<p style="margin:0 0 12px;font-size:18px;font-weight:bold;">'
        f'📚 {_esc(title)}</p>',
    ]

    if desc:
        parts.append(f'<p style="margin:0 0 12px;color:#555;">{_esc(desc)}</p>')

    # Выдержка из гайда — повышает ценность перехода
    if excerpt:
        parts.append(
            f'<blockquote style="margin:12px 0;padding:10px 16px;'
            f'border-left:3px solid #94a3b8;color:#475569;font-style:italic;">'
            f'«{_esc(excerpt)}»</blockquote>'
        )
    elif key_stat:
        parts.append(
            f'<p style="margin:0 0 12px;font-size:15px;font-weight:600;'
            f'color:#2563eb;">📊 {_esc(key_stat)}</p>'
        )

    if highlights:
        parts.append('<p style="margin:0 0 8px;font-weight:600;">Что внутри:</p>')
        parts.append('<ul style="margin:0 0 12px;padding-left:20px;">')
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
    if download_count > 10:
        meta.append(f"скачали {download_count}+ человек")
    parts.append(
        f'<p style="margin:0 0 12px;font-size:13px;color:#888;">'
        f'{" · ".join(meta)}</p>'
    )

    parts.append(
        f'<a href="{_esc(deep_link)}" '
        f'style="display:inline-block;background:#2563eb;color:#fff;'
        f'padding:12px 28px;border-radius:6px;text-decoration:none;'
        f'font-weight:bold;font-size:15px;">'
        f'📥 Скачать полную версию с шаблонами</a>'
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
    excerpt: str = "",
    download_count: int = 0,
) -> str:
    """CTA-блок для вставки в конец Telegraph-статьи.

    Включает превью контента для мотивации перехода.
    """
    parts: list[str] = [
        "─" * 30,
        "",
        f"📚 <b>Скачайте полный гайд: «{_esc(title)}»</b>",
    ]

    # Превью — цитата из гайда прямо в статье
    if excerpt:
        parts.append(f"\n<i>«{_esc(excerpt)}»</i>")
        parts.append("\n↑ Это лишь фрагмент. В полной версии — "
                     "пошаговые инструкции и шаблоны.")

    if highlights:
        parts.append("\nВнутри вы найдёте:")
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

    if download_count > 10:
        parts.append(f"\n👥 Уже скачали {download_count}+ предпринимателей")

    parts.append(
        f"\n👉 Скачать бесплатно: {deep_link}"
    )

    return "\n".join(parts)


def _build_linkedin_post(
    *,
    title: str,
    desc: str,
    highlights: list[str],
    hook: str,
    key_stat: str,
    deep_link: str,
    download_count: int = 0,
) -> str:
    """Текст для LinkedIn / Facebook поста (plain text, без HTML)."""
    parts: list[str] = []

    # Хук — первая строка видна в ленте
    if key_stat:
        parts.append(f"📊 {key_stat}")
    elif hook:
        parts.append(f"💡 {hook}")

    parts.append("")
    parts.append(f"Мы подготовили бесплатный гайд: «{title}»")
    parts.append("")

    if desc:
        parts.append(desc)
        parts.append("")

    if highlights:
        parts.append("Что внутри:")
        for item in highlights[:4]:
            parts.append(f"→ {item}")
        parts.append("")

    if download_count > 10:
        parts.append(f"Уже скачали {download_count}+ предпринимателей.")
        parts.append("")

    parts.append(f"📥 Скачать бесплатно → {deep_link}")
    parts.append("")
    parts.append("#юридическаяконсультация #бизнесвказахстане #гайд #чеклист")

    return "\n".join(parts)


def _build_email_snippet(
    *,
    title: str,
    desc: str,
    highlights: list[str],
    excerpt: str,
    pages: str,
    deep_link: str,
    download_count: int = 0,
) -> str:
    """HTML-сниппет для email-рассылки."""
    parts: list[str] = [
        '<table style="width:100%;border-collapse:collapse;margin:20px 0;">',
        '<tr><td style="padding:20px;background:#f8f9fa;border-radius:8px;">',
        f'<h3 style="margin:0 0 10px;color:#1e293b;">📚 {_esc(title)}</h3>',
    ]

    if desc:
        parts.append(f'<p style="margin:0 0 10px;color:#64748b;">{_esc(desc)}</p>')

    if excerpt:
        parts.append(
            f'<p style="margin:10px 0;padding:10px 15px;border-left:3px solid #2563eb;'
            f'color:#475569;font-style:italic;">«{_esc(excerpt)}»</p>'
        )

    if highlights:
        parts.append('<ul style="margin:0 0 10px;padding-left:18px;color:#334155;">')
        for item in highlights[:4]:
            parts.append(f"<li>{_esc(item)}</li>")
        parts.append("</ul>")

    meta: list[str] = []
    if pages:
        meta.append(f"{_esc(pages)} стр.")
    meta.append("PDF")
    meta.append("бесплатно")
    if download_count > 10:
        meta.append(f"{download_count}+ скачиваний")

    parts.append(
        f'<p style="margin:0 0 12px;font-size:12px;color:#94a3b8;">'
        f'{" · ".join(meta)}</p>'
    )

    parts.append(
        f'<a href="{_esc(deep_link)}" '
        f'style="display:inline-block;background:#2563eb;color:#ffffff;'
        f'padding:10px 24px;border-radius:6px;text-decoration:none;'
        f'font-weight:bold;">Скачать гайд →</a>'
    )

    parts.append("</td></tr></table>")
    return "\n".join(parts)
