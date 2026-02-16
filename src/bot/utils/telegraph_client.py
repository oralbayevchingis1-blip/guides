"""Простой клиент Telegraph API — публикация статей для Instant View."""

import logging
from typing import Optional

import aiohttp

from src.config import settings

logger = logging.getLogger(__name__)

TELEGRAPH_API = "https://api.telegra.ph"


async def create_account(short_name: str = "SOLIS Partners") -> str | None:
    """Создаёт аккаунт Telegraph и возвращает access_token."""
    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            f"{TELEGRAPH_API}/createAccount",
            json={"short_name": short_name, "author_name": short_name},
        )
        data = await resp.json()
        if data.get("ok"):
            token = data["result"]["access_token"]
            logger.info("Telegraph account created, token=%s...", token[:10])
            return token
        logger.error("Telegraph createAccount failed: %s", data)
        return None


async def publish_article(
    title: str,
    html_content: str,
    author_name: str = "SOLIS Partners",
    author_url: str = "https://www.solispartners.kz",
    *,
    guide_cta: Optional[dict] = None,
) -> Optional[str]:
    """Публикует статью в Telegraph. Возвращает URL или None.

    Args:
        guide_cta: Если передан, в конец статьи добавляется CTA-блок
            для скачивания гайда. Ожидаемые ключи:
            ``title``, ``highlights`` (list[str]), ``preview`` (str),
            ``pages`` (str), ``deep_link`` (str).
    """
    token = settings.TELEGRAPH_ACCESS_TOKEN
    if not token:
        logger.warning("TELEGRAPH_ACCESS_TOKEN не задан — публикация невозможна")
        return None

    content = _html_to_telegraph_nodes(html_content)

    if guide_cta:
        content.extend(_build_guide_cta_nodes(guide_cta))

    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            f"{TELEGRAPH_API}/createPage",
            json={
                "access_token": token,
                "title": title,
                "author_name": author_name,
                "author_url": author_url,
                "content": content,
                "return_content": False,
            },
        )
        data = await resp.json()

        if data.get("ok"):
            url = data["result"]["url"]
            logger.info("Статья опубликована: %s", url)
            return url

        logger.error("Telegraph createPage failed: %s", data)
        return None


def _html_to_telegraph_nodes(html: str) -> list:
    """Конвертирует простой HTML-текст в Telegraph Node-формат.

    Telegraph принимает массив Node-объектов. Для простоты разбиваем текст
    по абзацам и оборачиваем в <p> теги.
    """
    paragraphs = [p.strip() for p in html.split("\n") if p.strip()]
    nodes = []
    for p in paragraphs:
        if p.startswith("<"):
            nodes.append({"tag": "p", "children": [p]})
        else:
            nodes.append({"tag": "p", "children": [p]})
    return nodes if nodes else [{"tag": "p", "children": ["(пустая статья)"]}]


def _build_guide_cta_nodes(cta: dict) -> list:
    """Создаёт Telegraph-ноды для CTA-блока гайда в конце статьи.

    Блок включает: разделитель, заголовок, выдержки, мета, кнопку-ссылку.
    """
    nodes: list = []

    # Разделитель
    nodes.append({"tag": "hr"})

    # Заголовок
    guide_title = cta.get("title", "")
    nodes.append({
        "tag": "h4",
        "children": [f"📚 Скачайте полный гайд: «{guide_title}»"],
    })

    # Выдержки / что внутри
    highlights = cta.get("highlights", [])
    preview = cta.get("preview", "")

    if highlights:
        nodes.append({"tag": "p", "children": [
            {"tag": "strong", "children": ["Внутри вы найдёте:"]},
        ]})
        items = []
        for item in highlights[:5]:
            items.append({"tag": "li", "children": [item]})
        nodes.append({"tag": "ul", "children": items})
    elif preview:
        nodes.append({"tag": "p", "children": [
            {"tag": "strong", "children": ["Что внутри: "]},
            preview,
        ]})

    # Метаданные
    meta_parts = []
    pages = cta.get("pages", "")
    if pages:
        meta_parts.append(f"{pages} страниц")
    meta_parts.extend(["шаблоны документов", "чек-листы", "бесплатно"])
    nodes.append({"tag": "p", "children": [
        {"tag": "em", "children": ["📎 " + " · ".join(meta_parts)]},
    ]})

    # Кнопка-ссылка
    deep_link = cta.get("deep_link", "")
    if deep_link:
        nodes.append({"tag": "p", "children": [{
            "tag": "a",
            "attrs": {"href": deep_link},
            "children": ["👉 Скачать бесплатно в Telegram-боте"],
        }]})

    return nodes
