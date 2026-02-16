"""Клиент Telegraph API — публикация статей для Instant View в Telegram.

Используется telegra.ph — встроенный в Telegram инструмент для чтения
статей прямо внутри мессенджера (без перехода на внешний сайт).

Использование:
    from src.bot.utils.telegraph_client import publish_to_telegraph
    url = await publish_to_telegraph("Заголовок", "<p>HTML-контент</p>")
"""

import json
import logging
import os

logger = logging.getLogger(__name__)

TELEGRAPH_API = "https://api.telegra.ph"
TOKEN_FILE = os.path.join("data", "telegraph_token.txt")

# Теги, поддерживаемые Telegraph
ALLOWED_TAGS = frozenset({
    "a", "aside", "b", "blockquote", "br", "code", "em",
    "figcaption", "figure", "h3", "h4", "hr", "i", "img",
    "li", "ol", "p", "pre", "s", "strong", "u", "ul",
})

# Маппинг HTML-тегов → теги Telegraph
TAG_REMAP = {
    "h1": "h3",
    "h2": "h3",
    "h5": "h4",
    "h6": "h4",
    "div": None,     # unwrap
    "span": None,    # unwrap
    "section": None, # unwrap
    "article": None, # unwrap
    "header": None,
    "footer": None,
    "main": None,
    "nav": None,
}


def _html_to_nodes(html: str) -> list:
    """Конвертирует HTML-строку в формат Telegraph Node."""
    from bs4 import BeautifulSoup, NavigableString, Tag as BSTag

    soup = BeautifulSoup(html, "html.parser")

    def _convert(element) -> list:
        nodes = []
        for child in element.children:
            if isinstance(child, NavigableString):
                text = str(child)
                if text:
                    nodes.append(text)
            elif isinstance(child, BSTag):
                tag = child.name.lower()
                children = _convert(child)

                # Remap to Telegraph-compatible tag
                if tag in TAG_REMAP:
                    mapped = TAG_REMAP[tag]
                    if mapped is None:
                        # Unwrap — keep children, discard tag
                        nodes.extend(children)
                        continue
                    tag = mapped

                if tag not in ALLOWED_TAGS:
                    # Unsupported — unwrap
                    nodes.extend(children)
                    continue

                node: dict = {"tag": tag}

                # Preserve only relevant attributes
                if tag == "a" and child.get("href"):
                    node["attrs"] = {"href": child["href"]}
                elif tag == "img" and child.get("src"):
                    node["attrs"] = {"src": child["src"]}

                if children:
                    node["children"] = children

                nodes.append(node)
        return nodes

    result = _convert(soup)

    if not result:
        result = [{"tag": "p", "children": ["(пустая статья)"]}]

    return result


async def _api_call(method: str, **params) -> dict:
    """Вызов Telegraph API."""
    import aiohttp

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{TELEGRAPH_API}/{method}",
            data=params,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            data = await resp.json()
            if data.get("ok"):
                return data["result"]
            raise RuntimeError(f"Telegraph API: {data.get('error', 'unknown error')}")


async def _get_or_create_token() -> str:
    """Получает или создаёт аккаунт Telegraph."""
    os.makedirs(os.path.dirname(TOKEN_FILE) or ".", exist_ok=True)

    if os.path.isfile(TOKEN_FILE):
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            token = f.read().strip()
        if token:
            return token

    result = await _api_call(
        "createAccount",
        short_name="SOLIS Partners",
        author_name="SOLIS Partners",
        author_url="https://solispartners.kz",
    )
    token = result["access_token"]

    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        f.write(token)

    logger.info("Telegraph аккаунт создан, токен сохранён")
    return token


async def publish_to_telegraph(
    title: str,
    html_content: str,
    author_name: str = "SOLIS Partners",
    *,
    cover_image_url: str = "",
) -> str:
    """Публикует статью в Telegraph. Возвращает URL.

    Args:
        title: Заголовок статьи (1-256 символов).
        html_content: Содержание в HTML.
        author_name: Имя автора.
        cover_image_url: URL обложки (DALL-E или другой). Вставляется первым блоком.

    Returns:
        URL опубликованной страницы (telegra.ph/...).
    """
    token = await _get_or_create_token()

    # Вставляем обложку как первый блок статьи
    cover_nodes: list = []
    if cover_image_url:
        cover_nodes = [
            {
                "tag": "figure",
                "children": [
                    {
                        "tag": "img",
                        "attrs": {"src": cover_image_url},
                    },
                    {
                        "tag": "figcaption",
                        "children": [f"© SOLIS Partners — {title[:100]}"],
                    },
                ],
            },
        ]

    content_nodes = _html_to_nodes(html_content)
    all_nodes = cover_nodes + content_nodes

    result = await _api_call(
        "createPage",
        access_token=token,
        title=title[:256],
        content=json.dumps(all_nodes, ensure_ascii=False),
        author_name=author_name[:128],
        author_url="https://solispartners.kz",
        return_content="false",
    )

    url = result.get("url", "")
    logger.info("Статья опубликована в Telegraph: %s", url)
    return url
