"""Синхронизация статей: Google Sheets → articles.ts → Git push → Vercel deploy.

Использование:
    python sync_articles.py          # Синхронизирует и пушит
    python sync_articles.py --dry    # Только показать что изменится (без записи)
    python sync_articles.py --init   # Создать лист и загрузить текущие статьи в Sheets

Как работает:
    1. Читает лист «Статьи сайта» из Google Sheets
    2. Генерирует файл src/data/articles.ts
    3. Коммитит и пушит в GitHub
    4. Vercel автоматически деплоит
"""

import io
import json
import os
import subprocess
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv

load_dotenv()

import gspread
from google.oauth2.service_account import Credentials

# ── Конфигурация ──────────────────────────────────────────────────────

CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "google_credentials.json")
SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "")
SHEET_NAME = "Статьи сайта"
SITE_REPO_PATH = os.path.join(os.path.dirname(__file__), "legal-partner-pro-review")
ARTICLES_TS_PATH = os.path.join(SITE_REPO_PATH, "src", "data", "articles.ts")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Заголовки листа
HEADERS = [
    "id",
    "title",
    "date",
    "author",
    "category",
    "categoryRu",
    "image",
    "description",
    "externalUrl",
    "content",
    "isGoldTag",
    "practiceId",
    "telegramBotLink",
    "telegramBotCTA_title",
    "telegramBotCTA_description",
    "active",
]

# Допустимые категории
VALID_CATEGORIES = ["News", "Analytics", "Interview", "Guide", "Legal Opinion", "Media"]


# ── Генерация TypeScript ────────────────────────────────────────────────


def escape_ts_string(s: str) -> str:
    """Экранирует строку для TypeScript шаблонных литералов."""
    return s.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")


def row_to_article_ts(row: dict) -> str:
    """Конвертирует строку из Sheets в TypeScript объект."""
    article_id = str(row.get("id", "")).strip()
    if not article_id:
        return ""

    title = str(row.get("title", "")).strip()
    date = str(row.get("date", "")).strip()
    author = str(row.get("author", "SOLIS Partners")).strip()
    category = str(row.get("category", "Guide")).strip()
    category_ru = str(row.get("categoryRu", "")).strip()
    image = str(row.get("image", "/assets/logo-solis.jpg")).strip()
    description = str(row.get("description", "")).strip()
    external_url = str(row.get("externalUrl", "")).strip()
    content = str(row.get("content", "")).strip()
    is_gold = str(row.get("isGoldTag", "")).strip().upper() == "TRUE"
    practice_ids_raw = str(row.get("practiceId", "")).strip()
    bot_link = str(row.get("telegramBotLink", "")).strip()
    bot_cta_title = str(row.get("telegramBotCTA_title", "")).strip()
    bot_cta_desc = str(row.get("telegramBotCTA_description", "")).strip()

    if category not in VALID_CATEGORIES:
        category = "Guide"

    # Формируем practiceId массив
    practice_ids = []
    if practice_ids_raw:
        practice_ids = [p.strip() for p in practice_ids_raw.split(",") if p.strip()]

    lines = []
    lines.append("  {")
    lines.append(f'    id: "{escape_ts_string(article_id)}",')
    lines.append(f'    title: "{escape_ts_string(title)}",')
    lines.append(f'    date: "{escape_ts_string(date)}",')
    lines.append(f'    author: "{escape_ts_string(author)}",')
    lines.append(f'    category: "{category}",')
    lines.append(f'    categoryRu: "{escape_ts_string(category_ru)}",')
    lines.append(f'    image: "{escape_ts_string(image)}",')
    lines.append(f'    description: "{escape_ts_string(description)}",')

    if external_url:
        lines.append(f'    externalUrl: "{escape_ts_string(external_url)}",')

    if content:
        lines.append(f"    content: `{escape_ts_string(content)}`,")

    if is_gold:
        lines.append("    isGoldTag: true,")

    if practice_ids:
        ids_str = ", ".join(f'"{p}"' for p in practice_ids)
        lines.append(f"    practiceId: [{ids_str}],")

    if bot_link:
        lines.append(f'    telegramBotLink: "{escape_ts_string(bot_link)}",')

    if bot_cta_title or bot_cta_desc:
        lines.append("    telegramBotCTA: {")
        if bot_cta_title:
            lines.append(f'      title: "{escape_ts_string(bot_cta_title)}",')
        if bot_cta_desc:
            lines.append(f'      description: "{escape_ts_string(bot_cta_desc)}",')
        lines.append("    },")

    lines.append("  }")

    return "\n".join(lines)


def generate_articles_ts(rows: list[dict]) -> str:
    """Генерирует полный файл articles.ts из строк Sheets."""
    articles_blocks = []
    for row in rows:
        active = str(row.get("active", "TRUE")).strip().upper()
        if active != "TRUE":
            continue
        block = row_to_article_ts(row)
        if block:
            articles_blocks.append(block)

    articles_body = ",\n".join(articles_blocks)

    return f'''import {{ TrendingUp, Star, Newspaper, BookOpen, Mic, Sparkles, FileText }} from "lucide-react";

export interface Article {{
  id: string;
  title: string;
  date: string;
  author: string;
  category: "News" | "Analytics" | "Interview" | "Guide" | "Legal Opinion" | "Media";
  categoryRu: string;
  image: string;
  description: string;
  externalUrl?: string;
  content?: string;
  isGoldTag?: boolean;
  practiceId?: string[];
  telegramBotLink?: string;
  telegramBotCTA?: {{
    title?: string;
    description?: string;
  }};
}}

export const articles: Article[] = [
{articles_body}
];

export const getCategoryIcon = (category: string) => {{
  switch (category) {{
    case "Analytics": return TrendingUp;
    case "Legal Opinion": return Star;
    case "News": return Newspaper;
    case "Media": return Mic;
    case "Guide": return BookOpen;
    default: return FileText;
  }}
}};
'''


# ── Инициализация: загрузка текущих статей в Sheets ──────────────────────


def parse_existing_articles() -> list[dict]:
    """Парсит существующий articles.ts и возвращает список словарей."""
    if not os.path.isfile(ARTICLES_TS_PATH):
        print(f"  Файл {ARTICLES_TS_PATH} не найден")
        return []

    with open(ARTICLES_TS_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    # Простой парсер — ищем объекты между { и }
    articles = []
    # Извлекаем каждый объект из массива
    import re

    # Найдем массив articles
    match = re.search(r"export const articles.*?=\s*\[(.*)\];", content, re.DOTALL)
    if not match:
        print("  Не удалось найти массив articles в файле")
        return []

    array_body = match.group(1)

    # Разбиваем на блоки по закрывающей скобке + запятая
    # Простой подход: ищем поля через regex
    blocks = re.split(r"\n  \{", array_body)

    for block in blocks:
        if not block.strip():
            continue

        article = {}

        def extract(field: str, block_text: str) -> str:
            # Ищем field: "value" или field: `value`
            m = re.search(rf'{field}:\s*"([^"]*)"', block_text)
            if m:
                return m.group(1)
            m = re.search(rf"{field}:\s*`(.*?)`", block_text, re.DOTALL)
            if m:
                return m.group(1)
            return ""

        article["id"] = extract("id", block)
        if not article["id"]:
            continue

        article["title"] = extract("title", block)
        article["date"] = extract("date", block)
        article["author"] = extract("author", block)
        article["category"] = extract("category", block)
        article["categoryRu"] = extract("categoryRu", block)
        article["image"] = extract("image", block) or "/assets/logo-solis.jpg"
        article["description"] = extract("description", block)
        article["externalUrl"] = extract("externalUrl", block)

        # content — может быть многострочный
        content_match = re.search(r"content:\s*`(.*?)`", block, re.DOTALL)
        article["content"] = content_match.group(1).strip() if content_match else ""

        article["isGoldTag"] = "TRUE" if "isGoldTag: true" in block else ""

        # practiceId
        pid_match = re.search(r"practiceId:\s*\[(.*?)\]", block)
        if pid_match:
            ids = re.findall(r'"([^"]+)"', pid_match.group(1))
            article["practiceId"] = ", ".join(ids)
        else:
            article["practiceId"] = ""

        article["telegramBotLink"] = extract("telegramBotLink", block)

        # telegramBotCTA
        cta_match = re.search(r"telegramBotCTA:\s*\{(.*?)\}", block, re.DOTALL)
        if cta_match:
            cta_block = cta_match.group(1)
            article["telegramBotCTA_title"] = extract("title", cta_block)
            article["telegramBotCTA_description"] = extract("description", cta_block)
        else:
            article["telegramBotCTA_title"] = ""
            article["telegramBotCTA_description"] = ""

        article["active"] = "TRUE"

        articles.append(article)

    return articles


def init_sheet(spreadsheet: gspread.Spreadsheet) -> None:
    """Создаёт лист и заполняет текущими статьями из articles.ts."""
    print("Режим --init: загрузка текущих статей в Google Sheets")
    print()

    # Создаём или находим лист
    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
        print(f"  Лист '{SHEET_NAME}' уже существует")
        existing = ws.get_all_records()
        if existing:
            print(f"  В листе уже {len(existing)} статей. Пропускаю инициализацию.")
            print("  (Удалите лист вручную если хотите перезаписать)")
            return
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=200, cols=len(HEADERS))
        print(f"  Создан лист '{SHEET_NAME}'")

    # Записываем заголовки
    ws.update(values=[HEADERS], range_name="A1")
    print(f"  Заголовки записаны: {len(HEADERS)} колонок")

    # Парсим текущие статьи
    articles = parse_existing_articles()
    print(f"  Найдено {len(articles)} статей в articles.ts")

    if articles:
        rows = []
        for a in articles:
            row = [a.get(h, "") for h in HEADERS]
            rows.append(row)

        cell_range = f"A2:{chr(64 + len(HEADERS))}{len(rows) + 1}"
        ws.update(values=rows, range_name=cell_range)
        print(f"  Загружено {len(rows)} статей в лист")

    print()
    print("  Готово! Теперь вы можете:")
    print("  1. Открыть таблицу и редактировать статьи")
    print("  2. Добавлять новые строки")
    print("  3. Запустить 'python sync_articles.py' для публикации")


# ── Основная логика ────────────────────────────────────────────────────


def main() -> None:
    dry_run = "--dry" in sys.argv
    init_mode = "--init" in sys.argv

    print("=" * 60)
    print("  Синхронизация статей: Google Sheets → Сайт")
    if dry_run:
        print("  [РЕЖИМ ПРОСМОТРА — без записи]")
    print("=" * 60)
    print()

    # Подключение
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    if init_mode:
        init_sheet(spreadsheet)
        return

    # Читаем статьи из Sheets
    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  Лист '{SHEET_NAME}' не найден!")
        print(f"  Сначала запустите: python sync_articles.py --init")
        sys.exit(1)

    rows = ws.get_all_records()
    active_rows = [r for r in rows if str(r.get("active", "")).strip().upper() == "TRUE"]

    print(f"  Всего строк: {len(rows)}")
    print(f"  Активных статей: {len(active_rows)}")
    print()

    if not active_rows:
        print("  Нет активных статей. Нечего синхронизировать.")
        return

    # Генерируем TypeScript
    ts_content = generate_articles_ts(rows)

    if dry_run:
        print("  Превью первых 2000 символов:")
        print("-" * 60)
        print(ts_content[:2000])
        if len(ts_content) > 2000:
            print(f"  ... (ещё {len(ts_content) - 2000} символов)")
        print("-" * 60)
        print()
        print(f"  Файл НЕ записан (dry-run). Статей: {len(active_rows)}")
        return

    # Записываем файл
    if not os.path.isdir(SITE_REPO_PATH):
        print(f"  Папка сайта не найдена: {SITE_REPO_PATH}")
        print("  Сначала склонируйте репозиторий сайта рядом.")
        sys.exit(1)

    with open(ARTICLES_TS_PATH, "w", encoding="utf-8") as f:
        f.write(ts_content)

    print(f"  Файл записан: {ARTICLES_TS_PATH}")
    print(f"  Статей: {len(active_rows)}")
    print()

    # Git commit + push
    print("  Коммитим и пушим в GitHub...")
    try:
        os.chdir(SITE_REPO_PATH)
        subprocess.run(["git", "add", "src/data/articles.ts"], check=True)

        result = subprocess.run(
            ["git", "status", "--porcelain", "src/data/articles.ts"],
            capture_output=True,
            text=True,
        )
        if not result.stdout.strip():
            print("  Нет изменений — файл идентичен. Пуш не нужен.")
            return

        subprocess.run(
            ["git", "commit", "-m", f"content: sync {len(active_rows)} articles from Google Sheets"],
            check=True,
        )
        subprocess.run(["git", "push", "origin", "main"], check=True)
        print("  Готово! Изменения запушены.")
        print("  Vercel задеплоит автоматически через 1-2 минуты.")
    except subprocess.CalledProcessError as e:
        print(f"  Ошибка Git: {e}")
        print("  Файл articles.ts обновлён локально, но не запушен.")

    print()
    print("=" * 60)
    print("  Синхронизация завершена!")
    print("=" * 60)


if __name__ == "__main__":
    main()
