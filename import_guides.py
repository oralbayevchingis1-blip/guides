"""Импорт гайдов из папки Google Drive в лист «Каталог гайдов».

Использование:
    python import_guides.py

Что делает:
    1. Читает файлы из папки Google Drive
    2. Показывает список найденных PDF
    3. Добавляет их в лист «Каталог гайдов» в Google Sheets
"""

import io
import os
import sys
import re

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

# ID папки из URL: https://drive.google.com/drive/u/0/folders/{FOLDER_ID}
DRIVE_FOLDER_ID = "16riC1MFwcI1aVxHY24hyYvOZFrspe1pm"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def make_guide_id(filename: str) -> str:
    """Генерирует id из имени файла: убирает расширение, заменяет пробелы на _."""
    name = os.path.splitext(filename)[0]
    # Транслитерация не нужна — просто чистим
    guide_id = re.sub(r"[^\w\s-]", "", name)
    guide_id = re.sub(r"[\s-]+", "_", guide_id).strip("_").lower()
    return guide_id[:50]  # ограничим длину


def main() -> None:
    print("=" * 60)
    print("  Импорт гайдов из Google Drive в каталог бота")
    print("=" * 60)
    print()

    # Подключение
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    gc = gspread.authorize(creds)

    # Получаем список файлов из папки Drive
    print(f"Сканирую папку Drive: {DRIVE_FOLDER_ID} ...")
    try:
        file_list = gc.http_client.request(
            "get",
            f"https://www.googleapis.com/drive/v3/files",
            params={
                "q": f"'{DRIVE_FOLDER_ID}' in parents and trashed = false",
                "fields": "files(id, name, mimeType, size)",
                "orderBy": "name",
                "pageSize": 100,
            },
        )
        files = file_list.json().get("files", [])
    except Exception as e:
        print(f"Ошибка доступа к папке Drive: {e}")
        print()
        print("Убедитесь, что папка расшарена на сервисный аккаунт:")
        print("  bd-for-solis@gen-lang-client-0567650418.iam.gserviceaccount.com")
        sys.exit(1)

    if not files:
        print("Папка пуста или нет доступа.")
        sys.exit(1)

    # Фильтруем PDF
    pdf_files = [f for f in files if f.get("mimeType") == "application/pdf" or f["name"].lower().endswith(".pdf")]
    other_files = [f for f in files if f not in pdf_files]

    print(f"\nНайдено файлов: {len(files)} (PDF: {len(pdf_files)})")
    print()

    if not pdf_files:
        print("PDF-файлы не найдены. Все файлы в папке:")
        for f in files:
            print(f"  - {f['name']} ({f.get('mimeType', '?')})")
        print("\nДобавляю все файлы как гайды...")
        pdf_files = files

    # Показываем список
    print("Файлы для импорта:")
    print("-" * 60)
    for i, f in enumerate(pdf_files, 1):
        guide_id = make_guide_id(f["name"])
        print(f"  {i}. {f['name']}")
        print(f"     id: {guide_id}")
        print(f"     drive_file_id: {f['id']}")
        print()

    # Подключаемся к таблице
    print("Подключаюсь к Google Sheets...")
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    try:
        ws = spreadsheet.worksheet("Каталог гайдов")
    except gspread.exceptions.WorksheetNotFound:
        print("Лист 'Каталог гайдов' не найден! Сначала запустите setup_sheets.py")
        sys.exit(1)

    # Читаем существующие id чтобы не дублировать
    existing_ids = set()
    existing_data = ws.get_all_records()
    for row in existing_data:
        if row.get("id"):
            existing_ids.add(row["id"])

    # Добавляем новые гайды
    added = 0
    skipped = 0
    rows_to_add = []

    for f in pdf_files:
        guide_id = make_guide_id(f["name"])
        if guide_id in existing_ids:
            print(f"  [пропуск] {f['name']} (id={guide_id} уже есть)")
            skipped += 1
            continue

        title = os.path.splitext(f["name"])[0]
        row = [
            guide_id,           # id
            title,              # title (имя файла без расширения)
            "",                 # description (заполните вручную)
            f["id"],            # drive_file_id
            "",                 # category (заполните вручную)
            "TRUE",             # active
        ]
        rows_to_add.append(row)
        added += 1

    if rows_to_add:
        # Находим первую пустую строку
        next_row = len(existing_data) + 2  # +1 заголовок, +1 следующая
        cell_range = f"A{next_row}:F{next_row + len(rows_to_add) - 1}"
        ws.update(values=rows_to_add, range_name=cell_range)

    print()
    print("=" * 60)
    print(f"  Добавлено: {added} гайдов")
    print(f"  Пропущено (уже были): {skipped}")
    print(f"  Всего в каталоге: {len(existing_data) + added}")
    print()
    if added > 0:
        print("  Рекомендуется:")
        print("  1. Откройте таблицу и заполните 'description' и 'category'")
        print(f"     {spreadsheet.url}")
        print("  2. Отправьте боту /refresh чтобы обновить кеш")
    print("=" * 60)


if __name__ == "__main__":
    main()
