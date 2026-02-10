"""Скрипт автоматической настройки Google Sheets для бота SOLIS Partners.

Запуск:
    python setup_sheets.py

Что делает:
    1. Подключается к Google Sheets через Service Account
    2. Создаёт 3 листа: «Каталог гайдов», «Тексты бота», «Лиды»
    3. Заполняет заголовки и тексты бота
    4. Выводит ID таблицы для вставки в .env

Перед запуском:
    - Должен существовать файл google_credentials.json (Service Account key)
    - В .env должен быть GOOGLE_SPREADSHEET_ID (ID вашей таблицы)
    - Таблица должна быть расшарена на email сервисного аккаунта (Editor)
"""

import os
import sys
import io

# Фикс кодировки для Windows (эмодзи в консоли)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Добавляем корень проекта в sys.path
sys.path.insert(0, os.path.dirname(__file__))

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("❌ Библиотеки не установлены. Сначала выполните:")
    print("   pip install gspread google-auth")
    sys.exit(1)

from dotenv import load_dotenv

load_dotenv()

# ── Конфигурация ────────────────────────────────────────────────────────

CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "google_credentials.json")
SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── Данные для заполнения ───────────────────────────────────────────────

CATALOG_HEADERS = ["id", "title", "description", "drive_file_id", "category", "active"]

TEXTS_HEADERS = ["key", "text"]
TEXTS_DATA = [
    ["welcome_not_subscribed", "📋 Для получения бесплатного мини-гайда подпишитесь на наш канал @SOLISlegal.\n\nПосле подписки нажмите «Проверить подписку» 👇"],
    ["welcome_subscribed", "✅ Отлично! Вы подписаны на канал.\nВыберите интересующий вас гайд:"],
    ["subscription_success", "✅ Подписка подтверждена!\nТеперь выберите интересующий вас гайд:"],
    ["subscription_fail", "❌ Вы ещё не подписались на канал. Попробуйте снова."],
    ["guide_delivered", "📚 Ваш мини-гайд от SOLIS Partners.\nСохраните его для дальнейшего использования."],
    ["ask_email", "📝 Чтобы мы могли присылать вам дополнительные материалы и уведомления о новых гайдах, пожалуйста, укажите ваш email:"],
    ["invalid_email", "❌ Неверный формат email. Пожалуйста, введите корректный email:"],
    ["email_saved", "✅ Email сохранён. Теперь укажите ваше имя:"],
    ["invalid_name", "Пожалуйста, введите корректное имя (минимум 2 символа):"],
    ["consent_text", "🔒 **Согласие на обработку персональных данных**\n\nЯ даю согласие на обработку моих персональных данных в соответствии с политикой конфиденциальности.\n\n[Политика конфиденциальности]({privacy_url})\n\nБез согласия мы не сможем отправлять вам полезные материалы."],
    ["consent_given", "🎉 {name}, благодарим вас!\n\nТеперь вы будете получать:\n• Новые гайды и статьи\n• Юридические кейсы\n• Приглашения на вебинары\n\n📬 Первое письмо уже летит на {email}"],
    ["consent_declined", "Понимаем. Если передумаете — просто нажмите /start.\nВаш гайд уже у вас, пользуйтесь на здоровье! 📖"],
    ["disclaimer", "\n\n---\n⚖️ Данная информация носит ознакомительный характер и не является юридической консультацией. По конкретным вопросам обращайтесь к специалистам SOLIS Partners."],
    ["returning_user_thanks", "👋 {name}, рады снова вас видеть!\n\nГайд уже у вас. Приятного изучения! 📖"],
    ["guide_not_found", "Гайд не найден"],
    ["guide_pdf_unavailable", "📄 *{title}*\n\n{description}\n\n_(PDF-версия гайда будет доступна в ближайшее время)_"],
    ["cache_cleared", "✅ Кеш сброшен. Данные обновятся при следующем запросе."],
]

LEADS_HEADERS = ["timestamp", "user_id", "username", "name", "email", "guide", "consent", "source", "interests", "warmth"]

FOLLOWUP_HEADERS = ["key", "delay_hours", "text"]
FOLLOWUP_DATA = [
    ["step_1", "24", "Здравствуйте! Вчера вы скачали наш гайд. Удалось ли начать изучение? Если есть вопросы — мы всегда готовы помочь! Для консультации: @SOLISlegal"],
    ["step_2", "72", "Привет! Прошло несколько дней с момента скачивания гайда. Хотим поделиться практическим кейсом SOLIS Partners по этой теме. Подписывайтесь на наш канал! Другие гайды: /start"],
    ["step_3", "168", "Добрый день! Надеемся, гайд оказался полезным. Мы предлагаем бесплатную мини-консультацию (15 минут) по теме гайда с нашим специалистом. Для записи: @SOLISlegal"],
]

ANALYTICS_HEADERS = ["Метрика", "Значение", "Комментарий"]

# ── Новые листы (Admin Hub v2) ────────────────────────────────────────

ARTICLES_HEADERS = [
    "id", "title", "date", "author", "category", "categoryRu",
    "image", "description", "externalUrl", "content",
    "isGoldTag", "practiceIds", "telegramBotLink",
    "telegramBotCtaTitle", "telegramBotCtaDesc", "active",
]

DATA_ROOM_HEADERS = ["category", "title", "content", "updated"]

NEWS_FEED_HEADERS = ["timestamp", "source", "title", "url", "summary", "used"]

CONTENT_CAL_HEADERS = ["date", "type", "title", "status", "notes"]

AI_CONV_HEADERS = ["timestamp", "admin_message", "ai_response"]

CONSULT_LOG_HEADERS = ["timestamp", "user_id", "question", "answer"]


# ── Основная логика ────────────────────────────────────────────────────

def main() -> None:
    # Проверка файла credentials
    if not os.path.isfile(CREDENTIALS_PATH):
        print(f"❌ Файл {CREDENTIALS_PATH} не найден.")
        print("   Скачайте JSON-ключ Service Account из Google Cloud Console")
        print(f"   и положите его в: {os.path.abspath(CREDENTIALS_PATH)}")
        sys.exit(1)

    # Проверка SPREADSHEET_ID
    if not SPREADSHEET_ID or "ВСТАВЬТЕ" in SPREADSHEET_ID:
        print("❌ GOOGLE_SPREADSHEET_ID не задан в .env")
        print()
        print("   Инструкция:")
        print("   1. Создайте новую таблицу: https://sheets.google.com")
        print("   2. Скопируйте ID из URL: https://docs.google.com/spreadsheets/d/{ID}/edit")
        print("   3. Вставьте в .env: GOOGLE_SPREADSHEET_ID={ID}")
        print()

        # Предложить создать таблицу автоматически
        answer = input("   Создать таблицу автоматически? (y/n): ").strip().lower()
        if answer == "y":
            create_new_spreadsheet()
            return
        sys.exit(1)

    # Подключение
    print(f"📊 Подключаюсь к таблице {SPREADSHEET_ID}...")
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    gc = gspread.authorize(creds)

    try:
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    except gspread.exceptions.SpreadsheetNotFound:
        print("❌ Таблица не найдена. Проверьте:")
        print("   1. Правильный ли GOOGLE_SPREADSHEET_ID в .env")
        print("   2. Расшарена ли таблица на email сервисного аккаунта")
        sys.exit(1)

    print(f"✅ Подключено: {spreadsheet.title}")

    # Настройка основных листов
    setup_sheet(spreadsheet, "Каталог гайдов", CATALOG_HEADERS)
    setup_sheet(spreadsheet, "Тексты бота", TEXTS_HEADERS, TEXTS_DATA)
    setup_sheet(spreadsheet, "Лиды", LEADS_HEADERS)
    setup_sheet(spreadsheet, "Авто-серия", FOLLOWUP_HEADERS, FOLLOWUP_DATA)
    setup_sheet(spreadsheet, "Аналитика", ANALYTICS_HEADERS)

    # Настройка новых листов (Admin Hub v2)
    setup_sheet(spreadsheet, "Статьи сайта", ARTICLES_HEADERS)
    setup_sheet(spreadsheet, "Data Room", DATA_ROOM_HEADERS)
    setup_sheet(spreadsheet, "News Feed", NEWS_FEED_HEADERS)
    setup_sheet(spreadsheet, "Content Calendar", CONTENT_CAL_HEADERS)
    setup_sheet(spreadsheet, "AI Conversations", AI_CONV_HEADERS)
    setup_sheet(spreadsheet, "Consult Log", CONSULT_LOG_HEADERS)

    # Удалить дефолтный Sheet1 если есть
    try:
        default_sheet = spreadsheet.worksheet("Sheet1")
        if len(spreadsheet.worksheets()) > 1:
            spreadsheet.del_worksheet(default_sheet)
            print("🗑️  Удалён пустой лист Sheet1")
    except gspread.exceptions.WorksheetNotFound:
        pass
    except Exception:
        pass  # Не критично

    print()
    print("=" * 50)
    print("✅ Таблица настроена!")
    print(f"   ID: {SPREADSHEET_ID}")
    print(f"   URL: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print()
    print("Следующие шаги:")
    print("   1. Убедитесь что GOOGLE_SPREADSHEET_ID в .env = " + SPREADSHEET_ID)
    print("   2. Запустите бота: python -m src.bot.main")
    print("=" * 50)


def setup_sheet(
    spreadsheet: gspread.Spreadsheet,
    title: str,
    headers: list[str],
    data: list[list[str]] | None = None,
) -> None:
    """Создаёт или находит лист и заполняет заголовки + данные."""
    try:
        ws = spreadsheet.worksheet(title)
        print(f"📄 Лист «{title}» уже существует")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=100, cols=len(headers))
        print(f"📄 Создан лист «{title}»")

    # Проверяем заголовки
    existing = ws.row_values(1)
    if not existing:
        ws.update("A1", [headers])
        print(f"   ✅ Заголовки добавлены: {headers}")

        if data:
            # Заполняем данные начиная со 2-й строки
            cell_range = f"A2:{chr(64 + len(headers))}{len(data) + 1}"
            ws.update(cell_range, data)
            print(f"   ✅ Добавлено {len(data)} строк данных")
    else:
        print(f"   ⏭️  Заголовки уже есть, пропускаю")


def create_new_spreadsheet() -> None:
    """Создаёт новую таблицу через API и настраивает её."""
    print("📊 Создаю новую таблицу...")
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    gc = gspread.authorize(creds)

    spreadsheet = gc.create("SOLIS Legal Bot")
    spreadsheet_id = spreadsheet.id

    print(f"✅ Таблица создана: {spreadsheet_id}")

    # Делаем доступной по ссылке
    spreadsheet.share("", perm_type="anyone", role="writer")
    print("🔗 Доступ открыт по ссылке (Editor)")

    # Настраиваем основные листы
    setup_sheet(spreadsheet, "Каталог гайдов", CATALOG_HEADERS)
    setup_sheet(spreadsheet, "Тексты бота", TEXTS_HEADERS, TEXTS_DATA)
    setup_sheet(spreadsheet, "Лиды", LEADS_HEADERS)
    setup_sheet(spreadsheet, "Авто-серия", FOLLOWUP_HEADERS, FOLLOWUP_DATA)
    setup_sheet(spreadsheet, "Аналитика", ANALYTICS_HEADERS)

    # Новые листы (Admin Hub v2)
    setup_sheet(spreadsheet, "Статьи сайта", ARTICLES_HEADERS)
    setup_sheet(spreadsheet, "Data Room", DATA_ROOM_HEADERS)
    setup_sheet(spreadsheet, "News Feed", NEWS_FEED_HEADERS)
    setup_sheet(spreadsheet, "Content Calendar", CONTENT_CAL_HEADERS)
    setup_sheet(spreadsheet, "AI Conversations", AI_CONV_HEADERS)
    setup_sheet(spreadsheet, "Consult Log", CONSULT_LOG_HEADERS)

    # Удалить Sheet1
    try:
        default_sheet = spreadsheet.worksheet("Sheet1")
        spreadsheet.del_worksheet(default_sheet)
    except Exception:
        pass

    print()
    print("=" * 50)
    print("✅ Таблица готова!")
    print(f"   ID: {spreadsheet_id}")
    print(f"   URL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    print()
    print(f"⚠️  ВАЖНО: вставьте в .env:")
    print(f"   GOOGLE_SPREADSHEET_ID={spreadsheet_id}")
    print("=" * 50)


if __name__ == "__main__":
    main()
