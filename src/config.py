"""Конфигурация приложения — загрузка переменных окружения."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_env_path = Path(".env")


class Settings(BaseSettings):
    """Настройки бота, загружаемые из .env файла или системных переменных."""

    model_config = SettingsConfigDict(
        env_file=str(_env_path) if _env_path.exists() else None,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Telegram
    BOT_TOKEN: str
    CHANNEL_USERNAME: str  # например @your_channel
    ADMIN_ID: int

    # Database (SQLite — локальный backup)
    DATABASE_URL: str = "sqlite+aiosqlite:///data/legal_bot.db"

    # Google Workspace
    GOOGLE_CREDENTIALS_PATH: str = "google_credentials.json"
    GOOGLE_CREDENTIALS_BASE64: str = ""  # base64-encoded JSON (for Railway/Docker)
    GOOGLE_CREDENTIALS_JSON: str = ""   # raw JSON string (alternative to base64)
    GOOGLE_SPREADSHEET_ID: str = ""  # ID таблицы из URL
    CACHE_TTL_SECONDS: int = 300  # 5 минут

    # AI
    GEMINI_API_KEY: str = ""
    OPENAI_API_KEY: str = ""

    # Privacy
    PRIVACY_POLICY_URL: str = "https://www.solispartners.kz/privacy"

    # CRM Webhook (Pipedrive/HubSpot — пустой = отключен)
    CRM_WEBHOOK_URL: str = ""

    # Telegram Payments (Stripe/Kaspi/YooKassa — пустой = отключены)
    PAYMENT_PROVIDER_TOKEN: str = ""

    # Mini App URL (FastAPI dashboard)
    MINI_APP_URL: str = ""

    # Admin Support Chat ID (группа для Shared Inbox — пустой = только ADMIN_ID)
    SUPPORT_CHAT_ID: int = 0

    # Sentry DSN (пустой = Sentry отключен)
    SENTRY_DSN: str = ""

    # Email / SMTP (пустой = отключено)
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    FROM_EMAIL: str = "noreply@solispartners.kz"
    FROM_NAME: str = "SOLIS Partners"
    RESEND_API_KEY: str = ""

    # Environment tag
    ENVIRONMENT: str = "production"
    VERSION: str = "3.0.0"


settings = Settings()
