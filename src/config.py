"""Конфигурация приложения — загрузка переменных окружения."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки бота, загружаемые из .env файла."""

    model_config = SettingsConfigDict(
        env_file=".env",
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
    GOOGLE_SPREADSHEET_ID: str = ""  # ID таблицы из URL
    CACHE_TTL_SECONDS: int = 300  # 5 минут

    # AI
    GEMINI_API_KEY: str = ""
    OPENAI_API_KEY: str = ""

    # Privacy
    PRIVACY_POLICY_URL: str = "https://www.solispartners.kz/privacy"


settings = Settings()
