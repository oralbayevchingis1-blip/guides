"""SQLAlchemy модели базы данных."""

from datetime import datetime, timezone

from sqlalchemy import BigInteger, Boolean, DateTime, Index, Integer, String, Text, func
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.config import settings

# ──────────────────────── Engine & Session ──────────────────────────────

engine = create_async_engine(settings.DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(AsyncAttrs, DeclarativeBase):
    """Базовый класс моделей."""
    pass


# ──────────────────────── Модели ────────────────────────────────────────

class User(Base):
    """Пользователь Telegram."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False, index=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_subscribed: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    language: Mapped[str | None] = mapped_column(String(5), nullable=True, default="ru")
    timezone: Mapped[str | None] = mapped_column(String(50), nullable=True, default="Asia/Almaty")
    karma_points: Mapped[int] = mapped_column(Integer, default=0)
    partner_id: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    traffic_source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_activity: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    def __repr__(self) -> str:
        return f"<User(user_id={self.user_id}, username={self.username})>"


class Lead(Base):
    """Лид — контактные данные пользователя."""

    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    selected_guide: Mapped[str] = mapped_column(String(100), nullable=False)
    business_sphere: Mapped[str | None] = mapped_column(String(255), nullable=True)
    consent_given: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_leads_user_created", "user_id", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<Lead(user_id={self.user_id}, email={self.email})>"


class ConsentLog(Base):
    """Лог согласий для compliance."""

    __tablename__ = "consent_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    consent_type: Mapped[str] = mapped_column(String(100), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
    )
    ip_address: Mapped[str | None] = mapped_column(String(50), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(255), nullable=True)

    def __repr__(self) -> str:
        return f"<ConsentLog(user_id={self.user_id}, type={self.consent_type})>"


class Referral(Base):
    """Реферальная связка: кто кого привёл."""

    __tablename__ = "referrals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    referrer_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    referred_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    def __repr__(self) -> str:
        return f"<Referral(referrer={self.referrer_id}, referred={self.referred_id})>"


class FeedbackScore(Base):
    """NPS/Feedback оценки пользователей."""

    __tablename__ = "feedback_scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    score: Mapped[int] = mapped_column(Integer, nullable=False)
    context: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
    )

    def __repr__(self) -> str:
        return f"<FeedbackScore(user_id={self.user_id}, score={self.score})>"


class WaitlistEntry(Base):
    """Запись в Waitlist для Coming Soon услуг."""

    __tablename__ = "waitlist_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    service_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_waitlist_user_service", "user_id", "service_id", unique=True),
    )

    def __repr__(self) -> str:
        return f"<WaitlistEntry(user_id={self.user_id}, service={self.service_id})>"


class LegalTask(Base):
    """C3. Legal Task Tracker — задача для юриста."""

    __tablename__ = "legal_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    assignee: Mapped[str | None] = mapped_column(String(255), nullable=True)
    priority: Mapped[str] = mapped_column(String(20), default="normal")
    status: Mapped[str] = mapped_column(String(20), default="new", index=True)
    lead_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(),
    )

    __table_args__ = (
        Index("ix_tasks_user_status", "user_id", "status"),
    )

    def __repr__(self) -> str:
        return f"<LegalTask(id={self.id}, title={self.title}, status={self.status})>"


class PendingSheetsWrite(Base):
    """Очередь неудавшихся записей в Google Sheets (Circuit Breaker)."""

    __tablename__ = "pending_sheets_writes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    method_name: Mapped[str] = mapped_column(String(100), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(),
    )
    retries: Mapped[int] = mapped_column(Integer, default=0)

    def __repr__(self) -> str:
        return f"<PendingSheetsWrite(method={self.method_name}, retries={self.retries})>"


# ──────────────────────── Инициализация ─────────────────────────────────

async def init_db() -> None:
    """Создаёт все таблицы в базе данных."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
