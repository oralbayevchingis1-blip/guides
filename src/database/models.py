"""SQLAlchemy модели базы данных — User, Lead, ConsentLog, ScheduledTask, FunnelEvent."""

from datetime import datetime, timezone

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Index, Integer, String, Text
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.config import settings

# ──────────────────────── Engine & Session ──────────────────────────────

engine = create_async_engine(settings.DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(AsyncAttrs, DeclarativeBase):
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
    traffic_source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    business_sphere: Mapped[str | None] = mapped_column(String(255), nullable=True)
    company_size: Mapped[str | None] = mapped_column(String(50), nullable=True)
    company_stage: Mapped[str | None] = mapped_column(String(50), nullable=True)
    bot_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    last_activity: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
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
    traffic_source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    consent_given: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
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
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self) -> str:
        return f"<ConsentLog(user_id={self.user_id}, type={self.consent_type})>"


class ScheduledTask(Base):
    """Отложенная задача (follow-up и т.п.), хранится в БД — переживает рестарт."""

    __tablename__ = "scheduled_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    payload: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", index=True,
    )
    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    executed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_scheduled_tasks_status_run_at", "status", "run_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<ScheduledTask(id={self.id}, type={self.task_type}, "
            f"user={self.user_id}, status={self.status})>"
        )


class TopicSubscription(Base):
    """Подписка пользователя на обновления по категории гайдов."""

    __tablename__ = "topic_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_topic_sub_user_cat", "user_id", "category", unique=True),
    )

    def __repr__(self) -> str:
        return f"<TopicSubscription(user_id={self.user_id}, category={self.category})>"


class Question(Base):
    """Вопрос пользователя юристу."""

    __tablename__ = "questions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    question_text: Mapped[str] = mapped_column(Text, nullable=False)
    answer_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="new")
    admin_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    answered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    __table_args__ = (
        Index("ix_questions_status", "status"),
        Index("ix_questions_user", "user_id"),
    )

    def __repr__(self) -> str:
        return f"<Question(id={self.id}, user={self.user_id}, status={self.status})>"


class FunnelEvent(Base):
    """Событие воронки — сквозное отслеживание шагов пользователя.

    Этапы (step):
        bot_start       — /start (переход по ссылке / запуск бота)
        view_categories — просмотр категорий
        view_category   — выбор конкретной категории
        view_guide      — просмотр карточки гайда
        click_download  — нажатие «Скачать»
        sub_prompt      — показан барьер подписки
        sub_confirmed   — подписка подтверждена
        email_prompt    — показан запрос email
        email_submitted — email отправлен
        consent_given   — согласие дано
        pdf_delivered   — PDF отправлен
        consultation    — запись на консультацию
    """

    __tablename__ = "funnel_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    step: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    guide_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    meta: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )

    __table_args__ = (
        Index("ix_funnel_step_created", "step", "created_at"),
        Index("ix_funnel_user_step", "user_id", "step"),
    )

    def __repr__(self) -> str:
        return f"<FunnelEvent(user={self.user_id}, step={self.step})>"


class Referral(Base):
    """Реферальная связь: кто кого пригласил."""

    __tablename__ = "referrals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    referrer_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    referred_id: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True)
    referred_downloaded: Mapped[bool] = mapped_column(Boolean, default=False)
    reward_given: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_referrals_referrer", "referrer_id"),
        Index("ix_referrals_referred", "referred_id"),
    )

    def __repr__(self) -> str:
        return f"<Referral(referrer={self.referrer_id}, referred={self.referred_id})>"


class AdCampaign(Base):
    """Рекламная кампания — трекинг расходов и конверсий.

    Хранит данные о платных кампаниях (Facebook, Instagram, Telegram Ads)
    для расчёта стоимости лида (CPL) и ROAS.
    """

    __tablename__ = "ad_campaigns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    platform: Mapped[str] = mapped_column(String(50), nullable=False)
    guide_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    budget: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    spent: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    currency: Mapped[str] = mapped_column(String(10), nullable=False, default="KZT")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    __table_args__ = (
        Index("ix_adcampaign_platform", "platform"),
        Index("ix_adcampaign_status", "status"),
    )

    def __repr__(self) -> str:
        return f"<AdCampaign({self.campaign_id}, {self.platform}, spent={self.spent})>"


class ABExperiment(Base):
    """A/B тест — определяет эксперимент с двумя вариантами текста/логики.

    Variants хранятся как JSON: {"A": "текст A", "B": "текст B"}
    metric — какой funnel step считается конверсией (pdf_delivered, consultation, etc.)
    """

    __tablename__ = "ab_experiments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    variants_json: Mapped[str] = mapped_column(Text, nullable=False)
    metric: Mapped[str] = mapped_column(String(50), nullable=False, default="pdf_delivered")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("ix_ab_experiment_status", "status"),
    )

    def __repr__(self) -> str:
        return f"<ABExperiment({self.name}, status={self.status})>"


class ABAssignment(Base):
    """Назначение пользователя на вариант A/B теста."""

    __tablename__ = "ab_assignments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    variant: Mapped[str] = mapped_column(String(10), nullable=False)
    converted: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    converted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    __table_args__ = (
        Index("ix_ab_assign_exp_user", "experiment_name", "user_id", unique=True),
        Index("ix_ab_assign_exp_variant", "experiment_name", "variant"),
    )

    def __repr__(self) -> str:
        return f"<ABAssignment({self.experiment_name}, user={self.user_id}, v={self.variant})>"


# ──────────────────────── Инициализация ─────────────────────────────────

async def init_db() -> None:
    """Создаёт все таблицы и применяет лёгкие миграции."""
    from sqlalchemy import text as sa_text
    import logging

    _log = logging.getLogger(__name__)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Миграции: добавить колонки к существующим таблицам.
    # SQLAlchemy create_all не добавляет колонки в уже существующие таблицы.
    _migrations = [
        ("leads", "traffic_source", "VARCHAR(255)"),
        ("users", "business_sphere", "VARCHAR(255)"),
        ("users", "company_size", "VARCHAR(50)"),
        ("users", "company_stage", "VARCHAR(50)"),
        ("users", "bot_blocked", "BOOLEAN DEFAULT 0"),
    ]
    async with engine.begin() as conn:
        for table, column, col_type in _migrations:
            try:
                await conn.execute(
                    sa_text(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                )
                _log.info("Migration applied: %s.%s", table, column)
            except Exception:
                pass  # колонка уже существует
