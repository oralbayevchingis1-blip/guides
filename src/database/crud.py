"""CRUD-операции с базой данных."""

import logging
from typing import Any

from sqlalchemy import select

from sqlalchemy import func as sa_func

from src.database.models import ConsentLog, Lead, Referral, User, async_session

logger = logging.getLogger(__name__)


# ──────────────────────── Users ─────────────────────────────────────────

async def get_or_create_user(
    user_id: int,
    username: str | None = None,
    full_name: str | None = None,
) -> User:
    """Получает пользователя из БД или создаёт нового.

    Args:
        user_id: Telegram ID пользователя.
        username: Username в Telegram.
        full_name: Полное имя пользователя.

    Returns:
        Экземпляр User.
    """
    async with async_session() as session:
        stmt = select(User).where(User.user_id == user_id)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if user is None:
            user = User(
                user_id=user_id,
                username=username,
                full_name=full_name,
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)
            logger.info("Новый пользователь создан: user_id=%s", user_id)
        else:
            # Обновляем данные если изменились
            changed = False
            if username and user.username != username:
                user.username = username
                changed = True
            if full_name and user.full_name != full_name:
                user.full_name = full_name
                changed = True
            if changed:
                await session.commit()
                logger.info("Пользователь обновлён: user_id=%s", user_id)

        return user


# ──────────────────────── Leads ─────────────────────────────────────────

async def save_lead(
    user_id: int,
    email: str,
    name: str,
    selected_guide: str,
) -> Lead:
    """Сохраняет нового лида в базу данных.

    Args:
        user_id: Telegram ID пользователя.
        email: Email пользователя.
        name: Имя пользователя.
        selected_guide: ID выбранного гайда.

    Returns:
        Экземпляр Lead.
    """
    async with async_session() as session:
        lead = Lead(
            user_id=user_id,
            email=email,
            name=name,
            selected_guide=selected_guide,
            consent_given=True,
        )
        session.add(lead)
        await session.commit()
        await session.refresh(lead)
        logger.info("Лид сохранён: user_id=%s, email=%s", user_id, email)
        return lead


async def get_leads_by_user(user_id: int) -> list[Lead]:
    """Получает все лиды пользователя."""
    async with async_session() as session:
        stmt = select(Lead).where(Lead.user_id == user_id)
        result = await session.execute(stmt)
        return list(result.scalars().all())


async def get_lead_by_user_id(user_id: int) -> Lead | None:
    """Получает последний лид пользователя (для пропуска формы повторных пользователей).

    Args:
        user_id: Telegram ID пользователя.

    Returns:
        Последний Lead или None, если пользователь ещё не заполнял форму.
    """
    async with async_session() as session:
        stmt = (
            select(Lead)
            .where(Lead.user_id == user_id)
            .order_by(Lead.id.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()


async def get_all_user_ids() -> list[int]:
    """Получает все user_id из таблицы пользователей (для broadcast)."""
    async with async_session() as session:
        stmt = select(User.user_id)
        result = await session.execute(stmt)
        return [row[0] for row in result.all()]


# ──────────────────────── Referrals ─────────────────────────────────────

async def save_referral(referrer_id: int, referred_id: int) -> bool:
    """Сохраняет реферальную связку.

    Args:
        referrer_id: User ID реферера.
        referred_id: User ID нового пользователя.

    Returns:
        True если связка создана, False если уже существует.
    """
    async with async_session() as session:
        # Проверяем, не был ли уже этот пользователь зарегистрирован как реферал
        existing = await session.execute(
            select(Referral).where(Referral.referred_id == referred_id)
        )
        if existing.scalar_one_or_none():
            return False

        # Не позволяем самореферал
        if referrer_id == referred_id:
            return False

        referral = Referral(referrer_id=referrer_id, referred_id=referred_id)
        session.add(referral)
        await session.commit()
        logger.info("Реферал сохранён: %s -> %s", referrer_id, referred_id)
        return True


async def count_referrals(user_id: int) -> int:
    """Считает количество рефералов пользователя."""
    async with async_session() as session:
        result = await session.execute(
            select(sa_func.count(Referral.id)).where(
                Referral.referrer_id == user_id
            )
        )
        return result.scalar() or 0


# ──────────────────────── Consent Logs ──────────────────────────────────

async def save_consent_log(log_entry: dict[str, Any]) -> ConsentLog:
    """Сохраняет запись согласия в базу данных.

    Args:
        log_entry: Словарь с данными согласия.

    Returns:
        Экземпляр ConsentLog.
    """
    async with async_session() as session:
        consent = ConsentLog(
            user_id=log_entry["user_id"],
            consent_type=log_entry["consent_type"],
            timestamp=log_entry["timestamp"],
            ip_address=log_entry.get("ip_address"),
            user_agent=log_entry.get("user_agent"),
        )
        session.add(consent)
        await session.commit()
        await session.refresh(consent)
        logger.info("Согласие записано: user_id=%s", log_entry["user_id"])
        return consent
